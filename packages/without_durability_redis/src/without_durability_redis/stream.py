# The piece that is genuinely a service: knowing *when* a workflow can make progress. A
# checkpoint says what a workflow has done, and a pass's outcome says what it is waiting
# for, but neither brings anyone back. This does, with two Redis structures and no server
# of our own:
#
#   - a stream of workflow ids that can run *now*, which the API appends to when an
#     order arrives or a confirmation lands, and workers read as a consumer group;
#   - a sorted set of workflows waiting on a clock, scored by their deadline, which a
#     timer drains into the stream as each one comes due.
#
# A stream rather than a list, because a list loses work. `BLPOP` hands an id over and
# forgets it, so a worker that dies mid-pass takes the wakeup with it and the workflow
# never runs again. `XREADGROUP` moves the entry into that consumer's pending list
# instead of deleting it: it is *delivered but unacknowledged* until the worker says
# otherwise, and `XAUTOCLAIM` is how another worker takes over what a dead one was
# holding. The cost is that a wakeup must now be acknowledged, which is why a delivery
# is a value with a receipt rather than a bare id.
#
# `wake_due` is one operation rather than a claim and an append, and that is the whole of
# its design. Taking a workflow off the sleepers and queueing it are only *durable*
# together: between them the workflow is in neither structure, so a process that dies in
# the gap loses the wakeup. Redis has no single command that moves a member from a sorted
# set to a stream, so the move is a Lua script, which runs to completion in the server
# where the two writes have to happen together. It settles the timer's cardinality for
# free: every worker may run a timer, and they all see the same due workflow, but the
# script is serialized against itself, so the first mover takes it and the rest see an
# empty range. Leader election buys nothing over that.
#
# The naming matters as much as the atomicity: the protocol names the *transition*, so a
# caller cannot hold a claimed-but-unqueued id at all, which is the state that was lossy.
# Making it unrepresentable beats remembering to do both halves, and it is the same
# argument `Durable.arrive` makes one level up.
#
# The sorted set is an *index*, not the record: the deadline itself lives in the
# workflow's checkpoint, put there by `Run.sleep`. Losing the set leaves a workflow
# asleep forever rather than corrupt, and rebuilding it is a scan over checkpoints.

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from typing import cast
from uuid import uuid4

from redis.asyncio import Redis
from redis.commands.core import AsyncScript
from redis.exceptions import ResponseError
from without_durability.interfaces import LEASE
from without_durability.interfaces import Delivery
from without_durability.interfaces import check_duration
from without_streams import Sink
from without_streams import from_sink

from without_durability_redis.units import milliseconds

type Entries = list[tuple[str, dict[str, str]]]

# How often the trimmer looks. It is a housekeeping interval rather than a correctness
# one: nothing goes wrong if it never runs except that the stream keeps every entry.
TRIM_EVERY = timedelta(minutes=1)


# `LIMIT` bounds one tick's work, so a backlog drains over several ticks rather than in
# one enormous batch. `XADD` inside a script is allowed because Redis replicates a
# script's *effects* rather than the script itself, so the generated ids do not have to
# be reproducible.
#
# Deliberately no `MAXLEN`: a stream trims by *length*, not by what has been consumed,
# so capping it would drop the oldest entries once a backlog outgrew the cap, and the
# oldest entries are the ones nobody has run yet. A queue that sheds unread work under
# load is worse than one that grows. What bounds it instead is `trim`, which removes by
# `MINID` behind what every consumer group has finished with.
#
#   KEYS[1]  the sleeping sorted set, scored by deadline
#   KEYS[2]  the ready stream
#   ARGV[1]  now, as a unix timestamp in seconds
#   ARGV[2]  how many to move at most, so one tick cannot drain an unbounded backlog
#   returns  the workflows moved, empty when another timer got to them first
WAKE_DUE = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, workflow in ipairs(due) do
  redis.call('ZREM', KEYS[1], workflow)
  redis.call('XADD', KEYS[2], '*', 'workflow', workflow)
end
return due
"""


@dataclass(frozen=True, slots=True)
class RedisStreamScheduler:
    """
    `Scheduler` as one Redis stream (with a consumer group) and one sorted set.

    Like `RedisCheckpointer`, the client MUST be built with `decode_responses=True`:
    this app owns both ends of the queue, so it decides once here rather than every
    read deciding again.

    `next_ready` blocks *in Redis* rather than polling, so a worker with nothing to do
    costs nothing and a submitted order is picked up the instant it is appended. Its
    `within` bound is not a poll interval but a shutdown one: it caps how long a
    cancelled worker sits in a blocking read before it can notice.

    Every worker reads the same group under its own `consumer` name, which is how the
    work distributes: the group hands each entry to exactly one of them, so scaling out
    is starting another process rather than partitioning anything. A long-lived
    deployment would name consumers after the host and process (and retire dead ones with
    `XGROUP DELCONSUMER`) rather than minting one per instance as this does.

    `XACK` clears an entry from the pending list but leaves it in the stream, so the thing
    that bounds this queue is `trim`, run as its own control-plane task beside the worker
    (see `trimming`). Without it the stream is correct and grows forever.
    """

    redis: Redis
    namespace: str = "workflow"
    group: str = "workers"
    batch: int = 100
    consumer: str = field(default_factory=lambda: uuid4().hex)
    # How long a delivery may sit in a consumer's pending list before `reclaim` will take
    # it over. Here it is genuinely an *argument* to `XAUTOCLAIM` rather than something
    # written into the queue, which is the visibility-scored stores' route to the same
    # property, so the worker passes it back in on every pull. It is on the store anyway,
    # and for the reason the interface gives: it also bounds the checkpoint claim, and the two
    # drift the moment they are set in two places.
    lease: timedelta = LEASE
    # Registered at construction: this precomputes the digest and holds the client, so a
    # call sends the digest and falls back to the source only when the server has not
    # seen it.
    move: AsyncScript = field(init=False, repr=False, compare=False)
    # Where this worker's sweep of the pending list got to, as a one-element list because
    # the rest of this is a value and a cursor is not: it is the one thing here that
    # carries from one call to the next. See `reclaim` for why the sweep is resumed rather
    # than restarted, and why losing this costs a sweep rather than a delivery.
    scanned: list[str] = field(default_factory=lambda: ["0-0"], repr=False, compare=False)

    def __post_init__(self) -> None:
        check_duration("a lease", self.lease)
        object.__setattr__(self, "move", self.redis.register_script(WAKE_DUE))

    # The braces are Redis Cluster's hash tag, exactly as they are on `RedisCheckpointer`'s
    # pair, and for a reason that is stronger here: `wake_due` is a script over *both* of
    # these keys, so untagged they hash to two slots and the cluster refuses the call
    # before running any of it. That is not a degradation but a queue where nothing
    # sleeping ever wakes, and it takes the worker with it, since the timer and the pass
    # loop share a task group.
    #
    # The tag is the namespace rather than the workflow, which is the honest scope: a queue
    # is one shared structure that every worker reads, so it lives on one node whatever the
    # tag says. What the tag buys is that both halves of it live on the *same* one.
    @property
    def ready_key(self) -> str:
        return f"{{{self.namespace}}}:ready"

    @property
    def sleeping_key(self) -> str:
        return f"{{{self.namespace}}}:sleeping"

    async def prepare(self) -> None:
        """
        Create the consumer group, which every reader needs and no writer does.

        From `0` rather than `$`, so an order submitted before any worker existed is
        delivered rather than stranded: the group starts at the beginning of the stream
        instead of at whatever happened to be its end when the first worker booted.
        """
        try:
            await self.redis.xgroup_create(self.ready_key, self.group, id="0", mkstream=True)
        except ResponseError as error:
            # Already created, which is the ordinary case for every worker after the
            # first. Redis reports it as an error rather than a no-op, and redis-py
            # surfaces the server's string as-is.
            if "BUSYGROUP" not in str(error):
                raise

    async def make_ready(self, workflow: str) -> None:
        await self.redis.xadd(self.ready_key, {"workflow": workflow})

    async def wake_at(self, delivery: Delivery, when: datetime) -> None:
        """
        Put the workflow among the sleepers, and answer for the delivery that got it there.

        Nothing to compare here, which is the sorted set's problem rather than the
        stream's: a wakeup that arrived mid-pass is a *new entry* in the stream, so
        writing a deadline into the sleepers cannot overwrite it and acknowledging this
        delivery cannot remove it.
        """
        await self.redis.zadd(self.sleeping_key, {delivery.workflow: when.timestamp()})
        await self.done(delivery)

    async def wake_due(self, now: datetime) -> tuple[str, ...]:
        woken = await self.move(keys=[self.sleeping_key, self.ready_key], args=[now.timestamp(), self.batch])
        return tuple(cast(list[str], woken))

    async def next_ready(self, within: timedelta) -> Delivery | None:
        read = await self.redis.xreadgroup(
            self.group,
            self.consumer,
            {self.ready_key: ">"},
            count=1,
            # Rounded up rather than truncated, because `BLOCK 0` is not a shorter wait but
            # an unbounded one: a `within` under a millisecond would park this worker until
            # an entry arrives, and the bound exists precisely so a cancelled one can
            # notice (see `units`).
            block=milliseconds(within),
        )
        if not read:
            return None  # the block elapsed with nothing new
        # Indexed rather than guarded, because a reply for this stream carries at least
        # one entry: `>` reads only entries never delivered to this group, and the one
        # thing that removes entries here (`trim`) removes only what every group has
        # acknowledged, so nothing can vanish between the read and this line.
        return deliveries(read_entries(read, self.ready_key))[0]

    async def reclaim(self, idle: timedelta) -> Delivery | None:
        """
        Take over *one* delivery a worker has been holding without acknowledging.

        One, because a worker should never hold more than it is about to work on: taking
        a batch would mean owing several passes while running one, which is the thing
        pulling one at a time exists to avoid. A backlog of abandoned work is drained the
        same way any other work is, one free slot at a time.

        `idle` is a lease: too short and a slow pass is overtaken while it is still
        running, too long and a crashed worker's workflow waits that long to be picked
        up. Overtaking is survivable and not free, so the bound should exceed how long a
        pass can honestly take.

        The cursor is *kept* rather than discarded or exhausted, because `XAUTOCLAIM`
        bounds its own work: it scans about ten times `count` pending entries per call and
        then stops, handing back where it got to. Both of the obvious ways to spend that
        are wrong, in opposite directions.

        Starting from `0-0` every time gives up after ten entries and reports "nothing
        abandoned" while a dead worker's deliveries sit behind them, which is not a rare
        arrangement but one this worker makes for itself: a pool of twenty holds twenty
        entries whose idle clocks it keeps resetting, so the abandoned ones are exactly
        the entries furthest down the list. Walking the cursor to the end within one call
        finds them, and costs a full sweep of the pending list on the path that always
        runs: with a fleet holding two thousand entries in flight, that is a fifth of a
        second of round trips before every single pull, and it worsens as the fleet grows.

        One step per call, resumed from where the last one stopped, is what both of those
        miss. Each pull costs a single round trip, and successive pulls sweep the whole
        pending list and wrap around, so an abandoned delivery is found within one sweep
        rather than immediately or never. Holding a cursor makes this scheduler stateful
        in a way nothing else here is, and the state is a hint rather than a fact: losing
        it (a restart, a second scheduler over the same group) costs a sweep, not a
        delivery.
        """
        cursor, entries, _deleted = cast(
            tuple[str, Entries, list[str]],
            await self.redis.xautoclaim(
                self.ready_key,
                self.group,
                self.consumer,
                min_idle_time=milliseconds(idle),
                start_id=self.scanned[0],
                count=1,
            ),
        )
        # `0-0` is the cursor's way of saying it reached the end of the pending list, and
        # it is also where the next sweep starts, so there is nothing to special-case.
        self.scanned[0] = cursor
        taken = deliveries(entries)
        return taken[0] if taken else None

    async def done(self, delivery: Delivery) -> None:
        await self.redis.xack(self.ready_key, self.group, delivery.receipt)

    async def trim(self) -> int:
        """
        Drop the entries every consumer group has finished with, and report how many.

        `XACK` clears an entry from a group's pending list and leaves it in the stream,
        so without this the queue is append-only: correct, and unbounded. `ACKED` is the
        bound, and it is the server's own answer rather than one computed here: it
        removes only entries that every group has read *and* acknowledged. Working that
        floor out client-side is possible and strictly worse, because it races every ack
        that lands between the read and the trim. Capping by *length* instead would be
        the wrong bound entirely, since that drops the oldest entries, which are the ones
        nobody has run yet.

        `MAXLEN 0` reads as "keep nothing", and with `ACKED` that is exactly right:
        trimming still stops at the first entry somebody has not answered for, so the
        threshold only says "as much as you are allowed to".

        Note what `ACKED` does *not* do. With no consumer groups at all it has no effect
        and the trim degrades to a plain `MAXLEN 0`, which would delete a queue nobody
        has read yet - and orders can be queued before the first worker ever boots, which
        is the case `prepare` creates its group from `0` to handle. So this refuses to
        trim a stream that has no groups, which is the one hazard in an otherwise safe
        command.

        Safe to run from every process at once, and safe to never run at all: the trim is
        idempotent, what counts as acknowledged only grows, and a stream nobody trims is
        merely large.

        Requires Redis 8.2 or newer, which is where `ACKED` arrives.
        """
        try:
            groups = cast(list[dict[str, object]], await self.redis.xinfo_groups(self.ready_key))
        except ResponseError as error:
            # Nobody has created the stream yet, which is ordinary when the trimmer starts
            # before the first worker's `prepare`. There is nothing to trim either way.
            if "no such key" not in str(error):
                raise
            return 0
        if not groups:
            return 0
        # `execute_command` because redis-py's `xtrim` helper predates `ACKED` and has no
        # parameter for it.
        return int(await self.redis.execute_command("XTRIM", self.ready_key, "MAXLEN", 0, "ACKED"))


def trimming(scheduler: RedisStreamScheduler) -> Sink[object]:
    """
    Keep the stream tidy, once per event, over whatever stream you drive it with.

    A `Sink` rather than a loop with a sleep in it, which is the same shape `waking` has
    and for the same reason: what makes a trim happen is a value somebody supplies, so
    this runs off a timer, off an operator poking a queue, off a Kubernetes cron hitting
    an endpoint, or off three items in a test. A loop can only ever be a timer, and it
    buries the schedule inside the thing being scheduled. It takes `Sink[object]` because
    it reads nothing from the event: whatever the stream carries, a trim is a trim.

    ```python
    async with asyncio.TaskGroup() as group:
        group.create_task(work(durable, body))
        group.create_task(trimming(scheduler)(ticks(TRIM_EVERY)))
    ```

    Control plane rather than data plane, and deliberately not folded into `work`:
    whether an entry is still needed is a question about what every group has
    acknowledged, not about the delivery a worker happens to be holding, so triggering it
    by traffic would make housekeeping cost scale with load for no reason. Its
    cardinality needs no arranging either, since the trim is idempotent: every process
    may run one, and N of them just means the same trim happens N times.
    """

    async def tidy(_event: object) -> None:
        await scheduler.trim()

    return from_sink(tidy)


def read_entries(read: object, stream: str) -> Entries:
    """
    The entries a group read for one stream, whichever protocol the client negotiated.

    RESP2 answers `XREADGROUP` with a list of stream-and-entries pairs and RESP3 with a
    mapping of stream name to entries, and redis-py hands each back as it came rather than
    normalizing them. Which one arrives is a property of the client a caller built
    (`Redis(protocol=3)`), so it is read here rather than named as a constraint in a
    docstring that nothing enforces: an unread reply shape is a worker that dies on its
    first pull.
    """
    if isinstance(read, dict):
        return cast(dict[str, list[Entries]], read)[stream][0]
    return cast(list[tuple[str, Entries]], read)[0][1]


def deliveries(entries: Entries) -> tuple[Delivery, ...]:
    return tuple(Delivery(workflow=fields["workflow"], receipt=entry) for entry, fields in entries)
