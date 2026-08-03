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
from without import Sink
from without import from_sink
from without_durability.interfaces import LEASE
from without_durability.interfaces import Delivery
from without_durability.interfaces import check_duration

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

    def __post_init__(self) -> None:
        check_duration("a lease", self.lease)
        object.__setattr__(self, "move", self.redis.register_script(WAKE_DUE))

    @property
    def ready_key(self) -> str:
        return f"{self.namespace}:ready"

    @property
    def sleeping_key(self) -> str:
        return f"{self.namespace}:sleeping"

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

    async def wake_at(self, workflow: str, when: datetime) -> None:
        await self.redis.zadd(self.sleeping_key, {workflow: when.timestamp()})

    async def wake_due(self, now: datetime) -> tuple[str, ...]:
        woken = await self.move(keys=[self.sleeping_key, self.ready_key], args=[now.timestamp(), self.batch])
        return tuple(cast(list[str], woken))

    async def next_ready(self, within: timedelta) -> Delivery | None:
        read = cast(
            list[tuple[str, Entries]],
            await self.redis.xreadgroup(
                self.group,
                self.consumer,
                {self.ready_key: ">"},
                count=1,
                block=int(within.total_seconds() * 1000),
            ),
        )
        if not read:
            return None  # the block elapsed with nothing new
        _stream, entries = read[0]
        # Indexed rather than guarded, because a reply for this stream carries at least
        # one entry: `>` reads only entries never delivered to this group, and the one
        # thing that removes entries here (`trim`) removes only what every group has
        # acknowledged, so nothing can vanish between the read and this line.
        return deliveries(entries)[0]

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
        """
        _cursor, entries, _deleted = cast(
            tuple[str, Entries, list[str]],
            await self.redis.xautoclaim(
                self.ready_key,
                self.group,
                self.consumer,
                min_idle_time=int(idle.total_seconds() * 1000),
                start_id="0-0",
                count=1,
            ),
        )
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


def deliveries(entries: Entries) -> tuple[Delivery, ...]:
    return tuple(Delivery(workflow=fields["workflow"], receipt=entry) for entry, fields in entries)
