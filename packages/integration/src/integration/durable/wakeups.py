# The piece that is genuinely a service: knowing *when* a workflow can make progress.
# A checkpoint says what a workflow has done, and `Suspended` says what it is waiting
# for, but neither brings anyone back. This does, with two Redis structures and no
# server of our own:
#
#   - a stream of workflow ids that can run *now*, which the API appends to when an
#     order arrives or a confirmation lands, and workers read as a consumer group;
#   - a sorted set of workflows waiting on a clock, scored by their deadline, which a
#     timer drains into the stream as each one comes due.
#
# That is the question this toy exists to ask plainly: is this not what Temporal's
# server is doing? Yes. A durable workflow needs a queue of ready work and a timer, and
# once the *state* is a checkpoint anyone can read, those two are a stream and a sorted
# set rather than a cluster.
#
# A stream rather than a list, because a list loses work. `BLPOP` hands an id over and
# forgets it, so a worker that dies mid-pass takes the wakeup with it and the workflow
# never runs again. `XREADGROUP` moves the entry into that consumer's pending list
# instead of deleting it: it is *delivered but unacknowledged* until the worker says
# otherwise, and `XAUTOCLAIM` is how another worker takes over what a dead one was
# holding. The cost is that a wakeup must now be acknowledged, which is why a delivery
# is a value with a receipt rather than a bare id.
#
# `wake_due` is one operation rather than a claim and an append, and that is the whole
# of its design. Taking a workflow off the sleepers and queueing it are only *durable*
# together: between them the workflow is in neither structure, so a process that dies
# in the gap loses the wakeup. Redis has no single command that moves a member from a
# sorted set to a stream, so the move is a Lua script, which runs to completion in the
# server where the two writes have to happen together. It settles the timer's
# cardinality for free: every worker may run a timer, and they all see the same due
# workflow, but the script is serialized against itself, so the first mover takes it
# and the rest see an empty range. Leader election buys nothing over that.
#
# The naming matters as much as the atomicity: the protocol names the *transition*, so
# a caller cannot hold a claimed-but-unqueued id at all, which is the state that was
# lossy. Making it unrepresentable beats remembering to do both halves.
#
# The sorted set is an *index*, not the record: the deadline itself lives in the
# workflow's checkpoint, put there by `Run.sleep`. Losing the set leaves a workflow
# asleep forever rather than corrupt, and rebuilding it is a scan over checkpoints.

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from typing import Protocol
from typing import cast
from uuid import uuid4

from redis.asyncio import Redis
from redis.commands.core import AsyncScript
from redis.exceptions import ResponseError

type Entries = list[tuple[str, dict[str, str]]]


@dataclass(frozen=True, slots=True)
class Delivery:
    """
    One wakeup, taken by a worker and not yet acknowledged.

    The `receipt` is what makes the queue crash-safe: it names the entry the store is
    still holding on this worker's behalf, so acknowledging is a separate act from
    receiving and a worker that dies between them leaves the wakeup to be taken over
    rather than losing it.
    """

    workflow: str
    receipt: str


class Wakeups(Protocol):
    """
    Where a workflow's *right to run* is kept, apart from what it has done.

    The seam the API and the worker share: the API makes a workflow ready, a worker
    takes the next ready one and says when it is `done` with it. Injected like the
    checkpoint store, so the worker is drivable from a dict in a test.

    The requirements are about *not losing a wakeup*, since a lost one is a workflow
    that never runs again, and they are stated as properties rather than as mechanics
    because the two implementations here reach them by different routes.
    `RedisWakeups` is a stream beside a sorted set; `RedisSchedule` is one sorted set
    scored by visibility, where `wake_due`, `reclaim`, and `prepare` all have nothing to
    do. An implementation MUST guarantee that:

    - a workflow passed to `make_ready` is eventually yielded by some `next_ready`, even
      if the worker holding it dies mid-pass, and even if the wakeup arrives *while* a
      pass on that workflow is running;
    - `wake_due` moves each workflow it reports in one durable step, since one that
      removes a deadline and then queues the workflow loses it whenever it dies in
      between (an implementation with nothing to move satisfies this trivially);
    - a `wake_at` survives a `done` for a delivery taken before it, because the worker
      calls them in that order and the acknowledgement must not undo the scheduling.

    What is deliberately *not* required is that a workflow reach only one worker at a
    time. The stream will happily deliver two wakeups for one workflow to two consumers,
    and that is safe because exclusion belongs to `Checkpoints.claim` rather than here:
    this seam answers "who owes a pass", the checkpoint store answers "who may write".
    """

    async def prepare(self) -> None: ...

    async def make_ready(self, workflow: str) -> None: ...

    async def wake_at(self, workflow: str, when: datetime) -> None: ...

    async def wake_due(self, now: datetime) -> tuple[str, ...]: ...

    async def next_ready(self, within: timedelta) -> Delivery | None: ...

    async def reclaim(self, idle: timedelta) -> Delivery | None: ...

    async def done(self, delivery: Delivery) -> None: ...


# `LIMIT` bounds one tick's work, so a backlog drains over several ticks rather than in
# one enormous batch. `XADD` inside a script is allowed because Redis replicates a
# script's *effects* rather than the script itself, so the generated ids do not have to
# be reproducible.
#
# Deliberately no `MAXLEN`: a stream trims by *length*, not by what has been consumed,
# so capping it would drop the oldest entries once a backlog outgrew the cap, and the
# oldest entries are the ones nobody has run yet. A queue that sheds unread work under
# load is worse than one that grows. What bounds it instead is trimming by `MINID` once
# entries are acknowledged, which is a control-plane job this toy leaves out.
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
class RedisWakeups:
    """
    `Wakeups` as one Redis stream (with a consumer group) and one sorted set.

    Like `RedisCheckpoints`, the client MUST be built with `decode_responses=True`:
    this app owns both ends of the queue, so it decides once here rather than every
    read deciding again.

    `next_ready` blocks *in Redis* rather than polling, so a worker with nothing to do
    costs nothing and a submitted order is picked up the instant it is appended. Its
    `within` bound is not a poll interval but a shutdown one: it caps how long a
    cancelled worker sits in a blocking read before it can notice.

    Every worker reads the same group under its own `consumer` name, which is how the
    work distributes: the group hands each entry to exactly one of them, so scaling out
    is starting another process rather than partitioning anything. A long-lived
    deployment would name consumers after the host and process (and retire dead ones
    with `XGROUP DELCONSUMER`) rather than minting one per instance as this does.

    Nothing here trims the stream. `XACK` clears an entry from the pending list but
    leaves it in the stream, so a long-lived deployment needs a control-plane job that
    trims by `MINID` behind what every group has acknowledged. Capping the stream's
    *length* instead would be the wrong bound, since that drops the oldest entries,
    which are the ones nobody has run yet.
    """

    redis: Redis
    namespace: str = "workflow"
    group: str = "workers"
    batch: int = 100
    consumer: str = field(default_factory=lambda: uuid4().hex)
    # Derived at construction, the way `without_web.Router` compiles its trie: a store
    # has *one* script, and registering it is local work (it precomputes the digest and
    # holds the client), so calling it sends the digest and falls back to the source
    # only when the server has not seen it.
    move: AsyncScript = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
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
        return deliveries(entries)[0]

    async def reclaim(self, idle: timedelta) -> Delivery | None:
        """
        Take over *one* delivery a worker has been holding without acknowledging.

        One, because a worker should never hold more than it is about to work on:
        taking a batch would mean owing several passes while running one, which is the
        thing pulling one at a time exists to avoid. A backlog of abandoned work is
        drained the same way any other work is, one free slot at a time.

        `idle` is a lease, and the only real knob here: too short and a slow pass is
        overtaken while it is still running, too long and a crashed worker's workflow
        waits that long to be picked up. Overtaking is survivable (the second pass
        re-runs whatever step went unrecorded, which is the at-least-once bound the
        mechanism already carries) but it is not free, so the bound should exceed how
        long a pass can honestly take.
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


def deliveries(entries: Entries) -> tuple[Delivery, ...]:
    return tuple(Delivery(workflow=fields["workflow"], receipt=entry) for entry, fields in entries)
