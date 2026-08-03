# The same `Scheduler`, as one sorted set instead of a stream beside a sorted set.
#
# A workflow's score is the time it becomes visible, which turns three things that were
# separate into one:
#
#   - *queued now* is a score in the past, which is what `make_ready` writes;
#   - *sleeping* is a score in the future, which is what `wake_at` writes;
#   - *being worked on* is a score one lease ahead, which is what taking one writes.
#
# So there is nothing to move between structures, and `wake_due` has nothing to do: a
# workflow whose deadline passes becomes visible by the clock moving, not by anyone
# noticing. Reclaiming a dead worker's delivery is the same non-event, since the lease
# it pushed the score to simply arrives. The timer, the consumer group, the pending
# list, the acknowledgement, and the script that moved a workflow from one structure to
# the other are all gone, and so is the stream a trimmer has to keep tidy.
#
# What pays for that is the receipt, and the trick is that the *score* is the receipt.
# A stream never loses a wakeup because every `make_ready` appends a new entry; a sorted
# set holds each workflow once, so a wakeup that lands while a pass is running has
# nowhere to go except on top of the entry that pass is holding. Removing the entry
# afterwards would then throw the wakeup away, which is the lost-wakeup bug in its
# classic form. Since anything that wants another pass writes a *different* score, the
# fix is to make finishing conditional on the score still being the one this pass took:
#
#   take   -> score = now + lease, and that number is the receipt
#   done   -> remove only if the score is still that number
#
# A `make_ready` mid-pass (score = now), a `wake_at` mid-pass (score = the deadline), and
# another worker taking over an overrun pass (score = its own lease) all fail that
# comparison, so each one survives the finishing worker rather than being cleaned up by
# it. It is a compare-and-set where the version number is a value the design already had
# to store.
#
# The cost is real and it is latency. `XREADGROUP BLOCK` parks a worker inside Redis, so
# an idle one costs nothing and a submitted order is picked up the instant it lands; a
# sorted set has no blocking read, so this polls. That is the trade in one line: a stream
# is cheaper to wait on, a sorted set is cheaper to reason about.

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from time import monotonic
from typing import cast

from redis.asyncio import Redis
from redis.commands.core import AsyncScript
from without_durability.interfaces import LEASE
from without_durability.interfaces import Delivery
from without_durability.interfaces import check_duration
from without_durability.stepwise import now_utc

from without_durability_redis.units import milliseconds

# How often a worker with nothing to do asks again. This is the price of losing the
# blocking read, so it is the one number to look at if wakeups feel slow.
POLL = timedelta(milliseconds=50)

# Take the first workflow that is visible and push it a lease into the future, in one
# step, so two workers polling at the same instant cannot both take it. The clock is the
# server's, as it is for the checkpoint claim, because a lease compared against the
# taker's own clock is only as good as the agreement between the two.
#
# The half millisecond is what keeps the receipt a receipt. `done` removes the entry only
# when the score is still the one this take wrote, on the premise that anything wanting
# another pass writes a *different* score, and a score is a number rather than a version:
# a deadline that happens to land on the same millisecond as this lease is the same
# number, so the comparison passes and the acknowledgement throws that wakeup away. It is
# not a remote coincidence either, since `Run.sleep(key, LEASE)` under a scheduler holding
# the same `LEASE` computes a deadline a millisecond or two from the take.
#
# Only a take writes a half, and every other writer here writes whole milliseconds
# (`score`), so the two can no longer collide. Half a millisecond of extra invisibility is
# not a number anything else is measured against.
#
#   KEYS[1]  the schedule
#   ARGV[1]  lease, in milliseconds
#   returns  {workflow, receipt}, where the receipt is the score just written,
#            or nil if nothing is visible yet
TAKE = """
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now_ms, 'LIMIT', 0, 1)
if #due == 0 then return nil end
local held_until = now_ms + tonumber(ARGV[1]) + 0.5
redis.call('ZADD', KEYS[1], held_until, due[1])
return {due[1], string.format('%.1f', held_until)}
"""

# Finish, but only if nothing asked for another pass in the meantime. Anything that did
# wrote a different score, so the comparison is the whole check.
#
#   KEYS[1]  the schedule
#   ARGV[1]  the workflow
#   ARGV[2]  the receipt, which is the score this pass took
#   returns  1 if the workflow was dropped, 0 if it was left for another pass
DONE = """
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if score and tonumber(score) == tonumber(ARGV[2]) then
  redis.call('ZREM', KEYS[1], ARGV[1])
  return 1
end
return 0
"""

# Suspend until a deadline, under the same comparison and for the same reason. A workflow
# holds one entry here, so writing the deadline unconditionally would land on top of a
# `make_ready` that arrived while the pass was ending and push a confirmation out to a
# deadline that may be days away. Anything that asked for another pass wrote a different
# score, and this leaves that alone: the wakeup it wanted is the sooner of the two, and
# the deadline is recorded in the checkpoint, so the pass that runs writes it again.
#
#   KEYS[1]  the schedule
#   ARGV[1]  the workflow
#   ARGV[2]  the receipt, which is the score this pass took
#   ARGV[3]  when the workflow should next be visible
#   returns  1 if the deadline was written, 0 if a fresher wakeup was left in place
SUSPEND = """
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if score and tonumber(score) == tonumber(ARGV[2]) then
  redis.call('ZADD', KEYS[1], tonumber(ARGV[3]), ARGV[1])
  return 1
end
return 0
"""


@dataclass(frozen=True, slots=True)
class RedisSetScheduler:
    """
    `Scheduler` as a single sorted set scored by when each workflow becomes visible.

    A drop-in for `RedisStreamScheduler`: the same protocol, the same worker, one structure
    instead of two. Like the other Redis stores here, the client MUST be built with
    `decode_responses=True`.

    Two methods do nothing, and that is the finding rather than an omission. `prepare`
    has nothing to create, because a sorted set needs no consumer group. `wake_due` has
    nothing to move, because being due and being ready are the same score. `reclaim`
    likewise returns nothing: an abandoned workflow is picked up by `next_ready` along
    with everything else, since its lease elapsing is indistinguishable from a deadline
    arriving, and treating them the same is the point.
    """

    redis: Redis
    namespace: str = "workflow"
    # How long a taken workflow stays invisible, and so how long after a worker dies
    # before someone else picks its workflow up. `worker.work` reads it and claims the
    # workflow for the same span, which is the whole reason it is one number: a
    # workflow that becomes visible before its claim lapses is taken by a worker that
    # cannot write to it yet.
    lease: timedelta = LEASE
    poll: timedelta = POLL
    # Only `make_ready` reads it: "visible now" is the one score a caller names, where
    # the lease is measured by the server (in `TAKE`) and a deadline was chosen by the
    # workflow itself. Injected so a test can place a wakeup in a clock it controls.
    now: Callable[[], datetime] = now_utc
    # One field per script rather than a tuple read by index, so the name a call site uses
    # is checked against the script it was registered with instead of agreeing by position.
    take: AsyncScript = field(init=False, repr=False, compare=False)
    finish: AsyncScript = field(init=False, repr=False, compare=False)
    suspend: AsyncScript = field(init=False, repr=False, compare=False)
    # The two durations as the numbers the wire and `asyncio.sleep` actually want,
    # rendered once at construction. Both are read inside `next_ready`'s poll loop, which
    # is the one place here that runs more than once per unit of work.
    lease_ms: int = field(init=False, repr=False, compare=False)
    poll_seconds: float = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        check_duration("a lease", self.lease)
        check_duration("a poll interval", self.poll)
        object.__setattr__(self, "take", self.redis.register_script(TAKE))
        object.__setattr__(self, "finish", self.redis.register_script(DONE))
        object.__setattr__(self, "suspend", self.redis.register_script(SUSPEND))
        # Rounded up rather than truncated: a lease under a millisecond would otherwise be
        # sent as zero, which writes a score of *now* on the workflow it just took, so
        # every worker polling takes the same delivery with the same receipt and the first
        # `done` drops the entry out from under the rest (see `units`).
        object.__setattr__(self, "lease_ms", milliseconds(self.lease))
        object.__setattr__(self, "poll_seconds", self.poll.total_seconds())

    @property
    def schedule_key(self) -> str:
        return f"{self.namespace}:schedule"

    async def prepare(self) -> None:
        """Nothing to create: a sorted set is its own queue."""

    async def make_ready(self, workflow: str) -> None:
        """
        Make the workflow visible now, whatever it was waiting for before.

        A plain write rather than a conditional one, including over a pass in flight.
        Landing on top of a running pass's score is what keeps the wakeup alive (that
        pass will now decline to remove the entry), and landing on top of a *deadline*
        is correct too: the workflow wakes, finds its wait unfinished, and reschedules
        itself.
        """
        await self.redis.zadd(self.schedule_key, {workflow: score(self.now())})

    async def wake_at(self, delivery: Delivery, when: datetime) -> None:
        """
        Suspend the workflow until `when`, unless something asked for a pass meanwhile.

        The receipt is the score this pass took, so anything that rescheduled the workflow
        since (a confirmation, another worker taking over an overrun) wrote a different
        one and this leaves it be. Which is the right answer rather than a concession: the
        deadline lives in the workflow's checkpoint, so the pass that runs sooner reaches
        the same `sleep` and writes it again.
        """
        await self.suspend(
            keys=[self.schedule_key],
            args=[delivery.workflow, delivery.receipt, score(when)],
        )

    async def wake_due(self, now: datetime) -> tuple[str, ...]:
        """Nothing to do: a workflow whose score has passed is already visible."""
        return ()

    async def next_ready(self, within: timedelta) -> Delivery | None:
        """
        The next visible workflow, waiting up to `within` for one to appear.

        Polling, because a sorted set has no blocking read. `within` bounds how long a
        cancelled worker sits here before it can notice, exactly as the blocking read's
        argument did, but here it is also spent in round trips rather than in one parked
        call, which is the cost of this whole design.
        """
        deadline = monotonic() + within.total_seconds()
        while True:
            taken = cast(list[str] | None, await self.take(keys=[self.schedule_key], args=[self.lease_ms]))
            if taken is not None:
                workflow, receipt = taken
                return Delivery(workflow=workflow, receipt=receipt)
            remaining = deadline - monotonic()
            if remaining <= 0:
                return None
            await asyncio.sleep(min(self.poll_seconds, remaining))

    async def reclaim(self, idle: timedelta) -> Delivery | None:
        """Nothing to take over by hand: an abandoned workflow becomes visible on its own."""
        return None

    async def done(self, delivery: Delivery) -> None:
        """
        Drop the workflow, unless something asked for another pass while this one ran.

        The receipt is the score this pass took, so anything that rescheduled the
        workflow meanwhile (a confirmation, this pass's own `wake_at`, another worker
        taking over an overrun) wrote a different one and this leaves it alone. That is
        why a worker may call `wake_at` and then `done` in that order without the second
        undoing the first.
        """
        await self.finish(keys=[self.schedule_key], args=[delivery.workflow, delivery.receipt])


def score(when: datetime) -> int:
    """
    A moment as this set's score, which is when the workflow becomes visible.

    Whole milliseconds, which is half of what keeps a receipt unambiguous: a take writes
    the only fractional scores here (see `TAKE`), so no deadline a caller names can be
    mistaken for the lease a pass is holding.
    """
    return int(when.timestamp() * 1000)
