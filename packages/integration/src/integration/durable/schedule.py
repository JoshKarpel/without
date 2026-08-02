# The same `Wakeups`, as one sorted set instead of a stream beside a sorted set.
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
# the other are all gone, and so is the stream that nothing trims.
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
# sorted set has no blocking read, so this polls, and the poll interval is a floor under
# how fast anything starts. That is the trade in one line: a stream is cheaper to wait
# on, a sorted set is cheaper to reason about.

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

from integration.durable.stepwise import now_utc
from integration.durable.wakeups import Delivery

# How long a taken workflow stays invisible, and so how long after a worker dies before
# someone else picks its workflow up. The same reasoning as the checkpoint claim's lease:
# it has to exceed the longest a pass can honestly take.
LEASE = timedelta(minutes=1)
# How often a worker with nothing to do asks again. This is the price of losing the
# blocking read, so it is the one number to look at if wakeups feel slow.
POLL = timedelta(milliseconds=50)

# Take the first workflow that is visible and push it a lease into the future, in one
# step, so two workers polling at the same instant cannot both take it. The clock is the
# server's, as it is for the checkpoint claim, because a lease compared against the
# taker's own clock is only as good as the agreement between the two.
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
local held_until = now_ms + tonumber(ARGV[1])
redis.call('ZADD', KEYS[1], held_until, due[1])
return {due[1], string.format('%.0f', held_until)}
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


@dataclass(frozen=True, slots=True)
class RedisSchedule:
    """
    `Wakeups` as a single sorted set scored by when each workflow becomes visible.

    A drop-in for `RedisWakeups`: the same protocol, the same worker, one structure
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
    lease: timedelta = LEASE
    poll: timedelta = POLL
    # Only `make_ready` reads it: "visible now" is the one score a caller names, where
    # the lease is measured by the server (in `TAKE`) and a deadline was chosen by the
    # workflow itself. Injected so a test can place a wakeup in a clock it controls.
    now: Callable[[], datetime] = now_utc
    scripts: tuple[AsyncScript, ...] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        registered = tuple(self.redis.register_script(source) for source in (TAKE, DONE))
        object.__setattr__(self, "scripts", registered)

    @property
    def take(self) -> AsyncScript:
        return self.scripts[0]

    @property
    def finish(self) -> AsyncScript:
        return self.scripts[1]

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
        await self.redis.zadd(self.schedule_key, {workflow: milliseconds(self.now())})

    async def wake_at(self, workflow: str, when: datetime) -> None:
        await self.redis.zadd(self.schedule_key, {workflow: milliseconds(when)})

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
            await asyncio.sleep(min(self.poll.total_seconds(), remaining))

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

    @property
    def lease_ms(self) -> int:
        return int(self.lease.total_seconds() * 1000)


def milliseconds(when: datetime) -> int:
    return int(when.timestamp() * 1000)
