# The queue worker: one pass per ready workflow, plus the timer that makes a slept-out
# workflow ready again. It is a `Sink` over a `Stream` and a background task, which is
# the whole shape:
#
#   deliveries ──▶ pool of N passes ──▶ Suspended? ──▶ wake_at (a clock)
#         ▲                          │             └──▶ nothing (a confirmation)
#         │                          └──▶ done (this wakeup is answered for)
#   reclaim one, else read one
#   timer ──▶ wake_due (one move, in the store)
#
# The two arms of the `Suspended` branch are the two ways a workflow waits, and the
# value says which: a `due` means the workflow chose the wait itself, so the worker
# schedules the wakeup; no `due` means it is waiting on the outside world, so nobody
# schedules anything and the API's confirmation is what queues it. Nothing polls a
# workflow to ask whether it can proceed.
#
# Acknowledging is the crash-safety half. A delivery stays outstanding in the store
# until `done`, so the ack goes *after* the pass and after its error handling, on every
# path this process actually observed: a completed pass, a suspended one, and a workflow
# whose step raised are all outcomes it saw and can answer for. Cancellation is the one
# path that skips the ack, deliberately: a worker shutting down mid-pass has not
# finished the work, so leaving the delivery outstanding is what lets another worker
# reclaim it. That is why the ack is not in a `finally`.
#
# The delivery stream merges two sources, which is where fan-in belongs: new work, and
# work a dead worker was holding. `reclaim` assigns the latter to *this* worker, so
# whoever claims them must also run them, and yielding them into the same sink is
# exactly that.
#
# Backpressure comes free with that shape, and is worth naming because it is the point
# of driving a queue as a `Stream`. The pool pulls from a *lazy* source only when a slot
# frees, and every pull takes exactly one delivery, whether it comes from the abandoned
# pile or the stream. So a worker holds precisely as many wakeups as it is working on,
# never more: at `limit` passes in flight the reads simply stop, the work stays in the
# stream where another worker can take it, and nothing buffers ahead. "Pull one at a
# time" and "run twenty at a time" are the same sentence.
#
# The timer is control plane and the passes are data plane, so the timer runs on its
# own tick rather than being triggered by traffic. It is safe in every worker at once
# (see `wake_due`).
#
# Two claims, not one, and they answer different questions. The delivery is claimed from
# the queue, which decides who owes an answer for this *wakeup*; the workflow is claimed
# from the checkpoint store, which decides who may write to it. Only the second is a
# safety property. Two `make_ready` calls for one workflow put two entries in the stream
# and the group hands them to two workers, which is ordinary rather than exceptional
# (the submit-then-confirm flow does it every time), so the workflow claim is what stops
# both of them running the same unrecorded step. The loser does not queue behind the
# winner: it schedules itself a little later and lets go, because the pass in flight may
# well finish the work, and holding a slot to find out would spend the pool on waiting.
#
# Losing that claim has two arrival times and one answer. It is refused up front when
# someone already holds the workflow, and it surfaces mid-pass as `Fenced` when this
# pass's lease lapsed and another worker took over between two steps. Both mean the same
# thing (somebody else is advancing this workflow), so both take the same path out, and
# neither is a failure to warn about.
#
# There is still no retry policy: a workflow whose step raises is logged and
# acknowledged, and comes back only when something wakes it.

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import datetime
from datetime import timedelta

from without import Sink
from without import Stream
from without import from_sink
from without import limit_concurrency
from without import ticks

from without_durability.seams import LEASE
from without_durability.seams import Contended
from without_durability.seams import Delivery
from without_durability.seams import Durable
from without_durability.seams import Fenced
from without_durability.seams import Scheduler
from without_durability.seams import check_duration
from without_durability.stepwise import Run
from without_durability.stepwise import Suspended
from without_durability.stepwise import now_utc
from without_durability.stepwise import resume

logger = logging.getLogger(__name__)

TICK = timedelta(milliseconds=50)
BLOCKING = timedelta(seconds=1)
# How many workflows one worker runs at once. Passes are almost all waiting on someone
# else (a gateway, the store), so the useful number is far above the core count and is
# bounded by what the dependencies will take rather than by this process.
POOL = 20
# How long a worker that lost the claim waits before looking again. Long enough that the
# pass holding it has a fair chance to finish (and to make this wakeup redundant), short
# enough that a claim dropped immediately afterwards is not left sitting.
CONTENDED = timedelta(seconds=1)


def passes(
    durable: Durable,
    body: Callable[[Run], Awaitable[object]],
    limit: int = POOL,
    *,
    lease: timedelta = LEASE,
    contended: timedelta = CONTENDED,
    now: Callable[[], datetime] = now_utc,
) -> Sink[Delivery]:
    """
    The data plane: up to `limit` passes at once, and one delivery pulled per free slot.

    A `Sink` because a pass produces nothing another stage consumes; what it produces
    is recorded. A pass that raises is logged rather than propagated, since a workflow
    failing is this service's *data* (a gateway declined) and not a bug in the loop that
    ran it, and taking the worker down over one workflow would stop every other. What
    does propagate is a failure of the loop itself (the store refusing an ack), because
    that is not something to keep running through.

    The pool is `limit_concurrency` over a *lazy* mapping of the delivery stream, which
    is what keeps "pull one at a time" and "run twenty at a time" the same statement:
    the generator that turns a delivery into a pass is only advanced when a slot frees,
    so the queue is never read past what this worker can start. A worker holds `limit`
    unacknowledged deliveries at most, and the rest stay in the stream where another
    worker can take them.

    The acknowledgement comes last, on every path this process saw through: a completed
    pass, a suspended one, a failed one, and a contended one are all answers. Only
    cancellation skips it, which is the point, since a half-run pass should be reclaimed
    rather than forgotten. Releasing the claim is not on that list: it happens on the way
    out of every path including cancellation, because a shutting-down worker that keeps
    its claim makes every other worker wait out the lease for nothing.

    Losing the workflow is handled by name rather than falling into the failure arm, and
    it has to be: `Fenced` and `Contended` are `Interruption`s, so `except Exception` no
    longer reaches them, and treating them as failures was never right anyway. They say
    another pass owns the workflow, which is the same thing a refused claim says and gets
    the same answer, so the two paths share `look_again`. What they do not get is a
    warning, because nothing went wrong.
    """
    check_duration("a lease", lease)
    check_duration("a contended interval", contended)

    async def look_again(delivery: Delivery, why: str) -> None:
        """Hand the workflow back to whoever holds it, and ask for another look shortly."""
        logger.info(f"{delivery.workflow} {why}; looking again in {contended}")
        await durable.scheduler.wake_at(delivery.workflow, now() + contended)
        await durable.scheduler.done(delivery)

    async def advance(delivery: Delivery) -> None:
        holder = await durable.checkpointer.claim(delivery.workflow, lease)
        if holder is None:
            # Someone else is mid-pass. Whatever this wakeup carried is in the store
            # already, so the pass in flight may cover it; ask again shortly rather than
            # blocking a slot, and answer for the delivery so it is not reclaimed too.
            await look_again(delivery, "is held by another pass")
            return
        try:
            await resume(holder, durable.checkpointer, body)
        except Suspended as pause:
            if pause.due is not None:
                await durable.scheduler.wake_at(delivery.workflow, pause.due)
            logger.info(f"{delivery.workflow} suspended at {pause.key}")
        except (Fenced, Contended) as lost:
            # The claim lapsed mid-pass and someone else took the workflow, which is the
            # same situation as losing it outright, discovered later. So it gets the same
            # answer rather than being logged as a failure: the winner is advancing the
            # workflow, and this asks again in case it does not finish. Releasing is left
            # to the `finally`, where it is the no-op it has to be, since the whole
            # meaning of `Fenced` is that this pass's token no longer matches.
            await look_again(delivery, f"was taken over mid-pass ({lost!r})")
            return
        except Exception as error:  # noqa: BLE001 - a workflow's failure is data here, not a fault in the loop
            logger.warning(f"{delivery.workflow} failed: {error!r}")
        finally:
            await durable.checkpointer.release(holder)
        await durable.scheduler.done(delivery)

    async def pool(deliveries: Stream[Delivery]) -> None:
        async for finished in limit_concurrency((advance(delivery) async for delivery in deliveries), limit):
            finished.result()  # a pass swallows its workflow's failure; anything left is the loop's

    return pool


async def ready(
    scheduler: Scheduler,
    within: timedelta = BLOCKING,
    idle: timedelta = LEASE,
) -> AsyncGenerator[Delivery]:
    """
    The stream of scheduler this worker should act on: taken over, then new.

    A source stream like any other, so everything downstream is ordinary wiring, and
    swapping one queue for another changes this function alone. It merges the two
    sources because `reclaim` assigns a dead worker's delivery to *this* one, which
    obliges it to run it.

    Every pull answers the same question: is there work someone abandoned, and if not,
    is there anything new? One of each, never a batch, so a pull is always exactly the
    one delivery the caller has a slot for. Abandoned work goes first because it has
    been waiting the longest, and it is bounded work: the pending list is ordinarily
    empty, so the blocking read is what paces the loop.

    `idle` is deliberately not checked for being positive as the other durations are: it
    is a threshold rather than an interval, and a zero one is meaningful (take over
    anything outstanding, however recently it was delivered).
    """
    check_duration("a blocking read", within)
    while True:
        delivery = await scheduler.reclaim(idle)
        if delivery is None:
            delivery = await scheduler.next_ready(within)
        if delivery is not None:
            yield delivery


def waking(scheduler: Scheduler) -> Sink[datetime]:
    """
    The control plane: make every workflow whose deadline has passed ready again.

    A `Sink` over a stream of moments rather than its own timer, so *when* it runs is the
    caller's to decide and this only says what happens each time. Driven off `ticks` in
    `work`; driven off a list in a test.

    Safe to run in every worker, and safe to be killed at any point in it, because the
    move is the store's single operation rather than this sink's two (see `wake_due`).

    Whether it does anything is the queue's business. Over a stream beside a sorted set
    this is what carries a workflow from the sleepers to the queue; over a single
    structure scored by visibility it spins against a no-op, because being due and being
    ready are then the same score and nothing has to move. The worker runs it either way
    rather than asking which queue it has.
    """

    async def wake(moment: datetime) -> None:
        await scheduler.wake_due(moment)

    return from_sink(wake)


async def work(
    durable: Durable,
    body: Callable[[Run], Awaitable[object]],
    *,
    tick: timedelta = TICK,
    within: timedelta = BLOCKING,
    contended: timedelta = CONTENDED,
    limit: int = POOL,
    now: Callable[[], datetime] = now_utc,
) -> None:
    """
    Run the worker: the timer alongside the pass loop, until cancelled.

    Both halves live for the process, so they are a task group rather than a
    foreground and a background: cancelling either (a shutdown, a failed timer) takes
    the other down with it instead of leaving a worker that runs passes nobody wakes.
    `prepare` first, because reading a queue takes setup that writing to it does not.

    Both halves are also the same shape, which is the point of the vocabulary: a sink
    over a stream. One consumes deliveries, the other consumes moments, and a deployment
    that wants a third (trimming a Redis stream, sweeping old checkpointer) adds a task to
    this group rather than a mechanism.

    The lease is the scheduler's rather than an argument here, and it is the one number
    that is *not* a knob on this call. It bounds two things that have to agree (how long
    a delivery stays this worker's, and how long its claim on the workflow is good for),
    and the queue is where the first one already lives: a visibility-scored store writes
    it into the row it takes. Reading it back and claiming for exactly as long is what
    keeps a store constructed with a ten-minute lease from being reclaimed after one.
    Turning it means `PostgresScheduler(pool, lease=...)`, which is also where the
    matching `poll` and the store's own timings are set, so the passes a deployment can
    honestly run are described in one place.
    """
    # Up front, rather than leaving each to the function that spends it, so a bad timing
    # is a `ValueError` from this call instead of an `ExceptionGroup` out of the task
    # group below. The lease is checked where the scheduler was built.
    check_duration("a tick", tick)
    check_duration("a blocking read", within)
    check_duration("a contended interval", contended)
    await durable.scheduler.prepare()
    lease = durable.scheduler.lease

    async def sweep_due() -> None:
        await waking(durable.scheduler)(ticks(tick, now=now))

    async def advance_ready() -> None:
        await passes(durable, body, limit, lease=lease, contended=contended, now=now)(
            ready(durable.scheduler, within=within, idle=lease)
        )

    async with asyncio.TaskGroup() as group:
        group.create_task(sweep_due())
        group.create_task(advance_ready())
