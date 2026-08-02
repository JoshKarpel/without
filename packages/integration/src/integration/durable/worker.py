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
from without import limit_concurrency

from integration.durable.payout import Payout
from integration.durable.payout import Payouts
from integration.durable.payout import parse_items
from integration.durable.payout import pay_out
from integration.durable.shell import Checkpoints
from integration.durable.stepwise import Run
from integration.durable.stepwise import Suspended
from integration.durable.stepwise import now_utc
from integration.durable.stepwise import resume
from integration.durable.wakeups import Delivery
from integration.durable.wakeups import Wakeups

logger = logging.getLogger(__name__)

SETTLING = timedelta(seconds=1)
APPROVAL_OVER = 10_000
TICK = timedelta(milliseconds=50)
BLOCKING = timedelta(seconds=1)
# How many workflows one worker runs at once. Passes are almost all waiting on someone
# else (a gateway, the store), so the useful number is far above the core count and is
# bounded by what the dependencies will take rather than by this process.
POOL = 20
# How long a delivery may go unacknowledged before another worker takes it over, and how
# long a claim on a workflow is good for. One constant for both because they answer the
# same question, how long a pass may honestly take, and because a reclaim that beats the
# workflow claim to expiring just hands the work to someone who cannot yet do it. A
# minute is generous for passes whose steps are single calls.
LEASE = timedelta(minutes=1)
# How long a worker that lost the claim waits before looking again. Long enough that the
# pass holding it has a fair chance to finish (and to make this wakeup redundant), short
# enough that a claim dropped immediately afterwards is not left sitting.
CONTENDED = timedelta(seconds=1)


def submitting(
    *,
    settling: timedelta = SETTLING,
    approval_over: int = APPROVAL_OVER,
) -> Callable[[Run], Awaitable[Payout]]:
    """
    The workflow this deployment runs: a payout over whatever the API submitted.

    The order arrives as a *recorded value* rather than an argument, because the
    worker is handed a workflow id and nothing else. `awaiting` is the same call a
    human confirmation uses: an order that has not been submitted is simply one whose
    first value has not landed yet, so the worker needs no separate notion of "this
    workflow has not started".

    The two knobs are arguments rather than constants read inside so a test can run
    the same body against a settlement window it does not have to wait out.
    """

    async def body(run: Run) -> Payout:
        items = parse_items(await run.awaiting("order"))
        return await pay_out(run, run.workflow, in_memory(items), settling=settling, approval_over=approval_over)

    return body


submitted = submitting()


def in_memory(items: dict[str, int]) -> Payouts:
    """
    The payout's effects, standing in for a gateway and a warehouse.

    `items` hands back what the request carried; in a real system it reads the order
    table, which is why `pay_out` keeps it a step at all rather than an argument: the
    read can fail, and its result is worth recording.
    """

    async def line_items(order_id: str) -> dict[str, int]:
        return items

    async def capture(sku: str, amount: int) -> str:
        return f"cap-{sku}-{amount}"

    async def pay(order_id: str, total: int) -> str:
        return f"pay-{order_id}-{total}"

    return Payouts(items=line_items, capture=capture, pay=pay)


def passes(
    checkpoints: Checkpoints,
    wakeups: Wakeups,
    body: Callable[[Run], Awaitable[object]] = submitted,
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
    """

    async def advance(delivery: Delivery) -> None:
        holder = await checkpoints.claim(delivery.workflow, lease)
        if holder is None:
            # Someone else is mid-pass. Whatever this wakeup carried is in the store
            # already, so the pass in flight may cover it; ask again shortly rather than
            # blocking a slot, and answer for the delivery so it is not reclaimed too.
            logger.info(f"{delivery.workflow} is held by another pass; looking again in {contended}")
            await wakeups.wake_at(delivery.workflow, now() + contended)
            await wakeups.done(delivery)
            return
        try:
            await resume(holder, checkpoints, body)
        except Suspended as pause:
            if pause.due is not None:
                await wakeups.wake_at(delivery.workflow, pause.due)
            logger.info(f"{delivery.workflow} suspended at {pause.key}")
        except Exception as error:  # noqa: BLE001 - a workflow's failure is data here, not a fault in the loop
            logger.warning(f"{delivery.workflow} failed: {error!r}")
        finally:
            await checkpoints.release(holder)
        await wakeups.done(delivery)

    async def pool(deliveries: Stream[Delivery]) -> None:
        async for finished in limit_concurrency((advance(delivery) async for delivery in deliveries), limit):
            finished.result()  # a pass swallows its workflow's failure; anything left is the loop's

    return pool


async def ready(
    wakeups: Wakeups,
    within: timedelta = BLOCKING,
    idle: timedelta = LEASE,
) -> AsyncGenerator[Delivery]:
    """
    The stream of wakeups this worker should act on: taken over, then new.

    A source stream like any other, so everything downstream is ordinary wiring, and
    swapping Redis for another queue changes this function alone. It merges the two
    sources because `reclaim` assigns a dead worker's delivery to *this* one, which
    obliges it to run it.

    Every pull answers the same question: is there work someone abandoned, and if not,
    is there anything new? One of each, never a batch, so a pull is always exactly the
    one delivery the caller has a slot for. Abandoned work goes first because it has
    been waiting the longest, and it is bounded work: the pending list is ordinarily
    empty, so the blocking read is what paces the loop.
    """
    while True:
        delivery = await wakeups.reclaim(idle)
        if delivery is None:
            delivery = await wakeups.next_ready(within)
        if delivery is not None:
            yield delivery


async def waking(wakeups: Wakeups, *, tick: timedelta = TICK, now: Callable[[], datetime] = now_utc) -> None:
    """
    The control plane: make every workflow whose deadline has passed ready again.

    Runs on its own tick rather than on traffic, because whether a wait is over is a
    question about the clock and not about the request that happens to arrive. Safe to
    run in every worker, and safe to be killed at any point in it, because the move is
    the store's single operation rather than this loop's two (see `wake_due`).

    Whether it does anything is the queue's business. Over `RedisWakeups` this is what
    carries a workflow from the sleepers to the stream; over `RedisSchedule` it spins
    against a no-op, because there being due and being ready are one score and nothing
    has to move. The worker runs it either way rather than asking which queue it has.
    """
    while True:
        await wakeups.wake_due(now())
        await asyncio.sleep(tick.total_seconds())


async def work(
    checkpoints: Checkpoints,
    wakeups: Wakeups,
    *,
    tick: timedelta = TICK,
    idle: timedelta = LEASE,
    limit: int = POOL,
    body: Callable[[Run], Awaitable[object]] = submitted,
    now: Callable[[], datetime] = now_utc,
) -> None:
    """
    Run the worker: the timer alongside the pass loop, until cancelled.

    Both halves live for the process, so they are a task group rather than a
    foreground and a background: cancelling either (a shutdown, a failed timer) takes
    the other down with it instead of leaving a worker that runs passes nobody wakes.
    `prepare` first, because reading a queue takes setup that writing to it does not.
    """
    await wakeups.prepare()

    async def advance_ready() -> None:
        await passes(checkpoints, wakeups, body, limit, lease=idle, now=now)(ready(wakeups, idle=idle))

    async with asyncio.TaskGroup() as group:
        group.create_task(waking(wakeups, tick=tick, now=now))
        group.create_task(advance_ready())
