# The queue worker: one pass per ready workflow, plus the timer that makes a slept-out
# workflow ready again. It is a `Sink` over a `Stream` and a background task, which is
# the whole shape:
#
#   deliveries ──▶ pool of N passes ──▶ Sleeping  ──▶ wake_at (a clock)
#         ▲                          │  Blocked   ──▶ nothing (a write, from outside)
#         │                          │  Completed ──▶ nothing
#         │                          └──▶ done (this wakeup is answered for)
#   reclaim one, else read one
#   timer ──▶ wake_due (one move, in the store)
#
# The three arms are what one pass can come to, and `resume` hands them back as a sealed
# union rather than raising two of them: a `Sleeping` is a deadline the workflow chose, so
# the worker schedules it; a `Blocked` is every value and message the outside world owes
# it, so nobody schedules anything and the write that answers is what queues the workflow.
# Nothing polls a workflow to ask whether it can proceed.
#
# The delivery stream merges two sources, which is where fan-in belongs: new work, and
# work a dead worker was holding. `reclaim` assigns the latter to *this* worker, so
# whoever claims them must also run them, and yielding them into the same sink is
# exactly that. Backpressure then comes free, which is the point of driving a queue as a
# `Stream`: the pool pulls from a *lazy* source only when a slot frees, and every pull
# takes exactly one delivery, so at `limit` passes in flight the reads simply stop and
# the work stays where another worker can take it.
#
# The timer is control plane and the passes are data plane, so the timer runs on its own
# tick rather than being triggered by traffic. It is safe in every worker at once
# (see `wake_due`).
#
# Two claims, not one, and they answer different questions. The delivery is claimed from
# the queue, which decides who owes an answer for this *wakeup*; the workflow is claimed
# from the checkpoint store, which decides who may write to it. Only the second is a
# safety property, and it has to exist because two wakeups for one workflow are ordinary
# rather than exceptional: the submit-then-confirm flow produces them every time, and the
# group hands them to two workers.

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import suppress
from datetime import datetime
from datetime import timedelta
from typing import assert_never

from without_async import background_task
from without_async import limit_concurrency
from without_streams import Sink
from without_streams import Stream
from without_streams import from_sink
from without_streams import ticks

from without_durability.interfaces import BUDGET
from without_durability.interfaces import LEASE
from without_durability.interfaces import RENEWALS
from without_durability.interfaces import Contended
from without_durability.interfaces import Delivery
from without_durability.interfaces import Durable
from without_durability.interfaces import Fenced
from without_durability.interfaces import Scheduler
from without_durability.interfaces import check_duration
from without_durability.stepwise import Blocked
from without_durability.stepwise import Completed
from without_durability.stepwise import Outcome
from without_durability.stepwise import Run
from without_durability.stepwise import Sleeping
from without_durability.stepwise import extending
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
    budget: timedelta = BUDGET,
    contended: timedelta = CONTENDED,
    now: Callable[[], datetime] = now_utc,
) -> Sink[Delivery]:
    """
    The data plane: up to `limit` passes at once, and one delivery pulled per free slot.

    A `Sink` because a pass produces nothing another stage consumes; what it produces is
    recorded. A pass that raises is logged rather than propagated, since a workflow
    failing is this service's *data* (a gateway declined) and not a bug in the loop that
    ran it. What does propagate is a failure of the loop itself, such as the store
    refusing an ack.

    The pool is `limit_concurrency` over a *lazy* mapping of the delivery stream, which
    is what keeps "pull one at a time" and "run twenty at a time" the same statement: the
    generator that turns a delivery into a pass is only advanced when a slot frees, so
    the queue is never read past what this worker can start.

    The acknowledgement comes last, on every path this process saw through: a completed
    pass, a suspended one, a failed one, and a contended one are all answers. Only
    cancellation skips it, which is why it is not in a `finally`, since a half-run pass
    should be reclaimed rather than forgotten. Releasing the claim *is* attempted on
    every path including cancellation, because a shutting-down worker that keeps its
    claim makes every other worker wait out the lease for nothing. On that path the
    release is best effort: its `await` is a suspension point inside a task already being
    cancelled, so a second cancellation can interrupt it, and nothing is lost when it
    does because the claim expires with its lease anyway. That is why it is worth an
    attempt and not worth shielding.

    Losing the workflow is handled by name rather than falling into the failure arm, and
    it has to be, since `Fenced` and `Contended` are `Interruption`s that `except
    Exception` no longer reaches. They say another pass owns the workflow, which is what
    a refused claim says too, so the two paths share `look_again` and neither gets a
    warning. That they are still caught here while a suspension is not is the honest
    split: a suspension is something the pass *did*, so it comes back as a value, and
    losing the claim means there was no pass to have an outcome.
    """
    check_duration("a lease", lease)
    check_duration("a budget", budget)
    check_duration("a contended interval", contended)

    async def look_again(delivery: Delivery, why: str) -> None:
        """Hand the workflow back to whoever holds it, and ask for another look shortly."""
        logger.info(f"{delivery.workflow} {why}; looking again in {contended}")
        await durable.scheduler.wake_at(delivery, now() + contended)

    async def advance(taken: Delivery) -> None:
        # Rebound rather than reassigned, because `extend` renames a delivery on every store
        # whose receipt is the visibility it was taken under. Everything below answers for
        # the *current* name, and `keep_alive` waits out a renewal in flight before it
        # ends, so by the time anything here runs the name it reads is the one the store
        # holds rather than one a renewal was halfway through replacing.
        delivery = taken
        holder = await durable.checkpointer.claim(delivery.workflow, budget, lease)
        if holder is None:
            # Someone else is mid-pass. Whatever this wakeup carried is in the store
            # already, so the pass in flight may cover it; ask again shortly rather than
            # blocking a slot, and answer for the delivery so it is not reclaimed too.
            await look_again(delivery, "is held by another pass")
            return
        outcome: Outcome[object]
        running = asyncio.ensure_future(
            resume(
                holder,
                durable.checkpointer,
                body,
                extend=extending(durable.checkpointer, budget, lease, now=now),
                now=now,
            )
        )
        # Set by `keep_alive` and read by the handler below, because a cancelled `await`
        # cannot say who cancelled it and the two callers mean opposite things.
        lost_claim = False

        async def keep_alive() -> None:
            """
            Say this worker is still here, to both stores, for as long as the pass runs.

            Both halves on one tick because they answer the same question to two different
            readers: the claim decides who may write, the delivery decides who owes this
            wakeup, and a worker holding one without the other is either writing to a
            workflow somebody else has been handed or sitting on a wakeup it may not act
            on. Renewing them together is what keeps them lapsing together.

            This is what makes a pass longer than one `lease` ordinary rather than a
            mistake, and it is the *only* thing keeping the delivery alive across one: a
            single slow step makes no writes, so nothing else here would speak for minutes
            at a time.

            A tick that fails is logged and the next one tried, rather than ending the
            ticker: the tick *is* the retry, and `RENEWALS` per window is the margin that
            absorbs a missed one. Letting the error out would leave the pass running with
            nothing renewing it, to be taken over a window later, and would then surface
            the store's error at the end of a pass that was otherwise fine.

            The renewal of the delivery is shielded, and a cancellation that lands while
            it is in flight waits for it and takes its answer. On every store whose
            receipt is the visibility, the store has renamed the delivery the moment the
            statement commits, whether or not this task was still around to be told; a
            pass finishing at that moment would otherwise answer `done` under the old
            name, be silently declined, and be redelivered for nothing.
            """
            nonlocal lost_claim, delivery
            while True:
                await asyncio.sleep((lease / RENEWALS).total_seconds())
                try:
                    if not await durable.checkpointer.renew(holder, lease):
                        # Ending the pass here rather than leaving it to find out at its
                        # next write is the whole value of having asked: a step with an
                        # hour left would otherwise spend it on work that is already
                        # refused, and a step that will never return would otherwise
                        # never be ended at all. The delivery is deliberately not renewed
                        # on the way out, since whoever takes the workflow next is who
                        # should be handed it.
                        lost_claim = True
                        running.cancel()
                        return
                    renaming = asyncio.ensure_future(durable.scheduler.extend(delivery, lease))
                    try:
                        delivery = await asyncio.shield(renaming)
                    except asyncio.CancelledError:
                        while not renaming.done():
                            with suppress(asyncio.CancelledError):
                                await asyncio.wait([renaming])
                        if renaming.exception() is None:
                            delivery = renaming.result()
                        else:
                            logger.warning(
                                f"{delivery.workflow} could not renew its delivery: {renaming.exception()!r}"
                            )
                        raise
                except Exception as error:  # noqa: BLE001 - a missed tick is retried by the next one, not fatal
                    logger.warning(f"{delivery.workflow} could not be renewed: {error!r}; trying again next tick")

        try:
            async with background_task(keep_alive()):
                outcome = await running
        except asyncio.CancelledError:
            # This worker's or the world's, and only the flag tells them apart. One nobody
            # here asked for is a shutdown and belongs to whoever sent it; one `keep_alive`
            # sent is this worker discovering it no longer holds the workflow, whether
            # because somebody took it or because its budget ran out, which is the
            # `Fenced` case arriving early and gets the same answer.
            if not lost_claim:
                raise
            await look_again(delivery, "lost its claim mid-pass (superseded, or past its budget)")
            return
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
            # A pass raises for two very different reasons and this cannot tell them
            # apart: the workflow's own code failed, or the store it was reading and
            # writing was briefly unreachable, since `load` and every `record` raise
            # ordinary exceptions from inside the pass. So the delivery is left
            # *unanswered*, which is the only response that is right for both. A store
            # outage then costs a redelivery once the lease elapses rather than the
            # workflow, and a workflow that fails on its own code is retried on that same
            # interval instead of being dropped after one attempt with a log line.
            #
            # What that costs is stated rather than hidden: a workflow that fails on every
            # pass is retried for as long as it keeps failing, once per lease, and nothing
            # here backs that off or gives up. A deployment that needs a limit keeps the
            # count where it keeps everything else it needs to survive a crash, which is
            # the checkpoint.
            logger.warning(f"{delivery.workflow} failed: {error!r}")
            return
        finally:
            await durable.checkpointer.release(holder)
        # What a pass can come to, and the whole of what the worker owes each. A deadline
        # the workflow chose is the worker's to schedule; the values and messages the
        # outside world owes it are not, because no clock satisfies them and whoever
        # writes is what queues the workflow. `assert_never` is what makes this exhaustive
        # statically, so a further outcome would be a type error here rather than a
        # workflow that quietly stops being woken.
        #
        # Both arms answer for the delivery, and `wake_at` is one call rather than a
        # schedule and an acknowledgement, so there is no order to get right and no window
        # where a wakeup that arrived mid-pass can be overwritten by the deadline this
        # pass chose. Failing here propagates, because scheduling is the worker's own call
        # to its own queue: the delivery stays outstanding and another worker takes it.
        match outcome:
            case Sleeping(key=key, due=due):
                await durable.scheduler.wake_at(delivery, due)
                logger.info(f"{delivery.workflow} is sleeping at {key!r} until {due.isoformat()}")
            case Blocked() as blocked:
                await durable.scheduler.done(delivery)
                logger.info(f"{delivery.workflow} is blocked at {blocked.keys} until something writes")
            case Completed():
                await durable.scheduler.done(delivery)
            case _ as unreachable:
                assert_never(unreachable)

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
    The stream of deliveries this worker should act on: taken over, then new.

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
    budget: timedelta = BUDGET,
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
    that wants a third (trimming a Redis stream, sweeping old checkpoints) adds a task to
    this group rather than a mechanism.

    The lease is the scheduler's rather than an argument here, and it is the one number
    that is *not* a knob on this call. It bounds two things that have to agree (how long
    a delivery stays this worker's, and how long its claim on the workflow outlives the
    last word from it), and the queue is where the first one already lives: a
    visibility-scored store writes it into the row it takes. Reading it back and renewing
    both on that window is what keeps a store constructed with a ten-minute lease from
    being reclaimed after one. Turning it means `PostgresScheduler(pool, lease=...)`, which
    is also where the matching `poll` and the store's own timings are set, so the passes a
    deployment can honestly run are described in one place.

    `budget` is the other number and is deliberately *not* the scheduler's, because the
    queue has no opinion about it. It caps how long one pass may hold a workflow however
    alive it looks, so what it has to exceed is the longest a step can honestly take, where
    the lease has to exceed nothing at all and is sized for how fast a dead worker should
    be noticed. A step that knows better than this default says so with
    `Run.step(..., within=...)`, so this only has to cover the ones nobody annotated.
    """
    # Up front, rather than leaving each to the function that spends it, so a bad timing
    # is a `ValueError` from this call instead of an `ExceptionGroup` out of the task
    # group below. The lease is checked where the scheduler was built.
    check_duration("a tick", tick)
    check_duration("a blocking read", within)
    check_duration("a budget", budget)
    check_duration("a contended interval", contended)
    await durable.scheduler.prepare()
    lease = durable.scheduler.lease

    async def sweep_due() -> None:
        await waking(durable.scheduler)(ticks(tick, now=now))

    async def advance_ready() -> None:
        await passes(durable, body, limit, lease=lease, budget=budget, contended=contended, now=now)(
            ready(durable.scheduler, within=within, idle=lease)
        )

    async with asyncio.TaskGroup() as group:
        group.create_task(halves(sweep_due))
        group.create_task(halves(advance_ready))


async def halves(run: Callable[[], Awaitable[None]]) -> None:
    """
    Run one half of the worker, and let a cancellation nobody asked for end the worker.

    A task group is what makes the two halves live or die together, and it has one blind
    spot that matters here: a child that ends *cancelled* is not news to it, so the group
    exits that child and carries on with the other. That is right when the cancellation
    came from the group itself (a shutdown), and it is how a worker goes quietly
    half-dead otherwise. A workflow body that awaits something somebody else cancels
    raises `CancelledError` through the pass loop, which `advance` deliberately does not
    catch, and the worker is then a timer with nothing behind it: still running, still
    ticking, still reporting itself healthy, and advancing no workflow ever again. The
    queue backs up with nothing to alarm on but throughput.

    `cancelling()` is what tells the two apart, since it counts the cancellations aimed at
    *this* task. Nobody aimed one, so this is a workflow's `CancelledError` wearing the
    shutdown's clothes, and it is reported as the failure of the loop it is.
    """
    try:
        await run()
    except asyncio.CancelledError:
        current = asyncio.current_task()
        if current is not None and current.cancelling() == 0:
            raise RuntimeError(f"{run.__name__} was cancelled by something other than this worker") from None
        raise
