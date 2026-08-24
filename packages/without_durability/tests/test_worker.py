from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from time import monotonic

import pytest
from without import ticks
from without_durability import LEASE
from without_durability import Delivery
from without_durability import MemoryCheckpointer
from without_durability import MemoryScheduler
from without_durability import Run
from without_durability import SplitDurable
from without_durability import Suspended
from without_durability import claimed
from without_durability.worker import CONTENDED
from without_durability.worker import halves
from without_durability.worker import passes
from without_durability.worker import ready
from without_durability.worker import waking
from without_durability.worker import work

from .helpers import STARTED_AT
from .helpers import Clock
from .helpers import as_text

SMALL = {"widget": 1200, "gizmo": 800}
LARGE = {"piano": 90_000}
WORKFLOW = "wf-payout-1"
BRIEF = timedelta(milliseconds=10)
# Short enough that the assembled worker test waits it out for real. Every test that
# asserts on the suspension itself drives an injected `Clock` instead, because on the
# wall clock this window doubles as a budget for the pass: one slower than it reaches
# `sleep` with the deadline already behind, suspends at nothing, and runs to the end.
SETTLING = timedelta(milliseconds=20)
# Above this, the workflow stops for a human, which is the second way a pass suspends.
APPROVAL_OVER = 10_000


def settling_body(
    *,
    settling: timedelta = SETTLING,
    approval_over: int = APPROVAL_OVER,
) -> Callable[[Run], Awaitable[object]]:
    """
    A workflow that suspends both ways: on a clock, and on a value the world owes it.

    The worker's own tests need a body that reaches every branch of `advance`, and they
    should not reach for an application's workflow to get one. This is the smallest thing
    with a wait it schedules itself and a wait only somebody else can answer.
    """

    async def body(run: Run) -> object:
        items = await run.awaiting("order", as_amounts)
        total = await run.step("total", lambda: as_total(items), as_whole)
        await run.sleep("settling", settling)
        if total > approval_over:
            await run.awaiting("approved-by", as_text)
        return await run.step("paid", lambda: paid(run.workflow, total), as_text)

    return body


async def as_total(items: dict[str, int]) -> int:
    return sum(items.values())


# The parsers this body's four durable reads take. They are written out rather than
# reached for because parsing what a checkpoint holds is the *application's* job, and a
# test standing in for an application does it too.
def as_whole(recorded: object) -> int:
    if not isinstance(
        recorded, int
    ):  # pragma: no cover - the arm that makes this a parser rather than a cast; no test feeds it a bad value
        raise TypeError(f"{recorded!r} is not the whole number this step recorded")
    return recorded


def as_amounts(recorded: object) -> dict[str, int]:
    if not isinstance(
        recorded, dict
    ):  # pragma: no cover - the arm that makes this a parser rather than a cast; no test feeds it a bad value
        raise TypeError(f"{recorded!r} is not the order this workflow was waiting on")
    return {str(sku): as_whole(amount) for sku, amount in recorded.items()}


async def paid(workflow: str, total: int) -> str:
    return f"pay-{workflow}-{total}"


BODY = settling_body()


async def as_stream(*workflows: str) -> AsyncIterator[Delivery]:
    """The deliveries a queue would hand over, receipts and all."""
    for number, workflow in enumerate(workflows):
        yield Delivery(workflow=workflow, receipt=f"receipt-{number}")


def recording(ran: list[str]) -> Callable[[Run], Awaitable[None]]:
    """A workflow body that does nothing but say it ran, so a test can count passes."""

    async def counting(run: Run) -> None:
        ran.append(run.workflow)

    return counting


async def one_pass(
    checkpointer: MemoryCheckpointer,
    scheduler: MemoryScheduler,
    workflow: str = WORKFLOW,
    *,
    now: Clock,
) -> None:
    """
    Drive the worker's sink over exactly one delivered workflow, as the queue would.

    The clock is the caller's rather than the wall's, and required rather than defaulted,
    because `BODY` suspends on a deadline it computes from this clock and the pass only
    suspends while the deadline is still ahead of it. On the wall clock that makes the
    settling window a *budget for the pass itself*: a pass slower than the window runs
    straight through, schedules nothing, and the test reads it as a workflow that never
    waited. A clock the test moves has no such budget.
    """
    await passes(SplitDurable(checkpointer, scheduler), BODY, now=now)(as_stream(workflow))


async def test_a_workflow_nobody_has_submitted_waits_without_being_scheduled() -> None:
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()

    await one_pass(checkpointer, scheduler, now=Clock())

    assert await checkpointer.load(WORKFLOW) == {}
    assert scheduler.sleeping == {}, "a wait on a value has no deadline, so there is nothing to schedule"
    assert list(scheduler.queue) == []


async def test_a_submitted_order_runs_to_its_first_wait_and_schedules_the_wakeup() -> None:
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()
    await checkpointer.supply(WORKFLOW, "order", SMALL)

    await one_pass(checkpointer, scheduler, now=Clock())

    recorded = await checkpointer.load(WORKFLOW)
    assert set(recorded) == {"order", "total", "settling"}
    assert "paid" not in recorded, "the settlement window has not elapsed"
    # The worker read the deadline off `ScheduledWakeup` and handed it to the store's
    # sleeping set; nothing polls the workflow in the meantime.
    assert list(scheduler.sleeping) == [WORKFLOW]
    assert scheduler.sleeping[WORKFLOW] == datetime.fromisoformat(str(recorded["settling"]))


async def test_a_second_pass_after_the_wait_finishes_a_small_payout() -> None:
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()
    clock = Clock()
    await checkpointer.supply(WORKFLOW, "order", SMALL)

    await one_pass(checkpointer, scheduler, now=clock)
    clock.advance(SETTLING * 2)  # the workflow's own window, moved past rather than waited out

    assert await scheduler.wake_due(clock()) == (WORKFLOW,), "the timer finds it due and queues it again"
    await one_pass(checkpointer, scheduler, now=clock)

    assert (await checkpointer.load(WORKFLOW))["paid"] == f"pay-{WORKFLOW}-2000"


async def test_a_large_payout_stops_at_the_confirmation_and_resumes_once_it_is_recorded() -> None:
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()
    await checkpointer.supply(WORKFLOW, "order", LARGE)
    clock = Clock()

    await one_pass(checkpointer, scheduler, now=clock)
    clock.advance(SETTLING * 2)
    await scheduler.wake_due(clock())
    await one_pass(checkpointer, scheduler, now=clock)

    assert "paid" not in await checkpointer.load(WORKFLOW)
    assert scheduler.sleeping == {}, "the wait is on a person now, so this pass scheduled nothing"

    await checkpointer.supply(WORKFLOW, "approved-by", "auditor-7")
    await one_pass(checkpointer, scheduler, now=clock)

    assert (await checkpointer.load(WORKFLOW))["paid"] == f"pay-{WORKFLOW}-90000"


async def test_a_workflow_whose_step_raises_does_not_take_the_worker_down() -> None:
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()
    reached: list[str] = []

    async def declining(run: Run) -> None:
        reached.append(run.workflow)
        raise RuntimeError("the gateway declined")

    # Both workflows are consumed: the first one's failure is this service's data, so
    # the loop logs it and takes the next id rather than ending the worker.
    await passes(SplitDurable(checkpointer, scheduler), declining)(as_stream("wf-doomed", "wf-fine"))

    assert reached == ["wf-doomed", "wf-fine"]


async def test_a_wakeup_for_a_workflow_someone_else_is_passing_over_is_deferred_not_run() -> None:
    # The submit-then-confirm flow produces exactly this: a second wakeup arrives while
    # the first pass is still in flight. Running it would put two passes on one workflow,
    # both finding the same step unrecorded, so the second is scheduled to look again.
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()
    clock = Clock()
    ran: list[str] = []

    held = await claimed(checkpointer, WORKFLOW)
    try:
        await passes(SplitDurable(checkpointer, scheduler), recording(ran), now=clock)(as_stream(WORKFLOW))
    finally:
        await checkpointer.release(held)

    assert ran == [], "the claim was held elsewhere, so no pass ran"
    assert scheduler.sleeping == {WORKFLOW: STARTED_AT + CONTENDED}
    assert scheduler.outstanding == {}, "and the delivery was answered for rather than left to be reclaimed"


async def test_a_pass_whose_claim_lapsed_mid_run_is_deferred_rather_than_logged_as_a_failure() -> None:
    # Losing the workflow *during* a pass is the same situation as losing the claim
    # outright, found later, so it gets the same answer: hand it back and look again.
    # Treating it as a failed workflow would ack the wakeup and log a warning about a
    # workflow that is going fine in some other process.
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()
    clock = Clock()

    async def overtaken(run: Run) -> None:
        # Another worker takes the workflow while this pass is between two steps, which
        # is exactly what a lapsed lease produces.
        await checkpointer.release(run.holder)
        await claimed(checkpointer, run.workflow)
        await run.step("charged", lambda: paid(run.workflow, 1), as_text)

    await passes(SplitDurable(checkpointer, scheduler), overtaken, now=clock)(as_stream(WORKFLOW))

    assert await checkpointer.load(WORKFLOW) == {}, "the superseded pass wrote nothing"
    assert scheduler.sleeping == {WORKFLOW: STARTED_AT + CONTENDED}, "and asked to look again shortly"
    assert scheduler.outstanding == {}, "and answered for the delivery rather than leaving it to be reclaimed"


async def test_a_deferred_wakeup_runs_once_the_claim_is_free() -> None:
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()
    ran: list[str] = []

    await passes(SplitDurable(checkpointer, scheduler), recording(ran))(as_stream(WORKFLOW))

    assert ran == [WORKFLOW]
    assert scheduler.sleeping == {}, "nothing was deferred, because nothing else held the workflow"
    assert await checkpointer.claim(WORKFLOW, LEASE) is not None, "the pass let go of its claim on the way out"


async def test_the_pool_pulls_exactly_as_many_workflows_as_it_can_work_on() -> None:
    # The property that makes the queue safe to share: a worker never takes a wakeup it
    # has no slot for. With the pool full, the source is not advanced at all, so the
    # work stays where another worker can claim it instead of rotting in this one.
    limit = 3
    pulled = 0
    started = asyncio.Semaphore(0)
    release = asyncio.Event()

    async def endless() -> AsyncIterator[Delivery]:
        nonlocal pulled
        while True:
            pulled += 1
            yield Delivery(workflow=f"wf-{pulled}", receipt=f"receipt-{pulled}")

    async def blocking(run: Run) -> None:
        started.release()
        await release.wait()

    async def drive() -> None:
        await passes(SplitDurable(MemoryCheckpointer(), MemoryScheduler()), blocking, limit)(endless())

    running = asyncio.create_task(drive())
    try:
        for _ in range(limit):
            await started.acquire()
        for _ in range(5):  # give the driver every chance to over-pull
            await asyncio.sleep(0)

        assert pulled == limit, "a full pool stops reading, rather than queueing work it cannot start"

        release.set()
        for _ in range(limit):  # the next batch starts only as the first finishes
            await started.acquire()

        assert pulled == 2 * limit
    finally:
        running.cancel()


async def test_the_timer_makes_a_slept_out_workflow_ready_exactly_once() -> None:
    scheduler = MemoryScheduler()
    clock = Clock()
    # Suspended the way a pass suspends it: taken off the queue, then handed back with a
    # deadline. The event is what the timer's arrival is observed on below, so the one the
    # submission set is cleared first.
    await scheduler.make_ready(WORKFLOW)
    held = await scheduler.next_ready(BRIEF)
    assert held is not None
    await scheduler.wake_at(held, STARTED_AT + timedelta(seconds=30))
    scheduler.arrived.clear()

    async def sweep() -> None:
        await waking(scheduler)(ticks(BRIEF, now=clock))

    timer = asyncio.create_task(sweep())
    try:
        await asyncio.sleep(BRIEF.total_seconds() * 3)
        assert list(scheduler.queue) == [], "its deadline has not passed"

        clock.advance(timedelta(seconds=31))
        async with asyncio.timeout(3):
            await scheduler.arrived.wait()
    finally:
        timer.cancel()

    assert list(scheduler.queue) == [WORKFLOW]
    assert scheduler.sleeping == {}, "the move takes it off the sleepers, so a second tick cannot wake it twice"
    assert await scheduler.wake_due(clock()) == (), "and there is nothing left for another timer to find"


async def test_the_ready_stream_yields_each_queued_workflow() -> None:
    scheduler = MemoryScheduler()
    await scheduler.make_ready("wf-a")
    await scheduler.make_ready("wf-b")

    workflows = ready(scheduler, within=BRIEF, idle=LEASE)
    taken = [await anext(workflows), await anext(workflows)]
    await workflows.aclose()

    assert [delivery.workflow for delivery in taken] == ["wf-a", "wf-b"]
    assert len(scheduler.outstanding) == 2, "delivered, and nobody has answered for either yet"


async def test_the_ready_stream_takes_over_a_delivery_a_dead_worker_never_answered_for() -> None:
    # The crash the queue exists to survive: a worker takes a wakeup and dies before
    # acknowledging it. The delivery stays outstanding, and the next worker's stream
    # hands it back rather than leaving the workflow stuck forever.
    scheduler = MemoryScheduler()
    await scheduler.make_ready(WORKFLOW)
    abandoned = await scheduler.next_ready(BRIEF)

    assert abandoned is not None
    assert list(scheduler.queue) == [], "it is off the queue, so no new read will find it"

    workflows = ready(scheduler, within=BRIEF, idle=timedelta())  # nothing may go unanswered
    taken = await anext(workflows)
    await workflows.aclose()

    assert taken.workflow == WORKFLOW
    assert taken.receipt == abandoned.receipt, "the same delivery, taken over rather than duplicated"


async def test_the_worker_claims_a_workflow_for_the_lease_its_queue_hands_out() -> None:
    # The two windows that have to agree, and why the lease is the scheduler's rather than
    # an argument to `work`: a delivery that becomes reclaimable before its holder's claim
    # lapses goes to a worker that cannot write to it yet, which spends a whole pass
    # finding that out. The worker reads one number off the queue and claims for exactly
    # as long, so a store built with a longer lease is honoured without a second setting.
    lease = LEASE / 2
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler(lease=lease)
    holding = asyncio.Event()
    releasing = asyncio.Event()

    async def blocking(run: Run) -> None:
        holding.set()
        await releasing.wait()

    await scheduler.make_ready(WORKFLOW)
    worker = asyncio.create_task(work(SplitDurable(checkpointer, scheduler), tick=BRIEF, body=blocking))
    try:
        async with asyncio.timeout(3):
            await holding.wait()
        held_for = timedelta(seconds=checkpointer.held_until[WORKFLOW] - monotonic())
    finally:
        releasing.set()
        worker.cancel()

    # Whatever the runner spends between the worker writing the claim and this read comes
    # off `held_for`, and a loaded one spends far more than a tick there. The slack only has
    # to stay well short of the gap between the windows being told apart, so it is generous.
    slack = lease / 2
    assert lease - slack <= held_for <= lease, f"the claim runs to the queue's lease, not to {LEASE}"


@pytest.mark.parametrize("lease", [timedelta(), timedelta(seconds=-1)])
def test_a_scheduler_refuses_a_lease_that_is_not_a_positive_duration(lease: timedelta) -> None:
    # Checked where the value enters, because a lease is the one timing whose nonsense
    # value is silent: a claim that has already expired when it is granted excludes
    # nobody, so two passes run the same unrecorded step and nothing raises. A lease read
    # out of configuration is how a zero gets here.
    with pytest.raises(ValueError, match="a lease must be a positive duration"):
        MemoryScheduler(lease=lease)


@pytest.mark.parametrize(
    ("start", "refused"),
    [
        (lambda durable: work(durable, BODY, tick=timedelta()), "a tick"),
        (lambda durable: work(durable, BODY, within=timedelta(seconds=-1)), "a blocking read"),
        (lambda durable: work(durable, BODY, contended=timedelta()), "a contended interval"),
    ],
)
async def test_the_worker_refuses_a_timing_that_is_not_a_positive_duration(
    start: Callable[[SplitDurable], Awaitable[None]],
    refused: str,
) -> None:
    # Up front rather than inside the task group, so a mis-set duration is a `ValueError`
    # from the call that made it rather than an `ExceptionGroup` from somewhere in the
    # loop. None of these has a meaningful zero: each is a span to let pass.
    durable = SplitDurable(MemoryCheckpointer(), MemoryScheduler())

    with pytest.raises(ValueError, match=f"{refused} must be a positive duration"):
        await start(durable)


async def test_a_pass_loop_refuses_a_contended_interval_that_is_not_positive() -> None:
    # `passes` is drivable on its own, so it checks the two durations it is handed rather
    # than trusting whatever `work` already looked at.
    durable = SplitDurable(MemoryCheckpointer(), MemoryScheduler())

    with pytest.raises(ValueError, match="a contended interval must be a positive duration"):
        passes(durable, BODY, contended=timedelta())

    with pytest.raises(ValueError, match="a lease must be a positive duration"):
        passes(durable, BODY, lease=timedelta())


async def test_the_ready_stream_refuses_a_blocking_read_that_is_not_positive() -> None:
    # On the first pull rather than at the call, because a generator's body does not run
    # until it is advanced. `work` checks the same value up front, so the only caller who
    # sees it this late is one driving the stream directly, and it is still before the
    # first read of the queue. Its `idle` is deliberately not on this list: a threshold of
    # zero means "take over anything outstanding", which is a real thing to ask for.
    workflows = ready(MemoryScheduler(), within=timedelta())

    with pytest.raises(ValueError, match="a blocking read must be a positive duration"):
        await anext(workflows)


async def test_the_worker_defers_a_contended_wakeup_for_the_interval_it_was_given() -> None:
    # How long to wait before looking at a workflow somebody else holds is a deployment's
    # call (how long a pass usually takes, how much a redundant wakeup costs), so it is an
    # argument rather than a constant reachable only by calling `passes` directly.
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()
    clock = Clock()
    held = await claimed(checkpointer, WORKFLOW)
    await scheduler.make_ready(WORKFLOW)

    worker = asyncio.create_task(
        work(SplitDurable(checkpointer, scheduler), tick=BRIEF, body=BODY, contended=BRIEF, now=clock)
    )
    try:
        async with asyncio.timeout(3):
            while WORKFLOW not in scheduler.sleeping:
                await asyncio.sleep(BRIEF.total_seconds())
    finally:
        worker.cancel()
        await checkpointer.release(held)

    assert scheduler.sleeping == {WORKFLOW: STARTED_AT + BRIEF}, f"the interval it was given, not {CONTENDED}"


@dataclass(frozen=True, slots=True)
class RefusingWakeAt(MemoryScheduler):
    """The queue double, briefly unreachable for exactly the call under test."""

    async def wake_at(self, delivery: Delivery, when: datetime) -> None:
        raise ConnectionError("the queue was briefly unreachable")


async def delivered(delivery: Delivery) -> AsyncIterator[Delivery]:
    """One delivery a queue really handed out, receipt and all, as a stream of one."""
    yield delivery


async def test_a_queue_that_refuses_a_wakeup_fails_the_loop_rather_than_dropping_the_workflow() -> None:
    # Scheduling a slept-out workflow is the *worker's* call to its own queue, so its
    # failure is a failure of the loop exactly as a refused acknowledgement is, and not
    # the workflow failing. Swallowing it would be worse than either: the pass succeeded
    # and recorded its deadline, and the acknowledgement that follows would then leave a
    # workflow that is in no queue, holds no delivery, and nothing will ever wake.
    checkpointer = MemoryCheckpointer()
    scheduler = RefusingWakeAt()
    await checkpointer.supply(WORKFLOW, "order", SMALL)
    await scheduler.make_ready(WORKFLOW)
    taken = await scheduler.next_ready(BRIEF)
    assert taken is not None

    with pytest.raises(ConnectionError, match="the queue was briefly unreachable"):
        await passes(SplitDurable(checkpointer, scheduler), BODY, now=Clock())(delivered(taken))

    assert "settling" in await checkpointer.load(WORKFLOW), "the pass itself was fine; only the scheduling failed"
    assert list(scheduler.outstanding) == [taken.receipt], (
        "so the delivery is unanswered, and another worker will reclaim it"
    )


async def test_the_worker_measures_a_workflows_deadlines_on_the_clock_it_was_given() -> None:
    # The clock is injected so a test (or a simulation) can drive time, and it has to
    # reach every decision the worker makes about time, including the ones inside the
    # pass. A `sleep` computed against the wall clock and a `wake_due` compared against
    # the injected one never meet, so a seam that covers two of the three hangs the
    # workflow rather than controlling it.
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()
    await checkpointer.supply(WORKFLOW, "order", SMALL)

    await passes(SplitDurable(checkpointer, scheduler), BODY, now=Clock())(as_stream(WORKFLOW))

    assert scheduler.sleeping == {WORKFLOW: STARTED_AT + SETTLING}
    assert (await checkpointer.load(WORKFLOW))["settling"] == (STARTED_AT + SETTLING).isoformat()


async def test_the_worker_carries_a_submitted_order_through_its_own_wait() -> None:
    # The assembled thing: submitting is enough. The timer and the pass loop hand the
    # workflow back and forth until it is done, with nobody driving it.
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()
    await checkpointer.supply(WORKFLOW, "order", SMALL)
    await scheduler.make_ready(WORKFLOW)

    worker = asyncio.create_task(work(SplitDurable(checkpointer, scheduler), tick=BRIEF, within=BRIEF, body=BODY))
    try:
        async with asyncio.timeout(3):
            while "paid" not in await checkpointer.load(WORKFLOW):
                await asyncio.sleep(BRIEF.total_seconds())
    finally:
        worker.cancel()

    assert (await checkpointer.load(WORKFLOW))["paid"] == f"pay-{WORKFLOW}-2000"


async def test_a_body_driven_without_the_worker_suspends_the_same_way() -> None:
    # Driving the body directly, below `resume`, is where the suspension is still an
    # exception: that is how it unwinds straight-line code. `resume` is the boundary that
    # turns it into a `Sleeping`, which is what every driver above it matches on.
    checkpointer = MemoryCheckpointer()
    await checkpointer.supply(WORKFLOW, "order", SMALL)
    run = Run(
        holder=await claimed(checkpointer, WORKFLOW),
        checkpointer=checkpointer,
        recorded=await checkpointer.load(WORKFLOW),
        now=Clock(),
    )

    with pytest.raises(Suspended, match="suspended at 'settling'"):
        await BODY(run)

    assert (await checkpointer.load(WORKFLOW))["total"] == sum(SMALL.values())


async def test_a_pass_that_raised_leaves_its_wakeup_for_another_worker() -> None:
    # A pass raises for two very different reasons and the worker cannot tell them apart:
    # the workflow's own code failed, or the store it was reading was briefly unreachable,
    # since `load` and every `record` raise ordinary exceptions from inside the pass.
    # Answering for the delivery would be right for the first and permanent workflow loss
    # for the second, so it is left unanswered and comes back when the lease elapses.
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()

    async def declining(run: Run) -> None:
        raise RuntimeError("the gateway declined")

    await scheduler.make_ready(WORKFLOW)
    taken = await scheduler.next_ready(BRIEF)
    assert taken is not None

    await passes(SplitDurable(checkpointer, scheduler), declining)(delivered(taken))

    assert list(scheduler.outstanding) == [taken.receipt], "the wakeup outlives the pass that could not use it"
    assert await scheduler.reclaim(timedelta()) is not None, "so another worker takes it over"


async def test_a_wakeup_that_arrives_while_a_pass_is_ending_is_not_pushed_out_to_its_deadline() -> None:
    # The window this closes: a pass decides to sleep, and a confirmation lands before the
    # deadline is written. Scheduling and acknowledging as two calls made that a
    # read-modify-write over a value somebody else had just written, so on a store holding
    # one entry per workflow the confirmation was overwritten by a deadline days away.
    # `wake_at` takes the delivery, so the store can tell the two apart.
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()
    await checkpointer.supply(WORKFLOW, "order", SMALL)
    await scheduler.make_ready(WORKFLOW)
    taken = await scheduler.next_ready(BRIEF)
    assert taken is not None

    await scheduler.make_ready(WORKFLOW)  # the confirmation, arriving mid-pass
    await passes(SplitDurable(checkpointer, scheduler), BODY, now=Clock())(delivered(taken))

    assert list(scheduler.queue) == [WORKFLOW], "the wakeup that arrived is still there to be taken"
    assert scheduler.outstanding == {}, "and the delivery the pass held is answered for"


async def test_a_half_cancelled_by_something_other_than_the_worker_ends_the_worker() -> None:
    # A task group treats a child that ends *cancelled* as a child that finished, so it
    # exits that one and carries on with the other. That is right when the group did the
    # cancelling (a shutdown) and is how a worker would go quietly half-dead otherwise:
    # a timer with no pass loop behind it, still running and still reporting itself
    # healthy while the queue backs up, with nothing to alarm on but throughput.
    async def cancelled_by_somebody_else() -> None:
        raise asyncio.CancelledError

    with pytest.raises(RuntimeError, match="was cancelled by something other than this worker"):
        await halves(cancelled_by_somebody_else)


async def test_a_half_the_worker_cancelled_itself_ends_quietly() -> None:
    # The other side of the same question: a shutdown cancels both halves, and each one
    # unwinding is the shutdown working rather than a failure to report.
    stopping = asyncio.Event()

    async def waiting() -> None:
        stopping.set()
        await asyncio.Event().wait()

    half = asyncio.ensure_future(halves(waiting))
    await stopping.wait()
    half.cancel()

    with pytest.raises(asyncio.CancelledError):
        await half
