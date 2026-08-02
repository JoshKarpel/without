from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast

import pytest
from without import ticks
from without_durability import Delivery
from without_durability import MemoryCheckpointer
from without_durability import MemoryScheduler
from without_durability import Run
from without_durability import SplitDurable
from without_durability import Suspended
from without_durability import claimed
from without_durability import now_utc
from without_durability.worker import CONTENDED
from without_durability.worker import LEASE
from without_durability.worker import passes
from without_durability.worker import ready
from without_durability.worker import waking
from without_durability.worker import work

SMALL = {"widget": 1200, "gizmo": 800}
LARGE = {"piano": 90_000}
WORKFLOW = "wf-payout-1"
STARTED_AT = datetime(2026, 3, 14, 9, 30, tzinfo=UTC)
BRIEF = timedelta(milliseconds=10)
# Short enough that a test waits it out for real rather than mocking a clock.
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
        items = cast(dict[str, int], await run.awaiting("order"))
        total = await run.step("total", lambda: as_total(items))
        await run.sleep("settling", settling)
        if total > approval_over:
            await run.awaiting("approved-by")
        return await run.step("paid", lambda: paid(run.workflow, total))

    return body


async def as_total(items: dict[str, int]) -> int:
    return sum(items.values())


async def paid(workflow: str, total: int) -> str:
    return f"pay-{workflow}-{total}"


BODY = settling_body()


@dataclass(slots=True)
class Clock:
    at: datetime = STARTED_AT

    def __call__(self) -> datetime:
        return self.at

    def advance(self, by: timedelta) -> None:
        self.at += by


async def as_stream(*workflows: str) -> AsyncIterator[Delivery]:
    """The deliveries a queue would hand over, receipts and all."""
    for number, workflow in enumerate(workflows):
        yield Delivery(workflow=workflow, receipt=f"receipt-{number}")


def recording(ran: list[str]) -> Callable[[Run], Awaitable[None]]:
    """A workflow body that does nothing but say it ran, so a test can count passes."""

    async def counting(run: Run) -> None:
        ran.append(run.workflow)

    return counting


async def one_pass(checkpointer: MemoryCheckpointer, scheduler: MemoryScheduler, workflow: str = WORKFLOW) -> None:
    """Drive the worker's sink over exactly one delivered workflow, as the queue would."""
    await passes(SplitDurable(checkpointer, scheduler), BODY)(as_stream(workflow))


async def test_a_workflow_nobody_has_submitted_waits_without_being_scheduled() -> None:
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()

    await one_pass(checkpointer, scheduler)

    assert checkpointer.hashes[WORKFLOW] == {}
    assert scheduler.sleeping == {}, "a wait on a value has no deadline, so there is nothing to schedule"
    assert list(scheduler.queue) == []


async def test_a_submitted_order_runs_to_its_first_wait_and_schedules_the_wakeup() -> None:
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()
    await checkpointer.supply(WORKFLOW, "order", SMALL)

    await one_pass(checkpointer, scheduler)

    recorded = checkpointer.hashes[WORKFLOW]
    assert set(recorded) == {"order", "total", "settling"}
    assert "paid" not in recorded, "the settlement window has not elapsed"
    # The worker read the deadline off `Suspended` and handed it to the store's
    # sleeping set; nothing polls the workflow in the meantime.
    assert list(scheduler.sleeping) == [WORKFLOW]
    assert scheduler.sleeping[WORKFLOW] == datetime.fromisoformat(str(recorded["settling"]))


async def test_a_second_pass_after_the_wait_finishes_a_small_payout() -> None:
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()
    await checkpointer.supply(WORKFLOW, "order", SMALL)

    await one_pass(checkpointer, scheduler)
    await asyncio.sleep(SETTLING.total_seconds() * 2)  # the workflow's own window, waited out for real

    assert await scheduler.wake_due(now_utc()) == (WORKFLOW,), "the timer finds it due and queues it again"
    await one_pass(checkpointer, scheduler)

    assert checkpointer.hashes[WORKFLOW]["paid"] == f"pay-{WORKFLOW}-2000"


async def test_a_large_payout_stops_at_the_confirmation_and_resumes_once_it_is_recorded() -> None:
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()
    await checkpointer.supply(WORKFLOW, "order", LARGE)

    await one_pass(checkpointer, scheduler)
    await asyncio.sleep(SETTLING.total_seconds() * 2)
    await scheduler.wake_due(now_utc())
    await one_pass(checkpointer, scheduler)

    assert "paid" not in checkpointer.hashes[WORKFLOW]
    assert scheduler.sleeping == {}, "the wait is on a person now, so this pass scheduled nothing"

    await checkpointer.supply(WORKFLOW, "approved-by", "auditor-7")
    await one_pass(checkpointer, scheduler)

    assert checkpointer.hashes[WORKFLOW]["paid"] == f"pay-{WORKFLOW}-90000"


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
    await scheduler.wake_at(WORKFLOW, STARTED_AT + timedelta(seconds=30))

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


async def test_the_worker_carries_a_submitted_order_through_its_own_wait() -> None:
    # The assembled thing: submitting is enough. The timer and the pass loop hand the
    # workflow back and forth until it is done, with nobody driving it.
    checkpointer = MemoryCheckpointer()
    scheduler = MemoryScheduler()
    await checkpointer.supply(WORKFLOW, "order", SMALL)
    await scheduler.make_ready(WORKFLOW)

    worker = asyncio.create_task(work(SplitDurable(checkpointer, scheduler), tick=BRIEF, body=BODY))
    try:
        async with asyncio.timeout(3):
            while "paid" not in checkpointer.hashes[WORKFLOW]:
                await asyncio.sleep(BRIEF.total_seconds())
    finally:
        worker.cancel()

    assert checkpointer.hashes[WORKFLOW]["paid"] == f"pay-{WORKFLOW}-2000"


async def test_a_body_driven_without_the_worker_suspends_the_same_way() -> None:
    # `resume` is what the worker calls, so a body reaches the same wait whether a queue
    # delivered it or a test drove it by hand. That is what makes the worker optional.
    checkpointer = MemoryCheckpointer()
    await checkpointer.supply(WORKFLOW, "order", SMALL)
    run = Run(
        holder=await claimed(checkpointer, WORKFLOW),
        checkpointer=checkpointer,
        recorded=await checkpointer.load(WORKFLOW),
    )

    with pytest.raises(Suspended, match="suspended at 'settling'"):
        await BODY(run)

    assert checkpointer.hashes[WORKFLOW]["total"] == sum(SMALL.values())
