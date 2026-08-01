from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from doubles import MemoryCheckpoints
from doubles import MemoryWakeups
from integration.durable.stepwise import Run
from integration.durable.stepwise import Suspended
from integration.durable.stepwise import now_utc
from integration.durable.wakeups import Delivery
from integration.durable.worker import LEASE
from integration.durable.worker import passes
from integration.durable.worker import ready
from integration.durable.worker import submitting
from integration.durable.worker import waking
from integration.durable.worker import work

SMALL = {"widget": 1200, "gizmo": 800}
LARGE = {"piano": 90_000}
WORKFLOW = "wf-payout-1"
STARTED_AT = datetime(2026, 3, 14, 9, 30, tzinfo=UTC)
BRIEF = timedelta(milliseconds=10)
# The deployment settles for a second (see `worker.SETTLING`); these run the same body
# over a window they do not have to wait out.
SETTLING = timedelta(milliseconds=20)
BODY = submitting(settling=SETTLING)


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


async def one_pass(checkpoints: MemoryCheckpoints, wakeups: MemoryWakeups, workflow: str = WORKFLOW) -> None:
    """Drive the worker's sink over exactly one delivered workflow, as the queue would."""
    await passes(checkpoints, wakeups, BODY)(as_stream(workflow))


async def test_a_workflow_nobody_has_submitted_waits_without_being_scheduled() -> None:
    checkpoints = MemoryCheckpoints()
    wakeups = MemoryWakeups()

    await one_pass(checkpoints, wakeups)

    assert checkpoints.hashes[WORKFLOW] == {}
    assert wakeups.sleeping == {}, "a wait on a value has no deadline, so there is nothing to schedule"
    assert list(wakeups.queue) == []


async def test_a_submitted_order_runs_to_its_first_wait_and_schedules_the_wakeup() -> None:
    checkpoints = MemoryCheckpoints()
    wakeups = MemoryWakeups()
    await checkpoints.record(WORKFLOW, "order", SMALL)

    await one_pass(checkpoints, wakeups)

    recorded = checkpoints.hashes[WORKFLOW]
    assert set(recorded) == {"order", "items", "captured:widget", "captured:gizmo", "settling"}
    assert "paid" not in recorded, "the settlement window has not elapsed"
    # The worker read the deadline off `Suspended` and handed it to the store's
    # sleeping set; nothing polls the workflow in the meantime.
    assert list(wakeups.sleeping) == [WORKFLOW]
    assert wakeups.sleeping[WORKFLOW] == datetime.fromisoformat(str(recorded["settling"]))


async def test_a_second_pass_after_the_wait_finishes_a_small_payout() -> None:
    checkpoints = MemoryCheckpoints()
    wakeups = MemoryWakeups()
    await checkpoints.record(WORKFLOW, "order", SMALL)

    await one_pass(checkpoints, wakeups)
    await asyncio.sleep(SETTLING.total_seconds() * 2)  # the workflow's own window, waited out for real

    assert await wakeups.wake_due(now_utc()) == (WORKFLOW,), "the timer finds it due and queues it again"
    await one_pass(checkpoints, wakeups)

    assert checkpoints.hashes[WORKFLOW]["paid"] == f"pay-{WORKFLOW}-2000"


async def test_a_large_payout_stops_at_the_confirmation_and_resumes_once_it_is_recorded() -> None:
    checkpoints = MemoryCheckpoints()
    wakeups = MemoryWakeups()
    await checkpoints.record(WORKFLOW, "order", LARGE)

    await one_pass(checkpoints, wakeups)
    await asyncio.sleep(SETTLING.total_seconds() * 2)
    await wakeups.wake_due(now_utc())
    await one_pass(checkpoints, wakeups)

    assert "paid" not in checkpoints.hashes[WORKFLOW]
    assert wakeups.sleeping == {}, "the wait is on a person now, so this pass scheduled nothing"

    await checkpoints.record(WORKFLOW, "approved-by", "auditor-7")
    await one_pass(checkpoints, wakeups)

    assert checkpoints.hashes[WORKFLOW]["paid"] == f"pay-{WORKFLOW}-90000"


async def test_a_workflow_whose_step_raises_does_not_take_the_worker_down() -> None:
    checkpoints = MemoryCheckpoints()
    wakeups = MemoryWakeups()
    reached: list[str] = []

    async def declining(run: Run) -> None:
        reached.append(run.workflow)
        raise RuntimeError("the gateway declined")

    # Both workflows are consumed: the first one's failure is this service's data, so
    # the loop logs it and takes the next id rather than ending the worker.
    await passes(checkpoints, wakeups, declining)(as_stream("wf-doomed", "wf-fine"))

    assert reached == ["wf-doomed", "wf-fine"]


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
        await passes(MemoryCheckpoints(), MemoryWakeups(), blocking, limit)(endless())

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
    wakeups = MemoryWakeups()
    clock = Clock()
    await wakeups.wake_at(WORKFLOW, STARTED_AT + timedelta(seconds=30))

    timer = asyncio.create_task(waking(wakeups, tick=BRIEF, now=clock))
    try:
        await asyncio.sleep(BRIEF.total_seconds() * 3)
        assert list(wakeups.queue) == [], "its deadline has not passed"

        clock.advance(timedelta(seconds=31))
        async with asyncio.timeout(3):
            await wakeups.arrived.wait()
    finally:
        timer.cancel()

    assert list(wakeups.queue) == [WORKFLOW]
    assert wakeups.sleeping == {}, "the move takes it off the sleepers, so a second tick cannot wake it twice"
    assert await wakeups.wake_due(clock()) == (), "and there is nothing left for another timer to find"


async def test_the_ready_stream_yields_each_queued_workflow() -> None:
    wakeups = MemoryWakeups()
    await wakeups.make_ready("wf-a")
    await wakeups.make_ready("wf-b")

    workflows = ready(wakeups, within=BRIEF, idle=LEASE)
    taken = [await anext(workflows), await anext(workflows)]
    await workflows.aclose()

    assert [delivery.workflow for delivery in taken] == ["wf-a", "wf-b"]
    assert len(wakeups.outstanding) == 2, "delivered, and nobody has answered for either yet"


async def test_the_ready_stream_takes_over_a_delivery_a_dead_worker_never_answered_for() -> None:
    # The crash the queue exists to survive: a worker takes a wakeup and dies before
    # acknowledging it. The delivery stays outstanding, and the next worker's stream
    # hands it back rather than leaving the workflow stuck forever.
    wakeups = MemoryWakeups()
    await wakeups.make_ready(WORKFLOW)
    abandoned = await wakeups.next_ready(BRIEF)

    assert abandoned is not None
    assert list(wakeups.queue) == [], "it is off the queue, so no new read will find it"

    workflows = ready(wakeups, within=BRIEF, idle=timedelta())  # nothing may go unanswered
    taken = await anext(workflows)
    await workflows.aclose()

    assert taken.workflow == WORKFLOW
    assert taken.receipt == abandoned.receipt, "the same delivery, taken over rather than duplicated"


async def test_the_worker_carries_a_submitted_order_through_its_own_wait() -> None:
    # The assembled thing: submitting is enough. The timer and the pass loop hand the
    # workflow back and forth until it is done, with nobody driving it.
    checkpoints = MemoryCheckpoints()
    wakeups = MemoryWakeups()
    await checkpoints.record(WORKFLOW, "order", SMALL)
    await wakeups.make_ready(WORKFLOW)

    worker = asyncio.create_task(work(checkpoints, wakeups, tick=BRIEF, body=BODY))
    try:
        async with asyncio.timeout(3):
            while "paid" not in checkpoints.hashes[WORKFLOW]:
                await asyncio.sleep(BRIEF.total_seconds())
    finally:
        worker.cancel()

    assert checkpoints.hashes[WORKFLOW]["paid"] == f"pay-{WORKFLOW}-2000"


async def test_the_deployed_workflow_reads_its_order_out_of_the_checkpoint() -> None:
    # `submitting()` with no arguments is what the worker runs, so this covers the
    # deployment's own settlement window without waiting for it: reaching the wait is
    # the assertion.
    checkpoints = MemoryCheckpoints()
    await checkpoints.record(WORKFLOW, "order", SMALL)
    run = Run(workflow=WORKFLOW, checkpoints=checkpoints, recorded=await checkpoints.load(WORKFLOW))

    with pytest.raises(Suspended, match="suspended at 'settling'"):
        await submitting()(run)

    assert checkpoints.hashes[WORKFLOW]["items"] == SMALL
