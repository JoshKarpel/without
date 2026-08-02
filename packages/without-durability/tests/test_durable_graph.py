from __future__ import annotations

import asyncio
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from typing import Never

import pytest
from integration.durable import Order
from integration.durable import Reached
from integration.durable import Services
from integration.durable import fulfilment
from integration.durable import recorded_id
from integration.durable import unwinding
from without_dag import CompiledGraph
from without_dag import Graph
from without_durability import Checkpointer
from without_durability import Contended
from without_durability import Delivery
from without_durability import Fenced
from without_durability import MemoryCheckpointer
from without_durability import Pass
from without_durability import Recorded
from without_durability import SplitDurable
from without_durability import claimed
from without_durability import run_durably
from without_durability import run_saga

ORDER = Order(order_id="o-7", sku="widget", cents=2500)


async def durably[*Ins, Out](
    run: CompiledGraph[*Ins, Out],
    checkpointer: Checkpointer,
    workflow: str,
    *values: *Ins,
) -> Out:
    """
    Claim, run, release: what a driver does around every pass, so a test can say `durably`.

    Releasing at the end is what makes the *next* call in a test a resumption rather
    than a contended one, which is also true of the worker this stands in for.
    """
    holder = await claimed(checkpointer, workflow)
    try:
        return await run_durably(run, checkpointer, holder, *values)
    finally:
        await checkpointer.release(holder)


async def saga[In, Out, Reaches, Undone](
    forward: CompiledGraph[In, Out],
    unwind: CompiledGraph[Reaches, Undone],
    reaches: Callable[[Mapping[str, object]], Reaches],
    checkpointer: Checkpointer,
    workflow: str,
    value: In,
) -> Out:
    """The same, for the compensating runner, which takes its rollback's claim itself."""
    holder = await claimed(checkpointer, workflow)
    try:
        return await run_saga(forward, unwind, reaches, checkpointer, holder, value)
    finally:
        await checkpointer.release(holder)


@dataclass(slots=True)
class Gateway:
    """
    The outside world: records every effect, and fails or hangs where told to.

    `blocked` is what makes a failing sibling deterministic. An effect named there
    parks until the run cancels it, so it can never win a race to complete, and a
    test can pin exactly which effects reached the world before the failure.
    """

    calls: list[str] = field(default_factory=list)
    broken: set[str] = field(default_factory=set)
    blocked: set[str] = field(default_factory=set)

    async def perform(self, effect: str, result: str) -> str:
        if effect in self.blocked:
            await asyncio.Event().wait()
        self.calls.append(effect)
        if effect in self.broken:
            raise RuntimeError(f"{effect} is down")
        return result

    def services(self) -> Services:
        async def charge(order: Order) -> str:
            return await self.perform("charge", f"ch-{order.order_id}")

        async def reserve(order: Order) -> str:
            return await self.perform("reserve", f"rs-{order.sku}")

        async def ship(charge_id: str, reservation_id: str) -> str:
            return await self.perform("ship", f"tr-{charge_id}-{reservation_id}")

        async def refund(charge_id: str) -> str:
            return await self.perform("refund", f"rf-{charge_id}")

        async def release(reservation_id: str) -> str:
            return await self.perform("release", f"rl-{reservation_id}")

        return Services(charge=charge, reserve=reserve, ship=ship, refund=refund, release=release)


async def test_a_workflow_runs_every_step_once_and_returns_its_receipt() -> None:
    gateway = Gateway()

    receipt = await durably(fulfilment(gateway.services()), MemoryCheckpointer(), "wf-1", ORDER)

    assert receipt == {"order_id": "o-7", "charge_id": "ch-o-7", "tracking": "tr-ch-o-7-rs-widget", "cents": 2500}
    assert sorted(gateway.calls) == ["charge", "reserve", "ship"]


async def test_a_resumed_workflow_re_runs_only_what_had_not_finished() -> None:
    checkpointer = MemoryCheckpointer()
    gateway = Gateway(broken={"ship"})
    run = fulfilment(gateway.services())

    with pytest.raises(RuntimeError, match="ship is down"):
        await durably(run, checkpointer, "wf-2", ORDER)

    assert await checkpointer.load("wf-2") == {"charged": "ch-o-7", "reserved": "rs-widget"}

    gateway.broken.clear()
    gateway.calls.clear()

    receipt = await durably(run, checkpointer, "wf-2", ORDER)

    assert receipt["tracking"] == "tr-ch-o-7-rs-widget"
    assert gateway.calls == ["ship"], "the card was charged and the stock reserved before the crash"


async def test_re_running_a_finished_workflow_performs_no_effects() -> None:
    checkpointer = MemoryCheckpointer()
    gateway = Gateway()
    run = fulfilment(gateway.services())

    first = await durably(run, checkpointer, "wf-3", ORDER)
    gateway.calls.clear()
    second = await durably(run, checkpointer, "wf-3", ORDER)

    assert second == first
    assert gateway.calls == [], "a completed workflow is idempotent, not merely restartable"


async def test_a_failed_workflow_unwinds_what_it_had_already_done() -> None:
    checkpointer = MemoryCheckpointer()
    gateway = Gateway(broken={"ship"})
    services = gateway.services()

    with pytest.raises(RuntimeError, match="ship is down"):
        await saga(fulfilment(services), unwinding(services), Reached.of, checkpointer, "wf-4", ORDER)

    assert sorted(gateway.calls[-2:]) == ["refund", "release"]
    assert await checkpointer.load("wf-4:unwind") == {
        "refunded": "rf-ch-o-7",
        "released": "rl-rs-widget",
        "unwound": {"refunded": "rf-ch-o-7", "released": "rl-rs-widget"},
    }


async def test_a_workflow_that_failed_before_any_effect_has_nothing_to_unwind() -> None:
    checkpointer = MemoryCheckpointer()
    gateway = Gateway(broken={"charge"}, blocked={"reserve"})
    services = gateway.services()

    with pytest.raises(RuntimeError, match="charge is down"):
        await saga(fulfilment(services), unwinding(services), Reached.of, checkpointer, "wf-5", ORDER)

    assert await checkpointer.load("wf-5") == {}
    assert gateway.calls == ["charge"], "the reservation was cancelled mid-flight, so there is nothing to release"
    assert (await checkpointer.load("wf-5:unwind"))["unwound"] == {"refunded": None, "released": None}


async def test_a_rollback_interrupted_partway_resumes_instead_of_compensating_twice() -> None:
    checkpointer = MemoryCheckpointer()
    gateway = Gateway(broken={"ship"})
    services = gateway.services()

    with pytest.raises(RuntimeError, match="ship is down"):
        await durably(fulfilment(services), checkpointer, "wf-6", ORDER)

    # A rollback that refunded and then died. Which of two concurrent compensations
    # lands first is a scheduling detail, so the half-done state is written rather
    # than raced for; a checkpoint is a value either way.
    reached = Reached.of(await checkpointer.load("wf-6"))
    await checkpointer.supply("wf-6:unwind", "refunded", "rf-ch-o-7")
    gateway.calls.clear()

    rollback = await durably(unwinding(services), checkpointer, "wf-6:unwind", reached)

    assert rollback == {"refunded": "rf-ch-o-7", "released": "rl-rs-widget"}
    assert gateway.calls == ["release"], "the refund was already recorded, so the money is not returned twice"


@dataclass(frozen=True, slots=True)
class Preempted:
    """
    A store that answers `load` before another pass recorded, and `record` after it did.

    The one race a graph run cannot absorb, made deterministic. `stepwise` adopts the
    winner's value and carries on, but a graph has already handed its own result to the
    node's dependents by the time the write comes back, so it is downstream of a value
    the store rejected and the only honest move is to stop.
    """

    already: dict[str, object]

    async def load(self, workflow: str) -> dict[str, object]:
        return {}

    async def claim(self, workflow: str, lease: timedelta) -> Pass | None:
        return Pass(workflow=workflow, token=1)

    async def record(self, holder: Pass, key: str, value: object) -> Recorded:
        stored = self.already.setdefault(key, value)
        return Recorded(value=stored, first=stored is value)

    async def transact(self, holder: Pass, key: str, effect: Never) -> object:  # pragma: no cover - uncallable
        raise NotImplementedError

    async def supply(self, workflow: str, key: str, value: object) -> object:  # pragma: no cover - unused here
        return self.already.setdefault(key, value)

    async def release(self, holder: Pass) -> None:
        return None


async def test_a_graph_run_stops_when_a_node_was_already_recorded_by_another_pass() -> None:
    gateway = Gateway()
    checkpointer = Preempted(already={"charged": "ch-from-the-other-pass"})

    with pytest.raises(Contended, match="'charged' was recorded by another pass at 'wf-race'"):
        await durably(fulfilment(gateway.services()), checkpointer, "wf-race", ORDER)

    assert "ship" not in gateway.calls, "it stopped rather than shipping against a charge that is not the real one"


async def test_a_node_whose_result_does_not_survive_the_store_fails_on_the_pass_that_wrote_it() -> None:
    # A graph hands a node's result straight to its dependents, so a node that comes back
    # from the store as something else would feed them a tuple on the pass that computed
    # it and a list on the pass that restored it, with no crash needed for the two to
    # disagree. Both values are in hand here, so it is refused where it is cheap to
    # diagnose rather than days later in a dependent. This is what `stepwise` needs a
    # per-step parser for and a graph does not: verifying beats parsing while you still
    # hold what you sent.
    graph, (order,) = Graph.of(Order)

    async def bounds(placed: Order) -> tuple[int, int]:
        return (0, placed.cents)

    run = graph.build(output=graph.node("bounds", bounds, order))

    with pytest.raises(TypeError, match=r"'bounds' returned \(0, 2500\), which this store reads back as \[0, 2500\]"):
        await durably(run, MemoryCheckpointer(), "wf-reshaped", ORDER)


async def test_a_node_whose_result_round_trips_is_recorded_without_complaint() -> None:
    # The check is on the round trip, not on identity: an equal value that took a
    # different object identity through the codec is exactly the ordinary case.
    graph, (order,) = Graph.of(Order)

    async def lines(placed: Order) -> dict[str, int]:
        return {placed.sku: placed.cents}

    run = graph.build(output=graph.node("lines", lines, order))

    assert await durably(run, MemoryCheckpointer(), "wf-round-trips", ORDER) == {"widget": 2500}


async def test_a_pass_that_lost_the_workflow_mid_run_does_not_compensate() -> None:
    # The loser must stop, not unwind. Another pass holds the workflow and is advancing
    # it, so refunding here would give back a charge the winner is still building on.
    # `Fenced` is an `Interruption` precisely so the `except Exception` that drives the
    # compensation cannot reach it.
    checkpointer = MemoryCheckpointer()
    gateway = Gateway()
    services = gateway.services()

    stalled = await claimed(checkpointer, "wf-lost")
    await checkpointer.release(stalled)
    await claimed(checkpointer, "wf-lost")  # the winner, which outranks the stalled pass

    with pytest.raises(Fenced):
        await run_saga(fulfilment(services), unwinding(services), Reached.of, checkpointer, stalled, ORDER)

    assert "refund" not in gateway.calls, "the loser left the winner's charge alone"
    assert await checkpointer.load("wf-lost:unwind") == {}, "and took no claim on the rollback"


async def test_reading_a_checkpoint_written_by_something_else_fails_loudly() -> None:
    with pytest.raises(TypeError, match="'charged' was recorded as 17"):
        recorded_id({"charged": 17}, "charged")


@dataclass(frozen=True, slots=True)
class UnreachableQueue:
    """A `Scheduler` that cannot be written to, standing in for a crash mid-`arrive`."""

    async def prepare(self) -> None:  # pragma: no cover - unused here
        return None

    async def make_ready(self, workflow: str) -> None:
        raise RuntimeError("the queue is down")

    async def wake_at(self, workflow: str, when: datetime) -> None:  # pragma: no cover - unused here
        return None

    async def wake_due(self, now: datetime) -> tuple[str, ...]:  # pragma: no cover - unused here
        return ()

    async def next_ready(self, within: timedelta) -> Delivery | None:  # pragma: no cover - unused here
        return None

    async def reclaim(self, idle: timedelta) -> Delivery | None:  # pragma: no cover - unused here
        return None

    async def done(self, delivery: Delivery) -> None:  # pragma: no cover - unused here
        return None


async def test_an_arrival_over_a_split_store_records_before_it_queues() -> None:
    # The one guarantee `SplitDurable` can offer in place of a commit, and the reason its
    # two writes are in this order rather than the other. Losing the queue write leaves a
    # workflow holding its value and waiting for a wakeup, which anything asking again
    # supplies; losing the record instead would wake a pass that finds nothing to do and
    # answers for the delivery, dropping the value for good.
    checkpointer = MemoryCheckpointer()
    durable = SplitDurable(checkpointer, UnreachableQueue())

    with pytest.raises(RuntimeError, match="the queue is down"):
        await durable.arrive("wf-half-arrived", "order", {"piano": 90_000})

    assert await checkpointer.load("wf-half-arrived") == {"order": {"piano": 90_000}}, (
        "the recoverable half is the one that survives"
    )
