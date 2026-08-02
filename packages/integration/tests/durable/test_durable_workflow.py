from __future__ import annotations

import asyncio
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from datetime import timedelta
from typing import Never

import pytest
from doubles import MemoryCheckpoints
from integration.durable import Checkpoints
from integration.durable import Contended
from integration.durable import Order
from integration.durable import Pass
from integration.durable import Reached
from integration.durable import Services
from integration.durable import claimed
from integration.durable import fulfilment
from integration.durable import recorded_id
from integration.durable import run_durably
from integration.durable import run_saga
from integration.durable import unwinding
from without_dag import CompiledGraph

ORDER = Order(order_id="o-7", sku="widget", cents=2500)


async def durably[*Ins, Out](
    run: CompiledGraph[*Ins, Out],
    checkpoints: Checkpoints,
    workflow: str,
    *values: *Ins,
) -> Out:
    """
    Claim, run, release: what a driver does around every pass, so a test can say `durably`.

    Releasing at the end is what makes the *next* call in a test a resumption rather
    than a contended one, which is also true of the worker this stands in for.
    """
    holder = await claimed(checkpoints, workflow)
    try:
        return await run_durably(run, checkpoints, holder, *values)
    finally:
        await checkpoints.release(holder)


async def saga[In, Out, Reaches, Undone](
    forward: CompiledGraph[In, Out],
    unwind: CompiledGraph[Reaches, Undone],
    reaches: Callable[[Mapping[str, object]], Reaches],
    checkpoints: Checkpoints,
    workflow: str,
    value: In,
) -> Out:
    """The same, for the compensating runner, which takes its rollback's claim itself."""
    holder = await claimed(checkpoints, workflow)
    try:
        return await run_saga(forward, unwind, reaches, checkpoints, holder, value)
    finally:
        await checkpoints.release(holder)


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

    receipt = await durably(fulfilment(gateway.services()), MemoryCheckpoints(), "wf-1", ORDER)

    assert receipt == {"order_id": "o-7", "charge_id": "ch-o-7", "tracking": "tr-ch-o-7-rs-widget", "cents": 2500}
    assert sorted(gateway.calls) == ["charge", "reserve", "ship"]


async def test_a_resumed_workflow_re_runs_only_what_had_not_finished() -> None:
    checkpoints = MemoryCheckpoints()
    gateway = Gateway(broken={"ship"})
    run = fulfilment(gateway.services())

    with pytest.raises(RuntimeError, match="ship is down"):
        await durably(run, checkpoints, "wf-2", ORDER)

    assert checkpoints.hashes["wf-2"] == {"charged": "ch-o-7", "reserved": "rs-widget"}

    gateway.broken.clear()
    gateway.calls.clear()

    receipt = await durably(run, checkpoints, "wf-2", ORDER)

    assert receipt["tracking"] == "tr-ch-o-7-rs-widget"
    assert gateway.calls == ["ship"], "the card was charged and the stock reserved before the crash"


async def test_re_running_a_finished_workflow_performs_no_effects() -> None:
    checkpoints = MemoryCheckpoints()
    gateway = Gateway()
    run = fulfilment(gateway.services())

    first = await durably(run, checkpoints, "wf-3", ORDER)
    gateway.calls.clear()
    second = await durably(run, checkpoints, "wf-3", ORDER)

    assert second == first
    assert gateway.calls == [], "a completed workflow is idempotent, not merely restartable"


async def test_a_failed_workflow_unwinds_what_it_had_already_done() -> None:
    checkpoints = MemoryCheckpoints()
    gateway = Gateway(broken={"ship"})
    services = gateway.services()

    with pytest.raises(RuntimeError, match="ship is down"):
        await saga(fulfilment(services), unwinding(services), Reached.of, checkpoints, "wf-4", ORDER)

    assert sorted(gateway.calls[-2:]) == ["refund", "release"]
    assert checkpoints.hashes["wf-4:unwind"] == {
        "refunded": "rf-ch-o-7",
        "released": "rl-rs-widget",
        "unwound": {"refunded": "rf-ch-o-7", "released": "rl-rs-widget"},
    }


async def test_a_workflow_that_failed_before_any_effect_has_nothing_to_unwind() -> None:
    checkpoints = MemoryCheckpoints()
    gateway = Gateway(broken={"charge"}, blocked={"reserve"})
    services = gateway.services()

    with pytest.raises(RuntimeError, match="charge is down"):
        await saga(fulfilment(services), unwinding(services), Reached.of, checkpoints, "wf-5", ORDER)

    assert checkpoints.hashes["wf-5"] == {}
    assert gateway.calls == ["charge"], "the reservation was cancelled mid-flight, so there is nothing to release"
    assert checkpoints.hashes["wf-5:unwind"]["unwound"] == {"refunded": None, "released": None}


async def test_a_rollback_interrupted_partway_resumes_instead_of_compensating_twice() -> None:
    checkpoints = MemoryCheckpoints()
    gateway = Gateway(broken={"ship"})
    services = gateway.services()

    with pytest.raises(RuntimeError, match="ship is down"):
        await durably(fulfilment(services), checkpoints, "wf-6", ORDER)

    # A rollback that refunded and then died. Which of two concurrent compensations
    # lands first is a scheduling detail, so the half-done state is written rather
    # than raced for; a checkpoint is a value either way.
    reached = Reached.of(await checkpoints.load("wf-6"))
    await checkpoints.supply("wf-6:unwind", "refunded", "rf-ch-o-7")
    gateway.calls.clear()

    rollback = await durably(unwinding(services), checkpoints, "wf-6:unwind", reached)

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

    async def record(self, holder: Pass, key: str, value: object) -> object:
        return self.already.setdefault(key, value)

    async def transact(self, holder: Pass, key: str, effect: Never) -> object:  # pragma: no cover - uncallable
        raise NotImplementedError

    async def supply(self, workflow: str, key: str, value: object) -> object:  # pragma: no cover - unused here
        return self.already.setdefault(key, value)

    async def release(self, holder: Pass) -> None:
        return None


async def test_a_graph_run_stops_when_a_node_was_already_recorded_by_another_pass() -> None:
    gateway = Gateway()
    checkpoints = Preempted(already={"charged": "ch-from-the-other-pass"})

    with pytest.raises(Contended, match="'charged' was recorded by another pass at 'wf-race'"):
        await durably(fulfilment(gateway.services()), checkpoints, "wf-race", ORDER)

    assert "ship" not in gateway.calls, "it stopped rather than shipping against a charge that is not the real one"


async def test_reading_a_checkpoint_written_by_something_else_fails_loudly() -> None:
    with pytest.raises(TypeError, match="'charged' was recorded as 17"):
        recorded_id({"charged": 17}, "charged")
