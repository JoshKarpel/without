from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field

import pytest
from integration.durable import Order
from integration.durable import Reached
from integration.durable import Services
from integration.durable import fulfilment
from integration.durable import recorded_id
from integration.durable import run_durably
from integration.durable import run_saga
from integration.durable import unwinding
from without_dag import NodeKey

ORDER = Order(order_id="o-7", sku="widget", cents=2500)


@dataclass(frozen=True, slots=True)
class MemoryCheckpoints:
    """
    A `Checkpoints` that keeps the hashes in a dict: the same seam, no container.

    A test double rather than a mock, so the durable runner is exercised for real
    (the load, the record, the resume) and only the storage is swapped, which is
    what injecting the store buys.
    """

    hashes: dict[str, dict[NodeKey, object]] = field(default_factory=lambda: defaultdict(dict))

    async def load(self, workflow: str) -> dict[NodeKey, object]:
        return dict(self.hashes[workflow])

    async def record(self, workflow: str, key: NodeKey, value: object) -> None:
        self.hashes[workflow][key] = value


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

    receipt = await run_durably(fulfilment(gateway.services()), MemoryCheckpoints(), "wf-1", ORDER)

    assert receipt == {"order_id": "o-7", "charge_id": "ch-o-7", "tracking": "tr-ch-o-7-rs-widget", "cents": 2500}
    assert sorted(gateway.calls) == ["charge", "reserve", "ship"]


async def test_a_resumed_workflow_re_runs_only_what_had_not_finished() -> None:
    checkpoints = MemoryCheckpoints()
    gateway = Gateway(broken={"ship"})
    run = fulfilment(gateway.services())

    with pytest.raises(RuntimeError, match="ship is down"):
        await run_durably(run, checkpoints, "wf-2", ORDER)

    assert checkpoints.hashes["wf-2"] == {"charged": "ch-o-7", "reserved": "rs-widget"}

    gateway.broken.clear()
    gateway.calls.clear()

    receipt = await run_durably(run, checkpoints, "wf-2", ORDER)

    assert receipt["tracking"] == "tr-ch-o-7-rs-widget"
    assert gateway.calls == ["ship"], "the card was charged and the stock reserved before the crash"


async def test_re_running_a_finished_workflow_performs_no_effects() -> None:
    checkpoints = MemoryCheckpoints()
    gateway = Gateway()
    run = fulfilment(gateway.services())

    first = await run_durably(run, checkpoints, "wf-3", ORDER)
    gateway.calls.clear()
    second = await run_durably(run, checkpoints, "wf-3", ORDER)

    assert second == first
    assert gateway.calls == [], "a completed workflow is idempotent, not merely restartable"


async def test_a_failed_workflow_unwinds_what_it_had_already_done() -> None:
    checkpoints = MemoryCheckpoints()
    gateway = Gateway(broken={"ship"})
    services = gateway.services()

    with pytest.raises(RuntimeError, match="ship is down"):
        await run_saga(fulfilment(services), unwinding(services), Reached.of, checkpoints, "wf-4", ORDER)

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
        await run_saga(fulfilment(services), unwinding(services), Reached.of, checkpoints, "wf-5", ORDER)

    assert checkpoints.hashes["wf-5"] == {}
    assert gateway.calls == ["charge"], "the reservation was cancelled mid-flight, so there is nothing to release"
    assert checkpoints.hashes["wf-5:unwind"]["unwound"] == {"refunded": None, "released": None}


async def test_a_rollback_interrupted_partway_resumes_instead_of_compensating_twice() -> None:
    checkpoints = MemoryCheckpoints()
    gateway = Gateway(broken={"ship"})
    services = gateway.services()

    with pytest.raises(RuntimeError, match="ship is down"):
        await run_durably(fulfilment(services), checkpoints, "wf-6", ORDER)

    # A rollback that refunded and then died. Which of two concurrent compensations
    # lands first is a scheduling detail, so the half-done state is written rather
    # than raced for; a checkpoint is a value either way.
    reached = Reached.of(await checkpoints.load("wf-6"))
    await checkpoints.record("wf-6:unwind", "refunded", "rf-ch-o-7")
    gateway.calls.clear()

    rollback = await run_durably(unwinding(services), checkpoints, "wf-6:unwind", reached)

    assert rollback == {"refunded": "rf-ch-o-7", "released": "rl-rs-widget"}
    assert gateway.calls == ["release"], "the refund was already recorded, so the money is not returned twice"


async def test_reading_a_checkpoint_written_by_something_else_fails_loudly() -> None:
    with pytest.raises(TypeError, match="'charged' was recorded as 17"):
        recorded_id({"charged": 17}, "charged")
