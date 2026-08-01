from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from integration.durable import Payouts
from integration.durable import Run
from integration.durable import Suspended
from integration.durable import now_utc
from integration.durable import parse_approver
from integration.durable import parse_deadline
from integration.durable import parse_items
from integration.durable import pay_out
from integration.durable import resume

ORDER = "ord-88"
ITEMS = {"widget": 1200, "gizmo": 800}
BIG_ITEMS = {"piano": 90_000, "stool": 4_000}
SETTLING = timedelta(days=3)
APPROVAL_OVER = 10_000
STARTED_AT = datetime(2026, 3, 14, 9, 30, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class MemoryCheckpoints:
    """A `Checkpoints` in a dict: the same seam the Redis one implements, no container."""

    hashes: dict[str, dict[str, object]] = field(default_factory=lambda: defaultdict(dict))

    async def load(self, workflow: str) -> dict[str, object]:
        return dict(self.hashes[workflow])

    async def record(self, workflow: str, key: str, value: object) -> None:
        self.hashes[workflow][key] = value


@dataclass(slots=True)
class Clock:
    """A clock the test moves, so a three-day wait costs a line rather than three days."""

    at: datetime = STARTED_AT

    def __call__(self) -> datetime:
        return self.at

    def advance(self, by: timedelta) -> None:
        self.at += by


@dataclass(slots=True)
class Ledger:
    """The outside world: records every effect, and fails where told to."""

    items: dict[str, int] = field(default_factory=lambda: dict(ITEMS))
    calls: list[str] = field(default_factory=list)
    broken: set[str] = field(default_factory=set)

    def services(self) -> Payouts:
        async def items(order_id: str) -> dict[str, int]:
            self.perform(f"items:{order_id}")
            return self.items

        async def capture(sku: str, amount: int) -> str:
            self.perform(f"capture:{sku}")
            return f"cap-{sku}-{amount}"

        async def pay(order_id: str, total: int) -> str:
            self.perform("pay")
            return f"pay-{order_id}-{total}"

        return Payouts(items=items, capture=capture, pay=pay)

    def perform(self, effect: str) -> None:
        self.calls.append(effect)
        if effect in self.broken:
            raise RuntimeError(f"{effect} is down")


async def paying(
    ledger: Ledger,
    checkpoints: MemoryCheckpoints,
    clock: Clock,
    *,
    settling: timedelta = timedelta(),
) -> dict[str, object]:
    """One pass at the payout workflow, with the knobs every test here shares."""

    async def body(run: Run) -> dict[str, object]:
        return await pay_out(run, ORDER, ledger.services(), settling=settling, approval_over=APPROVAL_OVER)

    return await resume(ORDER, checkpoints, body, now=clock)


async def test_a_workflow_performs_each_effect_once_and_returns_its_payout() -> None:
    ledger = Ledger()
    checkpoints = MemoryCheckpoints()

    payout = await paying(ledger, checkpoints, Clock())

    assert payout == {
        "order_id": ORDER,
        "total": 2000,
        "reference": "pay-ord-88-2000",
        "approved_by": None,
        "captures": {"gizmo": "cap-gizmo-800", "widget": "cap-widget-1200"},
    }
    assert sorted(ledger.calls) == ["capture:gizmo", "capture:widget", f"items:{ORDER}", "pay"]


async def test_a_second_pass_over_a_finished_workflow_performs_no_effects() -> None:
    ledger = Ledger()
    checkpoints = MemoryCheckpoints()

    first = await paying(ledger, checkpoints, Clock())
    ledger.calls.clear()
    second = await paying(ledger, checkpoints, Clock())

    assert second == first
    assert ledger.calls == [], "every step reached its record, so the pass was a re-read"


async def test_a_pass_that_fails_partway_leaves_the_steps_that_finished_recorded() -> None:
    ledger = Ledger(broken={"pay"})
    checkpoints = MemoryCheckpoints()

    with pytest.raises(RuntimeError, match="pay is down"):
        await paying(ledger, checkpoints, Clock())

    assert checkpoints.hashes[ORDER] == {
        "items": ITEMS,
        "captured:gizmo": "cap-gizmo-800",
        "captured:widget": "cap-widget-1200",
        "settling": STARTED_AT.isoformat(),
    }

    ledger.broken.clear()
    ledger.calls.clear()

    payout = await paying(ledger, checkpoints, Clock())

    assert payout["reference"] == "pay-ord-88-2000"
    assert ledger.calls == ["pay"], "the captures were read back rather than re-charged"


async def test_the_fan_out_is_one_step_per_item_the_first_step_returned() -> None:
    # The shape a fixed graph cannot express: how many captures there are is a
    # *result*, not an input, and each carries its own key so a crash resumes item by
    # item rather than re-capturing the lot.
    ledger = Ledger(items={"a": 1, "b": 2, "c": 3, "d": 4})
    checkpoints = MemoryCheckpoints()

    await paying(ledger, checkpoints, Clock())

    assert [key for key in checkpoints.hashes[ORDER] if key.startswith("captured:")] == [
        "captured:a",
        "captured:b",
        "captured:c",
        "captured:d",
    ]


async def test_a_wait_suspends_the_pass_and_resumes_once_its_deadline_has_passed() -> None:
    ledger = Ledger()
    checkpoints = MemoryCheckpoints()
    clock = Clock()

    with pytest.raises(Suspended) as suspension:
        await paying(ledger, checkpoints, clock, settling=SETTLING)

    assert suspension.value.key == "settling"
    assert suspension.value.due == STARTED_AT + SETTLING
    assert "pay" not in ledger.calls

    clock.advance(SETTLING)
    payout = await paying(ledger, checkpoints, clock, settling=SETTLING)

    assert payout["reference"] == "pay-ord-88-2000"


async def test_a_wait_interrupted_partway_does_not_restart_its_clock() -> None:
    # The reason the *deadline* is recorded rather than the duration: a pass on day two
    # of a three-day wait must not push the deadline out to day five.
    ledger = Ledger()
    checkpoints = MemoryCheckpoints()
    clock = Clock()

    with pytest.raises(Suspended):
        await paying(ledger, checkpoints, clock, settling=SETTLING)

    clock.advance(timedelta(days=2))

    with pytest.raises(Suspended) as second:
        await paying(ledger, checkpoints, clock, settling=SETTLING)

    assert second.value.due == STARTED_AT + SETTLING
    assert checkpoints.hashes[ORDER]["settling"] == (STARTED_AT + SETTLING).isoformat()


async def test_a_payout_over_the_threshold_waits_for_an_approval_another_process_records() -> None:
    ledger = Ledger(items=dict(BIG_ITEMS))
    checkpoints = MemoryCheckpoints()

    with pytest.raises(Suspended) as suspension:
        await paying(ledger, checkpoints, Clock())

    assert suspension.value.key == "approved-by"
    assert suspension.value.due is None, "this wait ends when it is told, not when a clock says so"
    assert "pay" not in ledger.calls

    # Whoever took the approval writes one field into the workflow's checkpoint. It
    # shares nothing with the suspended pass, which is gone.
    await checkpoints.record(ORDER, "approved-by", "auditor-7")
    ledger.calls.clear()

    payout = await paying(ledger, checkpoints, Clock())

    assert payout["approved_by"] == "auditor-7"
    assert payout["total"] == 94_000
    assert ledger.calls == ["pay"], "the captures happened before the approval was asked for"


async def test_a_pass_refuses_two_steps_sharing_a_name() -> None:
    checkpoints = MemoryCheckpoints()

    async def body(run: Run) -> None:
        await run.step("charged", lambda: answering("first"))
        await run.step("charged", lambda: answering("second"))

    with pytest.raises(ValueError, match="'charged' was already used in this pass"):
        await resume(ORDER, checkpoints, body, now=Clock())


async def answering(value: str) -> str:
    return value


async def test_a_deadline_recorded_as_something_else_fails_loudly() -> None:
    with pytest.raises(TypeError, match="'settling' holds 17"):
        parse_deadline("settling", 17)


async def test_line_items_recorded_as_something_else_fail_loudly() -> None:
    with pytest.raises(TypeError, match="recorded as 'nope'"):
        parse_items("nope")

    with pytest.raises(TypeError, match="the amount recorded for 'widget' is '1200'"):
        parse_items({"widget": "1200"})


async def test_an_approval_recorded_as_something_else_fails_loudly() -> None:
    # The one value here that a *different* process writes, so the one most worth
    # parsing rather than trusting.
    with pytest.raises(TypeError, match="the approval holds 42"):
        parse_approver(42)


async def test_the_default_clock_reads_an_aware_utc_time() -> None:
    # Deadlines are compared and serialized, so a naive clock would compare against an
    # aware deadline read back from the store and raise at the comparison.
    assert now_utc().tzinfo is UTC
