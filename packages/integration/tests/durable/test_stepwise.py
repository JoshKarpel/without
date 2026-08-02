from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from doubles import MemoryCheckpoints
from doubles import MemoryEffect
from integration.durable import Contended
from integration.durable import Fenced
from integration.durable import Payouts
from integration.durable import Run
from integration.durable import Suspended
from integration.durable import claimed
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
    """
    One pass at the payout workflow, with the knobs every test here shares.

    Claim, run, release, which is what a worker does around every pass: holding the
    claim for the whole pass is the exclusion, and letting it go at the end is what
    makes the *next* pass in these tests a resumption rather than a contended one.
    """

    async def body(run: Run) -> dict[str, object]:
        return await pay_out(run, ORDER, ledger.services(), settling=settling, approval_over=APPROVAL_OVER)

    holder = await claimed(checkpoints, ORDER)
    try:
        return await resume(holder, checkpoints, body, now=clock)
    finally:
        await checkpoints.release(holder)


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
    await checkpoints.supply(ORDER, "approved-by", "auditor-7")
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
        await resume(await claimed(checkpoints, ORDER), checkpoints, body, now=Clock())


async def answering(value: str) -> str:
    return value


async def test_a_workflow_already_being_passed_over_cannot_be_claimed_again() -> None:
    # The property the whole seam exists for. Without it, two wakeups for one workflow
    # (which the submit-then-confirm flow produces every time) run two passes side by
    # side, and both find the same step unrecorded.
    checkpoints = MemoryCheckpoints()

    holder = await claimed(checkpoints, ORDER)

    assert await checkpoints.claim(ORDER, timedelta(minutes=1)) is None
    with pytest.raises(Contended, match=f"another pass holds {ORDER!r}"):
        await claimed(checkpoints, ORDER)

    await checkpoints.release(holder)

    assert await checkpoints.claim(ORDER, timedelta(minutes=1)) is not None, "released, so the next pass may run"


async def test_a_claim_outranks_every_claim_before_it() -> None:
    checkpoints = MemoryCheckpoints()

    first = await claimed(checkpoints, ORDER)
    await checkpoints.release(first)
    second = await claimed(checkpoints, ORDER)

    assert second.token > first.token, "releasing hands the workflow back, it does not rewind the fence"


async def test_a_write_from_a_superseded_pass_is_refused_rather_than_applied() -> None:
    # A lease alone cannot do this: a pass that stalls past its lease still believes it
    # holds the workflow, and only the store knows better. The token is what tells it.
    checkpoints = MemoryCheckpoints()
    stalled = await claimed(checkpoints, ORDER)
    await checkpoints.release(stalled)
    took_over = await claimed(checkpoints, ORDER)

    with pytest.raises(Fenced, match=f"pass {stalled.token} of {ORDER!r} was superseded"):
        await checkpoints.record(stalled, "paid", "pay-from-the-dead")

    assert await checkpoints.record(took_over, "paid", "pay-real") == "pay-real"
    assert checkpoints.hashes[ORDER] == {"paid": "pay-real"}

    await checkpoints.release(stalled)

    assert await checkpoints.claim(ORDER, timedelta(minutes=1)) is None, (
        "and a superseded pass cannot hand back a workflow that is no longer its to give"
    )


async def test_two_passes_that_both_ran_a_step_agree_on_its_result() -> None:
    # The cheaper half of the guarantee, and the one that still matters when exclusion
    # has already failed: the effect happened twice (nothing here can prevent that), but
    # the second pass is handed the first's value rather than overwriting it, so the two
    # do not carry different capture ids into everything downstream.
    checkpoints = MemoryCheckpoints()
    holder = await claimed(checkpoints, ORDER)

    won = await checkpoints.record(holder, "captured:widget", "cap-from-the-winner")
    lost = await checkpoints.record(holder, "captured:widget", "cap-from-the-loser")

    assert won == "cap-from-the-winner"
    assert lost == "cap-from-the-winner", "the loser learns the winner's value instead of clobbering it"


async def test_a_step_returns_what_the_store_holds_rather_than_what_its_effect_produced() -> None:
    # The same property seen from inside a workflow, which is where it does its work:
    # `run.step` hands back the recorded value, so a pass whose effect ran a second time
    # still proceeds on the one result everybody agrees on.
    checkpoints = MemoryCheckpoints()
    await checkpoints.supply(ORDER, "charged", "ch-recorded-earlier")
    holder = await claimed(checkpoints, ORDER)
    run = Run(holder=holder, checkpoints=checkpoints, recorded={})

    assert await run.step("charged", lambda: answering("ch-just-now")) == "ch-recorded-earlier"


def tallying(counter: str) -> MemoryEffect:
    """An effect over the store's own data: bump a counter, record what it reached."""

    def bump(data: dict[str, object]) -> object:
        running = data.get(counter, 0)
        assert isinstance(running, int)
        data[counter] = running + 1
        return data[counter]

    return bump


async def test_a_step_whose_record_never_lands_runs_its_effect_again() -> None:
    # The at-least-once bound, made concrete, so the next test has something to beat.
    # `step` performs the effect and *then* writes the record, and a pass that dies in
    # between leaves the effect done and unrecorded, so the next pass repeats it.
    checkpoints = MemoryCheckpoints()
    holder = await claimed(checkpoints, ORDER)
    run = Run(holder=holder, checkpoints=checkpoints, recorded={})
    effect = tallying("charges")

    async def charge() -> object:
        return effect(checkpoints.data)

    await run.step("charged", charge)
    checkpoints.hashes[ORDER].clear()  # the record that a crash lost
    await Run(holder=holder, checkpoints=checkpoints, recorded={}).step("charged", charge)

    assert checkpoints.data["charges"] == 2, "the card was charged twice, which is what an idempotency key is for"


async def test_a_transacted_step_cannot_be_run_without_being_recorded() -> None:
    # Exactly-once, because there is no in-between for a crash to land in: the effect and
    # its record are one commit, so a record that is missing means the effect did not
    # happen either. Losing the record the way the test above does is not something a
    # crash can produce here, so the only way to re-reach this step is a fresh pass, and
    # a fresh pass finds it recorded and performs nothing.
    checkpoints = MemoryCheckpoints()
    holder = await claimed(checkpoints, ORDER)

    first = await Run(holder=holder, checkpoints=checkpoints, recorded={}).transact("charged", tallying("charges"))
    again = await Run(
        holder=holder,
        checkpoints=checkpoints,
        recorded=await checkpoints.load(ORDER),
    ).transact("charged", tallying("charges"))
    fresh = await Run(holder=holder, checkpoints=checkpoints, recorded={}).transact("charged", tallying("charges"))

    assert (first, again, fresh) == (1, 1, 1), "one effect, however many passes reach it"
    assert checkpoints.data["charges"] == 1
    assert checkpoints.hashes[ORDER] == {"charged": 1}


async def test_a_transacted_step_is_refused_from_a_superseded_pass() -> None:
    # The fence covers `transact` as it covers `record`, and it has to: a stalled pass
    # performing its effect is worse here than a stalled write, since the effect is real.
    checkpoints = MemoryCheckpoints()
    stalled = await claimed(checkpoints, ORDER)
    await checkpoints.release(stalled)
    await claimed(checkpoints, ORDER)

    with pytest.raises(Fenced):
        await Run(holder=stalled, checkpoints=checkpoints, recorded={}).transact("charged", tallying("charges"))

    assert checkpoints.data == {}, "refused before the effect ran, not after"


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
