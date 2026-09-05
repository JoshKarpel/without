from __future__ import annotations

import asyncio
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import timedelta

import pytest
from integration.durable import Payouts
from integration.durable import parse_approver
from integration.durable import parse_held
from integration.durable import parse_items
from integration.durable import parse_reference
from integration.durable import pay_out
from without_durability import Blocked
from without_durability import Completed
from without_durability import Contended
from without_durability import Entry
from without_durability import Fenced
from without_durability import InputNeeded
from without_durability import MemoryCheckpointer
from without_durability import MemoryEffect
from without_durability import MessageNeeded
from without_durability import Outcome
from without_durability import Recorded
from without_durability import Run
from without_durability import ScheduledWakeup
from without_durability import Sleeping
from without_durability import Suspended
from without_durability import Swallowed
from without_durability import claimed
from without_durability import inbox_key
from without_durability import now_utc
from without_durability import parse_bound
from without_durability import parse_deadline
from without_durability import resume
from without_durability.stepwise import stopped_at
from without_durability.stepwise import unwound

from .helpers import STARTED_AT
from .helpers import Clock
from .helpers import ParkedWrites
from .helpers import as_text

ORDER = "ord-88"
ITEMS = {"widget": 1200, "gizmo": 800}
BIG_ITEMS = {"piano": 90_000, "stool": 4_000}
SETTLING = timedelta(days=3)
APPROVAL_OVER = 10_000


@dataclass(slots=True)
class Ledger:
    """The outside world: records every effect, and fails or parks where told to."""

    items: dict[str, int] = field(default_factory=lambda: dict(ITEMS))
    calls: list[str] = field(default_factory=list)
    broken: set[str] = field(default_factory=set)
    # A capture named here parks until something cancels it, and says so when that
    # happens. It is how a failing sibling is made deterministic (a parked effect can
    # never win the race to complete) and how the cancelling itself is observed.
    blocked: set[str] = field(default_factory=set)

    def services(self) -> Payouts:
        async def items(order_id: str) -> dict[str, int]:
            self.perform(f"items:{order_id}")
            return self.items

        async def capture(sku: str, amount: int) -> str:
            if f"capture:{sku}" in self.blocked:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    self.calls.append(f"cancelled:{sku}")
                    raise
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
    checkpointer: MemoryCheckpointer,
    clock: Clock,
    *,
    settling: timedelta = timedelta(),
) -> Outcome[dict[str, object]]:
    """
    One pass at the payout workflow, with the knobs every test here shares.

    Claim, run, release, which is what a worker does around every pass: holding the
    claim for the whole pass is the exclusion, and letting it go at the end is what
    makes the *next* pass in these tests a resumption rather than a contended one.
    """

    async def body(run: Run) -> dict[str, object]:
        return await pay_out(run, ORDER, ledger.services(), settling=settling, approval_over=APPROVAL_OVER)

    holder = await claimed(checkpointer, ORDER)
    try:
        return await resume(holder, checkpointer, body, now=clock)
    finally:
        await checkpointer.release(holder)


def finished[T](outcome: Outcome[T]) -> T:
    """The payout a pass returned, failing here rather than downstream if it stopped short."""
    assert isinstance(outcome, Completed), f"the pass did not finish: {outcome}"
    return outcome.value


async def test_a_workflow_performs_each_effect_once_and_returns_its_payout() -> None:
    ledger = Ledger()
    checkpointer = MemoryCheckpointer()

    payout = finished(await paying(ledger, checkpointer, Clock()))

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
    checkpointer = MemoryCheckpointer()

    first = await paying(ledger, checkpointer, Clock())
    ledger.calls.clear()
    second = await paying(ledger, checkpointer, Clock())

    assert second == first
    assert ledger.calls == [], "every step reached its record, so the pass was a re-read"


async def test_a_pass_that_fails_partway_leaves_the_steps_that_finished_recorded() -> None:
    ledger = Ledger(broken={"pay"})
    checkpointer = MemoryCheckpointer()

    with pytest.raises(RuntimeError, match="pay is down"):
        await paying(ledger, checkpointer, Clock())

    assert await checkpointer.load(ORDER) == {
        "items": ITEMS,
        "captured:gizmo": "cap-gizmo-800",
        "captured:widget": "cap-widget-1200",
        "settling": STARTED_AT.isoformat(),
        "held-for-approval": False,
    }

    ledger.broken.clear()
    ledger.calls.clear()

    payout = finished(await paying(ledger, checkpointer, Clock()))

    assert payout["reference"] == "pay-ord-88-2000"
    assert ledger.calls == ["pay"], "the captures were read back rather than re-charged"


async def test_the_fan_out_is_one_step_per_item_the_first_step_returned() -> None:
    # The shape a fixed graph cannot express: how many captures there are is a
    # *result*, not an input, and each carries its own key so a crash resumes item by
    # item rather than re-capturing the lot.
    ledger = Ledger(items={"a": 1, "b": 2, "c": 3, "d": 4})
    checkpointer = MemoryCheckpointer()

    await paying(ledger, checkpointer, Clock())

    assert [key for key in await checkpointer.load(ORDER) if key.startswith("captured:")] == [
        "captured:a",
        "captured:b",
        "captured:c",
        "captured:d",
    ]


async def test_a_wait_suspends_the_pass_and_resumes_once_its_deadline_has_passed() -> None:
    ledger = Ledger()
    checkpointer = MemoryCheckpointer()
    clock = Clock()

    # The pass comes back as a value saying what it stopped on, rather than raising it:
    # `Sleeping` carries the deadline a driver would schedule.
    suspension = await paying(ledger, checkpointer, clock, settling=SETTLING)

    assert suspension == Sleeping(key="settling", due=STARTED_AT + SETTLING)
    assert "pay" not in ledger.calls

    clock.advance(SETTLING)
    payout = finished(await paying(ledger, checkpointer, clock, settling=SETTLING))

    assert payout["reference"] == "pay-ord-88-2000"


async def test_a_wait_interrupted_partway_does_not_restart_its_clock() -> None:
    # The reason the *deadline* is recorded rather than the duration: a pass on day two
    # of a three-day wait must not push the deadline out to day five.
    ledger = Ledger()
    checkpointer = MemoryCheckpointer()
    clock = Clock()

    await paying(ledger, checkpointer, clock, settling=SETTLING)

    clock.advance(timedelta(days=2))

    second = await paying(ledger, checkpointer, clock, settling=SETTLING)

    assert second == Sleeping(key="settling", due=STARTED_AT + SETTLING)
    assert (await checkpointer.load(ORDER))["settling"] == (STARTED_AT + SETTLING).isoformat()


async def test_a_payout_over_the_threshold_waits_for_an_approval_another_process_records() -> None:
    ledger = Ledger(items=dict(BIG_ITEMS))
    checkpointer = MemoryCheckpointer()

    # `Blocked` rather than `Sleeping`: this wait ends when it is told, not when a clock
    # says so, and the type is what says which. There is no deadline on it to schedule.
    suspension = await paying(ledger, checkpointer, Clock())

    assert suspension == Blocked(waiting=frozenset({"approved-by"}))
    assert "pay" not in ledger.calls

    # Whoever took the approval writes one field into the workflow's checkpoint. It
    # shares nothing with the suspended pass, which is gone.
    await checkpointer.supply(ORDER, "approved-by", "auditor-7")
    ledger.calls.clear()

    payout = finished(await paying(ledger, checkpointer, Clock()))

    assert payout["approved_by"] == "auditor-7"
    assert payout["total"] == 94_000
    assert ledger.calls == ["pay"], "the captures happened before the approval was asked for"


async def test_a_pass_refuses_two_steps_sharing_a_name() -> None:
    checkpointer = MemoryCheckpointer()

    async def body(run: Run) -> None:
        await run.step("charged", lambda: answering("first"), as_text)
        await run.step("charged", lambda: answering("second"), as_text)

    with pytest.raises(ValueError, match="'charged' was already used in this pass"):
        await resume(await claimed(checkpointer, ORDER), checkpointer, body, now=Clock())


async def answering(value: str) -> str:
    return value


# The parsers these steps take. Written out rather than imported because deciding what
# a checkpoint holds is the application's job, and a test standing in for one does it too.
def as_count(recorded: object) -> int:
    if not isinstance(
        recorded, int
    ):  # pragma: no cover - the arm that makes this a parser rather than a cast; no test feeds it a bad value
        raise TypeError(f"{recorded!r} is not the count this step recorded")
    return recorded


async def test_reading_a_workflow_nobody_has_started_does_not_create_one() -> None:
    # The status endpoint's call for an id that has never been submitted, which is also
    # the one an unauthenticated 404 loop makes. Reading is not creating: the checkpoints
    # are a `defaultdict`, so indexing rather than asking would leave an empty entry
    # behind per miss for as long as the process lives.
    checkpointer = MemoryCheckpointer()

    assert await checkpointer.load("wf-never-submitted") == {}
    assert checkpointer.hashes == {}, "a miss left nothing behind"


async def test_a_workflow_already_being_passed_over_cannot_be_claimed_again() -> None:
    # The property the whole interface exists for. Without it, two wakeups for one workflow
    # (which the submit-then-confirm flow produces every time) run two passes side by
    # side, and both find the same step unrecorded.
    checkpointer = MemoryCheckpointer()

    holder = await claimed(checkpointer, ORDER)

    assert await checkpointer.claim(ORDER, timedelta(minutes=1)) is None
    with pytest.raises(Contended, match=f"another pass holds {ORDER!r}"):
        await claimed(checkpointer, ORDER)

    await checkpointer.release(holder)

    assert await checkpointer.claim(ORDER, timedelta(minutes=1)) is not None, "released, so the next pass may run"


async def test_a_claim_outranks_every_claim_before_it() -> None:
    checkpointer = MemoryCheckpointer()

    first = await claimed(checkpointer, ORDER)
    await checkpointer.release(first)
    second = await claimed(checkpointer, ORDER)

    assert second.token > first.token, "releasing hands the workflow back, it does not rewind the fence"


async def test_a_write_from_a_superseded_pass_is_refused_rather_than_applied() -> None:
    # A lease alone cannot do this: a pass that stalls past its lease still believes it
    # holds the workflow, and only the store knows better. The token is what tells it.
    checkpointer = MemoryCheckpointer()
    stalled = await claimed(checkpointer, ORDER)
    await checkpointer.release(stalled)
    took_over = await claimed(checkpointer, ORDER)

    with pytest.raises(Fenced, match=f"pass {stalled.token} of {ORDER!r} was superseded"):
        await checkpointer.record(stalled, "paid", "pay-from-the-dead")

    assert await checkpointer.record(took_over, "paid", "pay-real") == Recorded(value="pay-real", first=True)
    assert await checkpointer.load(ORDER) == {"paid": "pay-real"}

    await checkpointer.release(stalled)

    assert await checkpointer.claim(ORDER, timedelta(minutes=1)) is None, (
        "and a superseded pass cannot hand back a workflow that is no longer its to give"
    )


async def test_two_passes_that_both_ran_a_step_agree_on_its_result() -> None:
    # The cheaper half of the guarantee, and the one that still matters when exclusion
    # has already failed: the effect happened twice (nothing here can prevent that), but
    # the second pass is handed the first's value rather than overwriting it, so the two
    # do not carry different capture ids into everything downstream.
    checkpointer = MemoryCheckpointer()
    holder = await claimed(checkpointer, ORDER)

    won = await checkpointer.record(holder, "captured:widget", "cap-from-the-winner")
    lost = await checkpointer.record(holder, "captured:widget", "cap-from-the-loser")

    assert won == Recorded(value="cap-from-the-winner", first=True)
    assert lost == Recorded(value="cap-from-the-winner", first=False), (
        "the loser learns the winner's value instead of clobbering it, and learns that it lost"
    )


async def test_two_passes_that_recorded_the_same_value_both_count_as_the_writer() -> None:
    # A tie is not a race. Two passes that ran the same effect and produced the same
    # encoding have nothing to disagree about, so reporting the second as a loser would
    # stop a graph run over a difference that does not exist.
    checkpointer = MemoryCheckpointer()
    holder = await claimed(checkpointer, ORDER)

    await checkpointer.record(holder, "captured:widget", "cap-widget-1200")
    again = await checkpointer.record(holder, "captured:widget", "cap-widget-1200")

    assert again == Recorded(value="cap-widget-1200", first=True)


async def test_a_recorded_value_comes_back_through_the_codec_rather_than_as_it_went_in() -> None:
    # The double is a double all the way down: it encodes as every real store does, so a
    # value that does not survive the round trip fails on the first pass here, where a
    # test can see it, rather than on the second pass in production. The tuple is the
    # ordinary way to trip over this with the default JSON codec.
    checkpointer = MemoryCheckpointer()
    holder = await claimed(checkpointer, ORDER)

    recorded = await checkpointer.record(holder, "bounds", (0, 2000))

    assert recorded == Recorded(value=[0, 2000], first=True)
    assert await checkpointer.load(ORDER) == {"bounds": [0, 2000]}


async def test_a_step_returns_what_the_store_holds_rather_than_what_its_effect_produced() -> None:
    # The same property seen from inside a workflow, which is where it does its work:
    # `run.step` hands back the recorded value, so a pass whose effect ran a second time
    # still proceeds on the one result everybody agrees on.
    checkpointer = MemoryCheckpointer()
    await checkpointer.supply(ORDER, "charged", "ch-recorded-earlier")
    holder = await claimed(checkpointer, ORDER)
    run = Run(holder=holder, checkpointer=checkpointer, recorded={})

    assert await run.step("charged", lambda: answering("ch-just-now"), as_text) == "ch-recorded-earlier"


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
    checkpointer = MemoryCheckpointer()
    holder = await claimed(checkpointer, ORDER)
    run = Run(holder=holder, checkpointer=checkpointer, recorded={})
    effect = tallying("charges")

    async def charge() -> object:
        return effect(checkpointer.data)

    await run.step("charged", charge, as_count)
    checkpointer.hashes[ORDER].clear()  # the record that a crash lost
    await Run(holder=holder, checkpointer=checkpointer, recorded={}).step("charged", charge, as_count)

    assert checkpointer.data["charges"] == 2, "the card was charged twice, which is what an idempotency key is for"


async def test_a_transacted_step_cannot_be_run_without_being_recorded() -> None:
    # Exactly-once, because there is no in-between for a crash to land in: the effect and
    # its record are one commit, so a record that is missing means the effect did not
    # happen either. Losing the record the way the test above does is not something a
    # crash can produce here, so the only way to re-reach this step is a fresh pass, and
    # a fresh pass finds it recorded and performs nothing.
    checkpointer = MemoryCheckpointer()
    holder = await claimed(checkpointer, ORDER)

    first = await Run(holder=holder, checkpointer=checkpointer, recorded={}).transact(
        "charged", tallying("charges"), as_count
    )
    again = await Run(
        holder=holder,
        checkpointer=checkpointer,
        recorded=await checkpointer.load(ORDER),
    ).transact("charged", tallying("charges"), as_count)
    fresh = await Run(holder=holder, checkpointer=checkpointer, recorded={}).transact(
        "charged", tallying("charges"), as_count
    )

    assert (first, again, fresh) == (1, 1, 1), "one effect, however many passes reach it"
    assert checkpointer.data["charges"] == 1
    assert await checkpointer.load(ORDER) == {"charged": 1}


async def test_a_transacted_step_is_refused_from_a_superseded_pass() -> None:
    # The fence covers `transact` as it covers `record`, and it has to: a stalled pass
    # performing its effect is worse here than a stalled write, since the effect is real.
    checkpointer = MemoryCheckpointer()
    stalled = await claimed(checkpointer, ORDER)
    await checkpointer.release(stalled)
    await claimed(checkpointer, ORDER)

    with pytest.raises(Fenced):
        await Run(holder=stalled, checkpointer=checkpointer, recorded={}).transact(
            "charged", tallying("charges"), as_count
        )

    assert checkpointer.data == {}, "refused before the effect ran, not after"


async def test_a_deadline_recorded_as_something_else_fails_loudly() -> None:
    with pytest.raises(TypeError, match="'settling' holds 17"):
        parse_deadline("settling", 17)


async def test_a_gateway_reference_recorded_as_something_else_fails_loudly() -> None:
    # Every step's parser is a place a store holding the wrong thing gets caught, and
    # this is the one that guards the money: a capture or a payment reference read back
    # as anything but text means the checkpoint is not what this workflow wrote.
    with pytest.raises(TypeError, match="a gateway reference was recorded as 17"):
        parse_reference(17)

    assert parse_reference("cap-piano") == "cap-piano"


async def test_line_items_recorded_as_something_else_fail_loudly() -> None:
    with pytest.raises(TypeError, match="recorded as 'nope'"):
        parse_items("nope")

    with pytest.raises(TypeError, match="the amount recorded for 'widget' is '1200'"):
        parse_items({"widget": "1200"})

    # A line item worth nothing or less is refused here as well as at the API, because
    # this is where the *workflow* reads it: the approval gate compares a sum, so a
    # negative amount makes that a net rather than a total and lets a basket capture far
    # more than the gate ever saw.
    with pytest.raises(ValueError, match="the amount recorded for 'widget' is -1200"):
        parse_items({"widget": -1200})


async def test_an_approval_recorded_as_something_else_fails_loudly() -> None:
    # The one value here that a *different* process writes, so the one most worth
    # parsing rather than trusting.
    with pytest.raises(TypeError, match="the approval holds 42"):
        parse_approver(42)


async def test_the_default_clock_reads_an_aware_utc_time() -> None:
    # Deadlines are compared and serialized, so a naive clock would compare against an
    # aware deadline read back from the store and raise at the comparison.
    assert now_utc().tzinfo is UTC


async def test_a_suspension_resume_cannot_report_becomes_that_workflows_failure() -> None:
    # `Suspended` is public so a workflow author knows what must not be caught, not so it
    # can be raised: `Outcome` has an arm for each of its two subclasses and none for the
    # base. Raised anyway it would travel as an `Interruption`, which every driver's
    # `except Exception` is built to miss, so one workflow's mistake would take down the
    # loop running everyone else's. It comes back as an ordinary exception instead.
    checkpointer = MemoryCheckpointer()
    holder = await claimed(checkpointer, ORDER)

    async def confused(run: Run) -> None:
        raise Suspended("somewhere", "for something nobody named")

    with pytest.raises(TypeError, match="Suspended is not a suspension a pass can report"):
        await resume(holder, checkpointer, confused)


async def test_a_failed_capture_takes_its_siblings_down_with_the_pass() -> None:
    # `gather` raises the first failure and leaves the rest running, which here would mean
    # a capture still in flight after the pass that spawned it is over: it would record
    # into a claim the driver has already released, under a token the store has stopped
    # honouring, and nobody would be waiting to see it fail. The fan-out is owned instead,
    # so its survivors end when it does.
    checkpointer = MemoryCheckpointer()
    ledger = Ledger(broken={"capture:gizmo"}, blocked={"capture:widget"})

    with pytest.raises(RuntimeError, match="capture:gizmo is down"):
        await paying(ledger, checkpointer, Clock())

    assert "cancelled:widget" in ledger.calls, "the sibling was cancelled rather than left running"
    assert [key for key in await checkpointer.load(ORDER) if key.startswith("captured:")] == [], (
        "and neither capture recorded, so the next pass runs both"
    )


async def test_a_step_cancelled_while_writing_still_records_the_effect_it_performed() -> None:
    # Cancelling a step that has already called the gateway does not undo the charge, it
    # only removes the record of it, so the next pass charges again. And a step is
    # cancelled in the ordinary course of a fan-out (the test above is exactly that), not
    # only in a crash, so the sibling parked in its write is the common case rather than
    # an exotic one.
    checkpointer = ParkedWrites()
    holder = await claimed(checkpointer, ORDER)
    charging = asyncio.ensure_future(
        Run(holder=holder, checkpointer=checkpointer, recorded={}).step("charged", lambda: answering("ch-1"), as_text)
    )
    await checkpointer.writing.wait()

    charging.cancel()
    checkpointer.proceed.set()
    with pytest.raises(asyncio.CancelledError):
        await charging

    assert await checkpointer.load(ORDER) == {"charged": "ch-1"}, "the effect happened, so its record has to land"


async def test_a_suspension_raised_under_a_task_group_is_still_an_outcome() -> None:
    # A workflow that fans its steps out with a `TaskGroup` raises a `BaseExceptionGroup`,
    # which is neither an `Outcome` nor anything a driver's `except Exception` reaches, so
    # the one workflow that suspended under a group would take down the loop running every
    # other one. The suspension is unwrapped instead, and the pass reports what it stopped
    # at exactly as a bare one does.
    checkpointer = MemoryCheckpointer()
    holder = await claimed(checkpointer, ORDER)

    async def body(run: Run) -> None:
        async with asyncio.TaskGroup() as group:
            group.create_task(run.step("charged", lambda: answering("ch-1"), as_text))
            group.create_task(run.sleep("settling", SETTLING))

    outcome = await resume(holder, checkpointer, body, now=Clock())

    assert outcome == Sleeping(key="settling", due=STARTED_AT + SETTLING)
    assert (await checkpointer.load(ORDER))["charged"] == "ch-1", "the sibling's own step still landed"


async def test_a_suspension_nested_two_task_groups_deep_is_still_an_outcome() -> None:
    # Groups nest, so the group a suspension arrives in can hold another group rather than
    # the suspension itself. Reading only the outer layer would report the workflow as
    # having raised something unreportable, which is the same harm one layer down.
    checkpointer = MemoryCheckpointer()
    holder = await claimed(checkpointer, ORDER)

    async def inner(run: Run) -> None:
        async with asyncio.TaskGroup() as group:
            group.create_task(run.sleep("settling", SETTLING))

    async def body(run: Run) -> None:
        async with asyncio.TaskGroup() as group:
            group.create_task(inner(run))

    assert await resume(holder, checkpointer, body, now=Clock()) == Sleeping(key="settling", due=STARTED_AT + SETTLING)


async def test_a_task_group_that_both_suspends_and_fails_reports_the_failure() -> None:
    # The suspension is not an excuse to swallow the error beside it: `except*` takes the
    # arm it understands and re-raises the rest, which arrives as an ordinary
    # `ExceptionGroup` (every leaf left is an `Exception`) and so is caught by a driver as
    # that workflow's failure.
    checkpointer = MemoryCheckpointer()
    holder = await claimed(checkpointer, ORDER)

    async def declining() -> None:
        raise RuntimeError("the gateway declined")

    async def body(run: Run) -> None:
        async with asyncio.TaskGroup() as group:
            group.create_task(run.sleep("settling", SETTLING))
            group.create_task(declining())

    with pytest.raises(ExceptionGroup) as raised:
        await resume(holder, checkpointer, body, now=Clock())

    assert [type(each) for each in raised.value.exceptions] == [RuntimeError]


async def test_several_suspensions_at_once_report_the_earliest_deadline() -> None:
    # A pass has one outcome and a group can stop at several waits. A deadline wins over a
    # wait for input, because a wakeup is the only one of the two this driver can answer;
    # the earliest wins among deadlines, because a pass that wakes early suspends again
    # and one that wakes late kept a branch waiting for nothing.
    #
    # Asserted on the choice itself rather than through a task group, because a group
    # cancels its siblings the instant one of them raises: which suspensions arrive
    # together is a race, and what to do with the ones that did is not.
    reached: list[Suspended] = [
        InputNeeded("approved-by"),
        ScheduledWakeup("settling", due=STARTED_AT + SETTLING),
        ScheduledWakeup("clearing", due=STARTED_AT + timedelta(hours=1)),
    ]

    assert stopped_at([], reached) == Sleeping(key="clearing", due=STARTED_AT + timedelta(hours=1))


async def test_every_key_a_pass_waits_on_is_reported_rather_than_one_of_them() -> None:
    # The other half of the choice above: with no deadline among them there is nothing to
    # schedule, and the driver is told to leave the workflow alone until somebody writes.
    # *Which* somebody is the whole content of the outcome, so a pass blocked on two keys
    # reports two: naming one would tell a client the workflow needs an approval when it
    # needs an approval and a countersignature, and the client would supply one and wait.
    assert stopped_at([InputNeeded("approved-by"), InputNeeded("countersigned-by")], []) == Blocked(
        waiting=frozenset({"approved-by", "countersigned-by"})
    )


async def test_a_wait_for_a_message_is_reported_apart_from_a_wait_for_a_value() -> None:
    # Two fields rather than two types, and the distinction they carry is what to *do*: a
    # `waiting` key is an address a client answers with `arrive`, and a `listening` key
    # names the read step that stopped, which nobody writes to, so the answer is `deliver`.
    assert stopped_at([MessageNeeded("heard")], []) == Blocked(listening=frozenset({"heard"}))


async def test_a_pass_that_both_waits_and_listens_reports_both() -> None:
    # The case a type per kind could not express. A fan-out blocked on an approval *and* an
    # empty inbox is blocked on both at once, and either write advances it, so reporting
    # one and discarding the other hid a way to unblock the workflow. It was unstable as
    # well as lossy: the winner was whichever branch reached its raise first.
    assert stopped_at([MessageNeeded("heard"), InputNeeded("approved-by")], []) == Blocked(
        waiting=frozenset({"approved-by"}), listening=frozenset({"heard"})
    )


async def test_the_report_does_not_depend_on_which_branch_raised_first() -> None:
    # The same suspensions in the other order, which is what task scheduling decides. A set
    # has no order to leak, so this is the property falling out of the shape rather than
    # being arranged, and it is what stops two passes at one workflow disagreeing.
    raised = [InputNeeded("zzz"), MessageNeeded("heard"), InputNeeded("aaa")]

    assert stopped_at(raised, []) == stopped_at(list(reversed(raised)), [])


async def test_a_deadline_beats_a_wait_for_a_message_too() -> None:
    # A `Blocked` is answered by whoever writes next, which is not this driver; the
    # deadline is answered by this driver and by nobody else, so reporting the wait would
    # leave a branch that asked for a clock with nothing scheduled. This is the one place
    # the outcome still drops information, and it is bounded: the pass the wakeup produces
    # reaches those branches again.
    raised: list[Suspended] = [MessageNeeded("heard"), ScheduledWakeup("settling", due=STARTED_AT + SETTLING)]

    assert stopped_at(raised, []) == Sleeping(key="settling", due=STARTED_AT + SETTLING)


async def test_a_gather_that_propagates_one_suspension_still_reports_them_all() -> None:
    # `asyncio.gather` raises the *first* exception rather than a group, so the siblings
    # never reach `resume` at all. Reading the report off what the pass *reached* is what
    # makes it independent of which combinator the workflow used: written with a
    # `TaskGroup` this same body reports both keys, and it must not matter which was used.
    checkpointer = MemoryCheckpointer()

    async def gathered(run: Run) -> None:
        await asyncio.gather(run.awaiting("aaa", as_text), run.awaiting("zzz", as_text))

    assert await resume(await claimed(checkpointer, ORDER), checkpointer, gathered) == Blocked(
        waiting=frozenset({"aaa", "zzz"})
    )


async def test_a_workflow_that_catches_a_suspension_and_returns_is_that_workflows_error() -> None:
    # The failure this makes loud. `asyncio.wait` hands back done futures rather than
    # raising, so a suspension captured there never reaches `resume`, and the pass would
    # otherwise report `Completed` for a workflow still waiting on the world: nothing wakes
    # a finished workflow, and no record says a wait went unanswered.
    checkpointer = MemoryCheckpointer()

    async def swallowing(run: Run) -> str:
        waiting = asyncio.ensure_future(run.awaiting("approved-by", as_text))
        done, pending = await asyncio.wait([waiting], return_when=asyncio.FIRST_COMPLETED)
        for task in pending:  # pragma: no cover - nothing is pending; the wait suspended at once
            task.cancel()
        return f"{len(done)} done"

    with pytest.raises(Swallowed, match=r"caught a suspension at \('approved-by',\)"):
        await resume(await claimed(checkpointer, ORDER), checkpointer, swallowing)


async def test_a_workflow_that_finishes_without_suspending_is_still_completed() -> None:
    # The control for the check above: `reached` is empty on a pass that never waited, so
    # an ordinary workflow is unaffected by it. Without this, a `Swallowed` raised on every
    # pass would look like the check working.
    checkpointer = MemoryCheckpointer()

    async def straight_through(run: Run) -> str:
        return await run.step("charged", lambda: answering("ch-1"), as_text)

    assert await resume(await claimed(checkpointer, ORDER), checkpointer, straight_through) == Completed(value="ch-1")


async def test_a_pass_blocked_on_nothing_is_not_a_state_that_can_be_built() -> None:
    # An empty `Blocked` would say a workflow stopped for no reason, and a driver reading
    # one would park a workflow that nothing will ever wake.
    with pytest.raises(ValueError, match="blocked on something"):
        Blocked()


async def test_a_receive_takes_what_is_in_the_inbox_and_records_how_far_it_read() -> None:
    checkpointer = MemoryCheckpointer()
    first = await checkpointer.append(ORDER, "hello")
    second = await checkpointer.append(ORDER, "again")

    async def body(run: Run) -> tuple[Entry, ...]:
        return await run.receive("heard")

    assert await resume(await claimed(checkpointer, ORDER), checkpointer, body) == Completed(value=(first, second))
    # A key rather than a copy of the values, which is what makes the record small and what
    # makes it correct: entries are immutable, so naming the last one replays exactly.
    assert (await checkpointer.load(ORDER))["heard"] == second.key


async def test_a_pending_read_of_an_empty_inbox_returns_nothing_instead_of_suspending() -> None:
    checkpointer = MemoryCheckpointer()

    async def body(run: Run) -> tuple[Entry, ...]:
        return await run.pending("folded")

    assert await resume(await claimed(checkpointer, ORDER), checkpointer, body) == Completed(value=())


async def test_a_pending_read_that_took_nothing_still_replays_to_nothing() -> None:
    # The write is the whole point of `pending` recording an empty read. Left unrecorded, a
    # replay would re-evaluate against a fuller inbox and hand the second pass entries the
    # first one never saw, which is the divergence every step here exists to prevent.
    checkpointer = MemoryCheckpointer()

    async def body(run: Run) -> tuple[Entry, ...]:
        return await run.pending("folded")

    holder = await claimed(checkpointer, ORDER)
    first = await resume(holder, checkpointer, body)
    await checkpointer.release(holder)
    await checkpointer.append(ORDER, "arrived later")

    assert await resume(await claimed(checkpointer, ORDER), checkpointer, body) == first


async def test_a_pending_read_resumes_from_the_cursor_it_was_given() -> None:
    checkpointer = MemoryCheckpointer()
    first = await checkpointer.append(ORDER, "hello")
    second = await checkpointer.append(ORDER, "again")

    async def body(run: Run) -> tuple[tuple[Entry, ...], tuple[Entry, ...]]:
        opened = await run.pending("opened", limit=1)
        return opened, await run.pending("rest", after=opened[-1].key)

    assert await resume(await claimed(checkpointer, ORDER), checkpointer, body) == Completed(
        value=((first,), (second,))
    )


async def test_a_step_may_not_take_a_name_out_of_the_inbox_key_space() -> None:
    # The collision worth failing on: a step named into the inbox's keys would be read back
    # by `receive` as a message somebody delivered, silently, and re-running would never
    # reveal it. The store owns those names.
    checkpointer = MemoryCheckpointer()

    async def body(run: Run) -> None:
        await run.step(inbox_key(3), lambda: answering("mine"), as_text)

    with pytest.raises(ValueError, match="is in the inbox key space"):
        await resume(await claimed(checkpointer, ORDER), checkpointer, body)


async def test_a_cursor_recorded_as_something_else_fails_loudly() -> None:
    with pytest.raises(TypeError, match="is not a cursor this workflow wrote"):
        parse_bound("heard", 7)


async def test_a_transacted_effect_that_fails_leaves_the_stores_data_alone() -> None:
    # `transact` MUST NOT leave the effect applied without its record, and an effect that
    # moves the data and *then* raises is the way to reach that state: without a rollback
    # the next pass finds nothing recorded and runs the effect again over data it has
    # already moved. The double owes this like any store, or every test written against it
    # is a test of something production does not do.
    checkpointer = MemoryCheckpointer(data={"balance": 100})
    holder = await claimed(checkpointer, ORDER)

    def debit(data: dict[str, object]) -> object:
        balance = data["balance"]
        assert isinstance(balance, int)
        data["balance"] = balance - 20
        raise RuntimeError("the ledger refused the debit")

    with pytest.raises(RuntimeError, match="refused the debit"):
        await Run(holder=holder, checkpointer=checkpointer, recorded={}).transact("debited", debit, as_count)

    assert checkpointer.data == {"balance": 100}, "the effect went back with the record that never landed"
    assert await checkpointer.load(ORDER) == {}


async def test_losing_the_claim_under_a_task_group_still_arrives_as_the_interruption_it_is() -> None:
    # The same harm as a wrapped suspension, one door along: a group whose only leaf is a
    # `Fenced` is a `BaseExceptionGroup`, so a driver's `except (Fenced, Contended)` does
    # not match it and its `except Exception` cannot reach it, and the workflow that
    # merely lost its claim takes down the loop running every other one. It is unwrapped
    # for the same reason suspensions are, and raised bare so the driver sees what it was
    # written to catch.
    checkpointer = MemoryCheckpointer()
    stalled = await claimed(checkpointer, ORDER)
    await checkpointer.release(stalled)
    await claimed(checkpointer, ORDER)  # the winner, which outranks the stalled pass

    async def body(run: Run) -> None:
        async with asyncio.TaskGroup() as group:
            group.create_task(run.step("charged", lambda: answering("ch-1"), as_text))

    with pytest.raises(Fenced):
        await resume(stalled, checkpointer, body, now=Clock())


def test_a_lost_claim_is_reported_over_a_sibling_that_failed_beside_it() -> None:
    # A pass that may not write has nothing to say about a gateway declining: the decline
    # is a consequence, and reporting it would have a driver log a workflow failure for a
    # workflow that is fine and being advanced by whoever holds the claim.
    #
    # Asserted on the rule rather than through a task group, because a group cancels its
    # siblings the instant one of them raises: which leaves end up in it together is a
    # race, and what to do with the ones that did is not.
    lost = Fenced("pass 1 was superseded")

    with pytest.raises(Fenced) as raised:
        unwound(BaseExceptionGroup("fan-out", [RuntimeError("the gateway declined"), lost]), [])

    assert raised.value is lost


def test_a_fan_out_that_only_failed_is_re_raised_without_its_suspensions() -> None:
    # The other half: with no claim lost, the failures are the news. They come back as an
    # ordinary `ExceptionGroup` rather than the `BaseExceptionGroup` they arrived in,
    # because a suspension left among them would keep the group invisible to the driver's
    # `except Exception`, which is the whole harm being repaired.
    with pytest.raises(ExceptionGroup) as raised:
        unwound(
            BaseExceptionGroup("fan-out", [InputNeeded("approved-by"), RuntimeError("the gateway declined")]),
            [],
        )

    assert [type(each) for each in raised.value.exceptions] == [RuntimeError]
    assert isinstance(raised.value, Exception), "a driver's `except Exception` has to be able to see it"


async def test_a_deadline_whose_suspension_was_cancelled_is_still_reported() -> None:
    # `sleep` records the deadline and *then* raises, and a task group cancels its other
    # branches the instant one of them raises, so the branch that had just written its
    # deadline can be cancelled in between. The record is durable by then. Without the
    # deadline being noted on the pass, nothing reports it: the driver hears `Blocked`,
    # schedules no wakeup, and the workflow waits out a clock that will never fire while
    # its checkpoint holds the deadline that was supposed to end the wait.
    checkpointer = ParkedWrites()
    holder = await claimed(checkpointer, ORDER)

    async def waiting_on_a_person(run: Run) -> None:
        # Held until the sleeping branch is *inside* its write, then let go, so the
        # cancellation lands exactly where the defect lives rather than wherever the
        # scheduler happens to put it. That instant is the one that produces a durable
        # deadline with no suspension to report it: the write is held past the
        # cancellation, and the raise that would have announced it never runs.
        await checkpointer.writing.wait()
        checkpointer.proceed.set()
        await run.awaiting("approved-by", parse_approver)

    async def body(run: Run) -> None:
        async with asyncio.TaskGroup() as group:
            group.create_task(run.sleep("timing-out", SETTLING))
            group.create_task(waiting_on_a_person(run))

    outcome = await resume(holder, checkpointer, body, now=Clock())

    assert outcome == Sleeping(key="timing-out", due=STARTED_AT + SETTLING), (
        "the branch that asked for a clock is the one a driver can answer"
    )
    assert (await checkpointer.load(ORDER))["timing-out"] == (STARTED_AT + SETTLING).isoformat()


async def test_a_step_cancelled_twice_still_records_the_effect_it_performed() -> None:
    # One cancelled pass delivers two cancellations, which is the ordinary case rather
    # than a caller being emphatic: a fan-out gathered under `asyncio.gather` cancels its
    # children when the pass is cancelled, and the gather returns as soon as the first
    # child answers, so the caller's own teardown cancels the rest again. Honouring the
    # second one drops a write whose gateway call has already happened, which is the
    # charge the first one was held for.
    checkpointer = ParkedWrites()
    holder = await claimed(checkpointer, ORDER)
    charging = asyncio.ensure_future(
        Run(holder=holder, checkpointer=checkpointer, recorded={}).step("charged", lambda: answering("ch-1"), as_text)
    )
    await checkpointer.writing.wait()

    charging.cancel()
    await asyncio.sleep(0)  # a turn for the step to reach the wait that holds the write
    charging.cancel()
    checkpointer.proceed.set()
    with pytest.raises(asyncio.CancelledError):
        await charging

    assert await checkpointer.load(ORDER) == {"charged": "ch-1"}, "the effect happened, so its record has to land"


async def test_a_step_cancelled_over_a_write_that_failed_records_nothing() -> None:
    # The other half of holding the write past a cancellation: a write that did not land
    # leaves nothing behind. Its failure belongs to the pass being torn down rather than
    # to whoever cancelled it, so it is dropped rather than raised in the cancellation's
    # place, and the key stays absent so a later pass performs the effect again, which is
    # the at-least-once bound `step` already documents.
    checkpointer = ParkedWrites(refuse=True)
    holder = await claimed(checkpointer, ORDER)
    run = Run(holder=holder, checkpointer=checkpointer, recorded={})
    charging = asyncio.ensure_future(run.step("charged", lambda: answering("ch-1"), as_text))
    await checkpointer.writing.wait()

    charging.cancel()
    checkpointer.proceed.set()
    with pytest.raises(asyncio.CancelledError):
        await charging

    assert run.recorded == {}, "a write that failed is not a record"
    assert await checkpointer.load(ORDER) == {}


async def test_a_sleep_cancelled_before_its_deadline_landed_schedules_nothing() -> None:
    # The deadline is read back off the pass, so there has to be one: a `sleep` cancelled
    # while its write was still in flight *and* failing has recorded nothing, and there is
    # no deadline to report. The pass says it is waiting on the value it can name, and the
    # next pass writes the deadline afresh.
    checkpointer = ParkedWrites(refuse=True)
    holder = await claimed(checkpointer, ORDER)

    async def waiting_on_a_person(run: Run) -> None:
        await checkpointer.writing.wait()
        checkpointer.proceed.set()
        await run.awaiting("approved-by", parse_approver)

    async def body(run: Run) -> None:
        async with asyncio.TaskGroup() as group:
            group.create_task(run.sleep("timing-out", SETTLING))
            group.create_task(waiting_on_a_person(run))

    assert await resume(holder, checkpointer, body, now=Clock()) == Blocked(waiting=frozenset({"approved-by"}))
    assert await checkpointer.load(ORDER) == {}


async def test_an_order_with_no_line_items_is_refused_where_the_workflow_reads_it() -> None:
    # The API refuses an empty basket, and so does this: what a store hands back is
    # external input to the workflow however it got there, and an order with nothing in it
    # captures nothing, pays a gateway zero, and records a `paid` for it.
    with pytest.raises(ValueError, match="the line items are empty"):
        parse_items({})


async def test_a_held_payout_stays_held_when_the_threshold_moves_between_passes() -> None:
    # The approval gate is the one decision here that a later pass must not re-litigate.
    # `approval_over` is a deployment's setting rather than the workflow's own value, so a
    # deploy between two passes would otherwise find a held payout no longer held and pay
    # it with nobody in the loop, leaving a checkpoint that shows neither the approval nor
    # that one was ever wanted.
    ledger = Ledger(items=dict(BIG_ITEMS))
    checkpointer = MemoryCheckpointer()

    async def body(run: Run, over: int) -> dict[str, object]:
        return await pay_out(run, ORDER, ledger.services(), settling=timedelta(), approval_over=over)

    holder = await claimed(checkpointer, ORDER)
    assert await resume(holder, checkpointer, lambda run: body(run, APPROVAL_OVER)) == Blocked(
        waiting=frozenset({"approved-by"})
    )
    await checkpointer.release(holder)

    raised = await claimed(checkpointer, ORDER)
    outcome = await resume(raised, checkpointer, lambda run: body(run, 1_000_000))

    assert outcome == Blocked(waiting=frozenset({"approved-by"})), (
        "the pass that held it decided; a later one abides by that"
    )
    assert "paid" not in await checkpointer.load(ORDER)


async def test_an_approval_gate_recorded_as_something_else_fails_loudly() -> None:
    with pytest.raises(TypeError, match="the approval gate holds 'yes'"):
        parse_held("yes")
