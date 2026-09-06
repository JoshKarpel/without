from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from enum import IntEnum
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
from without_durability import LEASE
from without_durability import Checkpointer
from without_durability import Contended
from without_durability import Delivery
from without_durability import Entry
from without_durability import Fenced
from without_durability import MemoryCheckpointer
from without_durability import Pass
from without_durability import Recorded
from without_durability import SplitDurable
from without_durability import Written
from without_durability import claimed
from without_durability import inbox_key
from without_durability import now_utc
from without_durability import run_durably
from without_durability.graph import survives
from without_durability.graph import written

from .helpers import ParkedWrites


class Unwound(Exception):
    """A workflow that was compensated, and so must not be run again under its own id."""


ORDER = Order(order_id="o-7", sku="widget", cents=2500)


class Tier(IntEnum):
    """A domain value that is an `int` and is not one, which is the pairing that matters."""

    BASIC = 1
    GOLD = 2


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
    """
    Run `forward`, and on failure compensate with `unwind` before re-raising.

    Written out here rather than imported, because there is no saga runner to import:
    a compensation is another graph, so unwinding is a second `run_durably` under a
    second id. This is the whole of it, and it is this short because the checkpoint is
    a *value*: what the forward run achieved is a mapping of results, so deciding what
    to give back is a pure function of it (`reaches`) rather than a replay log or an
    engine to interrogate.

    The rollback goes through `run_durably` too, so it is checkpointed exactly like the
    forward run and a crash partway through it resumes rather than refunding twice. Its
    id is `:unwind` beside the workflow's here, but that is this driver's choice out of
    its own namespace and nothing in the library knows the string.

    Two things fall out of the shapes rather than being arranged. The original failure
    is re-raised once the compensation lands, since compensating does not make the
    workflow succeed, and a failure *inside* the compensation propagates in its place,
    carrying the original as context, because a half-unwound saga is the more urgent
    problem. And `except Exception` is already the right net: cancellation and every
    `Interruption` descend from `BaseException`, and each of them stops for a reason
    that makes unwinding actively wrong (a `Fenced` loser that compensated would refund
    a charge the winner is still building on).
    """
    holder = await claimed(checkpointer, workflow)
    try:
        return await compensating(forward, unwind, reaches, checkpointer, holder, value)
    finally:
        await checkpointer.release(holder)


async def compensating[In, Out, Reaches, Undone](
    forward: CompiledGraph[In, Out],
    unwind: CompiledGraph[Reaches, Undone],
    reaches: Callable[[Mapping[str, object]], Reaches],
    checkpointer: Checkpointer,
    holder: Pass,
    value: In,
) -> Out:
    """
    The saga itself, under a claim somebody else took, which is where the shapes show.

    A compensated workflow is *finished*, and saying so is the driver's job because the id
    namespace is the driver's. Nothing in the forward checkpoint records that its steps
    were given back, so a client retrying the same idempotency key would resume it, find
    every node recorded, and ship against a charge that has been refunded and stock that
    has been released. The rollback records `unwound` under the forward id, and this
    refuses to run a workflow carrying it.
    """
    if "unwound" in await checkpointer.load(holder.workflow):
        raise Unwound(f"{holder.workflow!r} was compensated, so its steps have been given back")
    try:
        return await run_durably(forward, checkpointer, holder, value)
    except Exception:
        undoing = await claimed(checkpointer, f"{holder.workflow}:unwind")
        try:
            await run_durably(unwind, checkpointer, undoing, reaches(await checkpointer.load(holder.workflow)))
            await checkpointer.supply(holder.workflow, "unwound", True)
        finally:
            await checkpointer.release(undoing)
        raise


@dataclass(slots=True)
class Gateway:
    """
    The outside world: records every effect, and fails or hangs where told to.

    `blocked` is what makes a failing sibling deterministic. An effect named there
    parks until the run cancels it, so it can never win a race to complete, and a
    test can pin exactly which effects reached the world before the failure. It says
    so when it is cancelled, which turns "the sibling was stopped" from something a
    test infers from an absence into something it can assert.
    """

    calls: list[str] = field(default_factory=list)
    broken: set[str] = field(default_factory=set)
    blocked: set[str] = field(default_factory=set)
    # An effect named here raises *from its own cleanup* when it is cancelled, which is
    # ordinary in a saga (an undo that fails) and is the one thing a teardown cannot be
    # allowed to report in place of what brought the run down.
    unclean: set[str] = field(default_factory=set)

    async def perform(self, effect: str, result: str) -> str:
        if effect in self.blocked:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.calls.append(f"cancelled:{effect}")
                if effect in self.unclean:
                    raise RuntimeError(f"{effect} could not be cleaned up") from None
                raise
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

    assert await checkpointer.load("wf-5") == {"unwound": True}, "compensated, so this id is finished"
    assert gateway.calls == ["charge", "cancelled:reserve"], (
        "the reservation was cancelled mid-flight, so there is nothing to release"
    )
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

    async def append(self, workflow: str, value: object) -> Entry:  # pragma: no cover - a graph has no inbox
        key = inbox_key(len(self.already))
        self.already[key] = value
        return Entry(key=key, value=value)

    async def history(self, workflow: str) -> dict[str, Written]:  # pragma: no cover - unused here
        return {key: Written(value=value, at=now_utc()) for key, value in self.already.items()}

    async def discard(self, workflow: str) -> int:  # pragma: no cover - unused here
        removed = len(self.already)
        self.already.clear()
        return removed

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


async def test_a_pass_that_lost_the_workflow_stops_the_effects_it_had_in_flight() -> None:
    # Losing the workflow means stopping, and stopping has to include the nodes this pass
    # already spawned: they are siblings of the refused one, still performing effects
    # against a workflow somebody else is now advancing, so the winner's charge is joined
    # by the loser's. An abandoned async generator is not a stopped one, which is why the
    # stream is closed on the way out rather than left to the traceback: closing it is
    # what reaches the `finally` that cancels them.
    checkpointer = MemoryCheckpointer()
    gateway = Gateway(blocked={"reserve"})

    stalled = await claimed(checkpointer, "wf-fenced")
    await checkpointer.release(stalled)
    await claimed(checkpointer, "wf-fenced")  # the winner, which outranks the stalled pass

    with pytest.raises(Fenced):
        await run_durably(fulfilment(gateway.services()), checkpointer, stalled, ORDER)

    assert "cancelled:reserve" in gateway.calls, "the sibling ended with the pass rather than running on"


async def test_a_node_whose_result_changes_type_without_changing_value_is_refused() -> None:
    # Equality is not the question the guard is asking. An `IntEnum` is stored as its
    # number and restored as a plain `int`, comparing equal the whole way, so the pass
    # that computed the node feeds its dependents `Tier.GOLD` and the pass that resumed it
    # feeds them `1`. Nothing raised and the two passes computed different answers, which
    # is exactly the divergence this check exists to prevent.
    graph, (order,) = Graph.of(Order)

    async def tier(placed: Order) -> Tier:
        return Tier.GOLD if placed.cents > 1000 else Tier.BASIC

    run = graph.build(output=graph.node("tier", tier, order))

    with pytest.raises(TypeError, match=re.escape("'tier' returned <Tier.GOLD: 2>")):
        await durably(run, MemoryCheckpointer(), "wf-enum", ORDER)


@pytest.mark.parametrize(
    ("sent", "restored", "intact"),
    [
        ({"sku": "widget", "cents": 2500}, {"sku": "widget", "cents": 2500}, True),
        ([1, 2, 3], [1, 2, 3], True),
        ("cap-o-7", "cap-o-7", True),
        ((0, 2500), [0, 2500], False),
        (Tier.GOLD, 2, False),
        ([Tier.GOLD], [2], False),
        ({"tier": Tier.GOLD}, {"tier": 2}, False),
        ({Tier.GOLD: "premium"}, {2: "premium"}, False),
        ({Tier.GOLD}, {2}, False),
        ({1: "a"}, {"1": "a"}, False),
        ([1, 2], [1, 2, 3], False),
        ({"a": 1}, {"b": 1}, False),
        ({"a": 1}, {"a": 1, "b": 2}, False),
        ({"gold"}, {"gold", "bronze"}, False),
        ({"gold"}, {"bronze"}, False),
        ({"gold", "bronze"}, {"bronze", "gold"}, True),
        ("ch-1", "ch-2", False),
        ({"b": 1, "aa": 2}, {"aa": 2, "b": 1}, True),
    ],
    ids=[
        "a mapping that came back whole",
        "a list that came back whole",
        "text, which is a value rather than a sequence of them",
        "a tuple, which JSON has no room for",
        "an enum, restored as the number it is",
        "the same enum, one container down",
        "and one mapping down",
        "and as a mapping's key, where equality finds its own number",
        "and inside a set",
        "a key that is no longer the key it was",
        "a length that changed",
        "a name that changed",
        "a mapping that grew a key",
        "a set that grew a member",
        "a set whose member is not the one that went in",
        "a set a store handed back in its own order",
        "text that is simply different",
        "a mapping a store handed back in its own order",
    ],
)
def test_a_result_survives_its_store_only_if_its_shape_survives_too(
    sent: object,
    restored: object,
    intact: bool,
) -> None:
    # What the guard has to answer is "would a later pass hand the dependents this same
    # value", and equality answers a different question: an `IntEnum` compares equal to
    # the number it is stored as, at the top level and inside every container, so an
    # equality check passes a node whose dependents see a different type on every pass
    # after the first.
    assert survives(sent, restored) is intact


async def test_a_node_whose_result_round_trips_is_recorded_without_complaint() -> None:
    # The check is on the round trip, not on identity: an equal value that took a
    # different object identity through the codec is exactly the ordinary case.
    graph, (order,) = Graph.of(Order)

    async def lines(placed: Order) -> dict[str, int]:
        return {placed.sku: placed.cents}

    run = graph.build(output=graph.node("lines", lines, order))

    assert await durably(run, MemoryCheckpointer(), "wf-round-trips", ORDER) == {"widget": 2500}


async def test_a_graph_whose_output_is_one_of_its_entries_is_refused_before_it_runs() -> None:
    # `evaluate` runs such a graph happily, because an entry it was handed is a value it
    # can return. Durably it is not runnable at all: the output is read back out of the
    # checkpoint, and an entry is the one thing a checkpoint never holds, so without this
    # the run would perform every effect and *then* fail looking for a key that was never
    # going to be written.
    graph, (order,) = Graph.of(Order)
    gateway = Gateway()
    graph.node("charged", gateway.services().charge, order)
    run = graph.build(output=order)

    with pytest.raises(ValueError, match="'input:0' is one of this graph's entries rather than a node"):
        await durably(run, MemoryCheckpointer(), "wf-identity", ORDER)

    assert gateway.calls == [], "and it is refused before the first effect, not after"


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
        await compensating(fulfilment(services), unwinding(services), Reached.of, checkpointer, stalled, ORDER)

    assert "refund" not in gateway.calls, "the loser left the winner's charge alone"
    assert await checkpointer.load("wf-lost:unwind") == {}, "and took no claim on the rollback"


async def test_reading_a_checkpoint_written_by_something_else_fails_loudly() -> None:
    with pytest.raises(TypeError, match="'charged' was recorded as 17"):
        recorded_id({"charged": 17}, "charged")


@dataclass(frozen=True, slots=True)
class UnreachableQueue:
    """A `Scheduler` that cannot be written to, standing in for a crash mid-`arrive`."""

    lease: timedelta = LEASE

    async def prepare(self) -> None:  # pragma: no cover - unused here
        return None

    async def make_ready(self, workflow: str) -> None:
        raise RuntimeError("the queue is down")

    async def wake_at(self, delivery: Delivery, when: datetime) -> None:  # pragma: no cover - unused here
        return None

    async def wake_due(self, now: datetime) -> tuple[str, ...]:  # pragma: no cover - unused here
        return ()

    async def next_ready(self, within: timedelta) -> Delivery | None:  # pragma: no cover - unused here
        return None

    async def reclaim(self, idle: timedelta) -> Delivery | None:  # pragma: no cover - unused here
        return None

    async def cancel(self, workflow: str) -> None:  # pragma: no cover - unused here
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


def test_a_value_nested_deeper_than_the_interpreter_recurses_is_still_checked() -> None:
    # How deep a checkpointed value may be is the *codec's* business: the stdlib's JSON
    # encoder is C and round-trips a structure hundreds of levels deep without complaint.
    # A check that walked it recursively would raise `RecursionError` out of a function
    # annotated `-> bool` and fail a run over a value that survived its store perfectly.
    deep: list[object] = []
    current = deep
    for _ in range(2000):
        nested: list[object] = []
        current.append(nested)
        current = nested

    assert survives(deep, deep) is True
    assert survives(deep, [[]]) is False


async def test_a_pass_that_lost_the_workflow_says_so_even_when_a_sibling_failed_first() -> None:
    # Tearing the run down must not change what it is reporting. The nodes still in flight
    # are cancelled and awaited on the way out, and one of them may have *already failed*
    # while the record was in flight; awaiting that one re-raises its exception from the
    # teardown, replacing the `Fenced` the caller needs to see. A worker told a workflow
    # failed, when what happened is that another pass owns the workflow, logs it as data
    # and acknowledges the wakeup, and nothing wakes it again.
    checkpointer = MemoryCheckpointer()
    gateway = Gateway(broken={"reserve"})

    stalled = await claimed(checkpointer, "wf-both")
    await checkpointer.release(stalled)
    await claimed(checkpointer, "wf-both")  # the winner, which outranks the stalled pass

    with pytest.raises(Fenced):
        await run_durably(fulfilment(gateway.services()), checkpointer, stalled, ORDER)


async def test_a_saga_does_not_unwind_an_order_that_is_already_in_the_air() -> None:
    # Shipping has no compensation, which is why it is the last effect the workflow
    # performs. But "last effect" is not "last thing that can fail": its record is a
    # second round trip and the node that folds the receipt runs after it, so a failure
    # can land with the parcel already gone. Refunding then leaves the customer holding
    # goods nobody was paid for and the warehouse believing it still has them, which is
    # worse than the failure that prompted it. What was reached is a value, so the
    # compensation can read the one fact that says to stand down.
    gateway = Gateway()
    reached = Reached.of({"charged": "ch-o-7", "reserved": "rs-widget", "shipped": "tr-ch-o-7-rs-widget"})

    rollback = await durably(unwinding(gateway.services()), MemoryCheckpointer(), "wf-shipped:unwind", reached)

    assert rollback == {"refunded": None, "released": None}
    assert gateway.calls == [], "nothing to undo once the parcel is in the air"


def test_a_value_that_reaches_itself_is_answered_rather_than_walked_forever() -> None:
    # A codec that preserves references (a pickle, anything with a reference table) meets
    # both of `CheckpointCodec`'s requirements and can carry a value that reaches itself,
    # and a tree with parent pointers is an ordinary domain value rather than a contrived
    # one. Without remembering the pairs it has compared, the walk follows the cycle until
    # it runs out of memory: not a failed run but one that never returns.
    tree: dict[str, object] = {"name": "root"}
    tree["parent"] = tree
    restored: dict[str, object] = {"name": "root"}
    restored["parent"] = restored

    assert survives(tree, restored) is True
    assert survives(tree, {"name": "root", "parent": {"name": "root"}}) is False


def test_a_value_reachable_by_many_paths_is_compared_once() -> None:
    # The same memo, doing the other half of its job: a value shared by many paths through
    # a structure would otherwise be walked once per path, which is exponential in the
    # depth. Twenty-four levels of sharing is a 161-byte encoding and was over a minute of
    # comparison.
    shared: object = [0]
    for _ in range(24):
        shared = [shared, shared]

    assert survives(shared, shared) is True


async def test_a_node_whose_cleanup_raises_does_not_replace_the_pass_s_own_failure() -> None:
    # The teardown is the last thing to run and must not change what the run is reporting.
    # A node cancelled on the way out can raise from its *own* cleanup, and awaiting that
    # would hand the caller a node's error in place of the `Fenced` it closed the run to
    # report, leaving the workflow logged as failed rather than handed back to whoever
    # holds it. The nodes behind it would not be awaited at all.
    checkpointer = MemoryCheckpointer()
    gateway = Gateway(blocked={"reserve"}, unclean={"reserve"})

    stalled = await claimed(checkpointer, "wf-cleanup")
    await checkpointer.release(stalled)
    await claimed(checkpointer, "wf-cleanup")  # the winner, which outranks the stalled pass

    with pytest.raises(Fenced):
        await run_durably(fulfilment(gateway.services()), checkpointer, stalled, ORDER)

    assert "cancelled:reserve" in gateway.calls, "and the sibling still ended with the pass"


async def test_a_node_record_cancelled_in_flight_still_lands() -> None:
    # The same hold `Run.step` keeps, for the same reason and one runner along: the node's
    # effect has already happened when the record is written, so cancelling the write does
    # not undo the parcel, it drops the record of it and the pass that takes the workflow
    # over ships again. Cancellation here is a rolling deploy rather than a crash.
    checkpointer = ParkedWrites()
    holder = await claimed(checkpointer, "wf-held")
    recording = asyncio.ensure_future(written(checkpointer, holder, "shipped", "tr-1"))
    await checkpointer.writing.wait()

    recording.cancel()
    checkpointer.proceed.set()
    with pytest.raises(asyncio.CancelledError):
        await recording

    assert await checkpointer.load("wf-held") == {"shipped": "tr-1"}, "the effect happened, so its record has to land"


async def test_a_compensated_workflow_is_not_resumed_under_its_own_id() -> None:
    # A compensation gives the forward run's steps back, and the forward checkpoint says
    # nothing about it: every node is still recorded with the id it recorded. A client
    # retrying the same idempotency key would resume that, find the work done, and ship
    # against a charge that has been refunded and stock that has been released, which is
    # the recovery path the whole design rests on turned into the worst outcome available.
    checkpointer = MemoryCheckpointer()
    gateway = Gateway(broken={"ship"})
    services = gateway.services()

    with pytest.raises(RuntimeError, match="ship is down"):
        await saga(fulfilment(services), unwinding(services), Reached.of, checkpointer, "wf-given-back", ORDER)

    assert "refund" in gateway.calls
    gateway.broken.clear()
    gateway.calls.clear()

    with pytest.raises(Unwound, match="was compensated"):
        await saga(fulfilment(services), unwinding(services), Reached.of, checkpointer, "wf-given-back", ORDER)

    assert gateway.calls == [], "and the retry performed nothing rather than shipping"
