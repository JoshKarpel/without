from __future__ import annotations

import asyncio
import gc
import weakref
from collections.abc import Awaitable
from collections.abc import Callable
from graphlib import CycleError

import pytest
from without_dag import Node
from without_dag import Plan
from without_dag import drive
from without_dag import evaluate


class Sentinel:
    """A weakref-able value, so a test can observe when it is released."""


def returning(value: object) -> Callable[[tuple[object, ...]], Awaitable[object]]:
    async def run(args: tuple[object, ...]) -> object:
        return value

    return run


async def test_evaluate_returns_the_targets_result() -> None:
    async def double(args: tuple[object, ...]) -> object:
        value = args[0]
        assert isinstance(value, int)
        return value * 2

    doubled = Node(key="out", dependencies=("in",), run=double)

    result = await evaluate(Plan.of([doubled]), "out", inputs={"in": 21}, limit=4)

    assert result == 42


async def test_evaluate_returns_an_input_target_without_running_anything() -> None:
    result = await evaluate(Plan.of([]), "entry", inputs={"entry": 7}, limit=1)

    assert result == 7


async def test_evaluate_raises_for_a_target_that_is_neither_node_nor_input() -> None:
    with pytest.raises(KeyError, match="ghost"):
        await evaluate(Plan.of([Node("out", ("in",), returning(1))]), "ghost", inputs={"in": None}, limit=1)


async def test_evaluate_runs_every_node_even_those_the_output_ignores() -> None:
    ran: list[str] = []

    async def side(args: tuple[object, ...]) -> object:
        ran.append("side")
        return "side-value"

    nodes = [
        Node("side", ("in",), side),
        Node("out", ("in",), returning("out-value")),
    ]

    result = await evaluate(Plan.of(nodes), "out", inputs={"in": None}, limit=4)

    assert result == "out-value"
    assert ran == ["side"]


async def test_evaluate_runs_a_shared_ancestor_exactly_once() -> None:
    calls = 0

    async def root(args: tuple[object, ...]) -> object:
        nonlocal calls
        calls += 1
        return 10

    async def plus(delta: int) -> Callable[[tuple[object, ...]], Awaitable[object]]:
        async def run(args: tuple[object, ...]) -> object:
            base = args[0]
            assert isinstance(base, int)
            return base + delta

        return run

    async def join(args: tuple[object, ...]) -> object:
        return (args[0], args[1])

    nodes = [
        Node("root", ("in",), root),
        Node("left", ("root",), await plus(1)),
        Node("right", ("root",), await plus(2)),
        Node("join", ("left", "right"), join),
    ]

    result = await evaluate(Plan.of(nodes), "join", inputs={"in": None}, limit=4)

    assert calls == 1
    assert result == (11, 12)


async def test_drive_releases_an_intermediate_result_once_its_last_consumer_has_read_it() -> None:
    # A chain of three: `head`'s result must be dropped the moment `middle` (its
    # only consumer) has captured it, while the run is still in flight. Observing
    # the release *during* the run, not after `drive` returns, is what pins the
    # behavior: once the generator is exhausted its `results` dict is gone, so a
    # post-run weakref check would pass even if nothing pruned mid-run.
    witness: list[weakref.ref[Sentinel]] = []

    async def produce(args: tuple[object, ...]) -> object:
        value = Sentinel()
        witness.append(weakref.ref(value))
        return value

    nodes = [
        Node("head", ("in",), produce),
        Node("middle", ("head",), returning("middle-value")),
        Node("tail", ("middle",), returning("tail-value")),
    ]

    async for key, _ in drive(Plan.of(nodes), inputs={"in": None}, limit=4):
        if key == "tail":
            gc.collect()
            assert witness[0]() is None, "head's result was not released after middle consumed it"


async def test_evaluate_never_exceeds_the_concurrency_limit() -> None:
    limit = 2
    active = 0
    peak = 0
    release = asyncio.Event()
    started = asyncio.Semaphore(0)

    def worker(value: int) -> Callable[[tuple[object, ...]], Awaitable[object]]:
        async def run(args: tuple[object, ...]) -> object:
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            started.release()
            await release.wait()
            active -= 1
            return value

        return run

    middles = [Node(f"m{index}", ("in",), worker(index + 1)) for index in range(5)]
    sink = Node("sink", tuple(f"m{index}" for index in range(5)), returning("done"))

    task = asyncio.create_task(evaluate(Plan.of([*middles, sink]), "sink", inputs={"in": None}, limit=limit))
    for _ in range(limit):
        await started.acquire()

    assert active == limit
    assert peak == limit

    release.set()
    result = await task

    assert result == "done"
    assert peak == limit


async def test_evaluate_runs_every_ready_node_at_once_when_limit_is_none() -> None:
    width = 4
    active = 0
    peak = 0
    release = asyncio.Event()
    started = asyncio.Semaphore(0)

    def worker(value: int) -> Callable[[tuple[object, ...]], Awaitable[object]]:
        async def run(args: tuple[object, ...]) -> object:
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            started.release()
            await release.wait()
            active -= 1
            return value

        return run

    middles = [Node(f"m{index}", ("in",), worker(index + 1)) for index in range(width)]
    sink = Node("sink", tuple(f"m{index}" for index in range(width)), returning("done"))

    task = asyncio.create_task(evaluate(Plan.of([*middles, sink]), "sink", inputs={"in": None}, limit=None))
    for _ in range(width):
        await started.acquire()

    assert peak == width

    release.set()
    assert await task == "done"


async def test_evaluate_propagates_a_node_error_and_cancels_in_flight_siblings() -> None:
    cancelled = asyncio.Event()

    async def slow(args: tuple[object, ...]) -> object:
        try:
            return await asyncio.get_running_loop().create_future()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async def boom(args: tuple[object, ...]) -> object:
        raise RuntimeError("boom")

    nodes = [
        Node("slow", (), slow),
        Node("boom", (), boom),
        Node("join", ("slow", "boom"), returning("unreached")),
    ]

    with pytest.raises(RuntimeError, match="boom"):
        await evaluate(Plan.of(nodes), "join", inputs={}, limit=4)

    assert cancelled.is_set()


async def test_evaluate_cancels_in_flight_nodes_when_cancelled() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def slow(args: tuple[object, ...]) -> object:
        started.set()
        try:
            return await asyncio.get_running_loop().create_future()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    task = asyncio.create_task(evaluate(Plan.of([Node("slow", (), slow)]), "slow", inputs={}, limit=1))
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert cancelled.is_set()


async def test_evaluate_raises_cycle_error_on_a_cyclic_graph() -> None:
    nodes = [
        Node("a", ("b",), returning(1)),
        Node("b", ("a",), returning(2)),
    ]

    with pytest.raises(CycleError):
        await evaluate(Plan.of(nodes), "a", inputs={}, limit=2)


async def test_evaluate_rejects_a_nonpositive_limit() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        await evaluate(Plan.of([]), "entry", inputs={"entry": 1}, limit=0)


async def test_drive_yields_every_node_result() -> None:
    nodes = [
        Node("left", ("in",), returning("left-value")),
        Node("right", ("in",), returning("right-value")),
        Node("tip", ("left",), returning("tip-value")),
    ]

    collected = {key: value async for key, value in drive(Plan.of(nodes), inputs={"in": None}, limit=4)}

    assert collected == {"left": "left-value", "right": "right-value", "tip": "tip-value"}


async def test_drive_yields_a_dependency_before_its_consumer() -> None:
    nodes = [
        Node("root", ("in",), returning("root-value")),
        Node("leaf", ("root",), returning("leaf-value")),
    ]

    order = [key async for key, _ in drive(Plan.of(nodes), inputs={"in": None}, limit=1)]

    assert order.index("root") < order.index("leaf")


async def test_drive_raises_on_a_dangling_dependency() -> None:
    orphan = Node("out", ("ghost",), returning(1))

    with pytest.raises(KeyError, match="ghost"):
        [pair async for pair in drive(Plan.of([orphan]), inputs={}, limit=2)]


async def test_drive_skips_a_node_whose_result_is_already_supplied() -> None:
    # The resumption primitive at the seam: `inputs` names a *node*, not a source,
    # so the node does not run, is never yielded, and its supplied value is what
    # its dependents consume.
    ran: list[str] = []

    async def track(args: tuple[object, ...]) -> object:
        ran.append("head")
        return "recomputed"

    async def echo(args: tuple[object, ...]) -> object:
        return args[0]

    nodes = [
        Node("head", ("in",), track),
        Node("tail", ("head",), echo),
    ]

    collected = {key: value async for key, value in drive(Plan.of(nodes), {"in": None, "head": "restored"}, limit=2)}

    assert collected == {"tail": "restored"}
    assert ran == []


async def test_drive_keeps_filling_past_a_node_whose_result_is_supplied() -> None:
    # A supplied node must not stall the pass that spawns ready work: its siblings
    # still start at once, so resuming from a checkpoint never serializes what is
    # left to do. `supplied` sits *between* the two workers so the scheduler reaches
    # it mid-pass, with `first` already in flight and `second` not yet spawned.
    started = asyncio.Semaphore(0)
    release = asyncio.Event()

    async def work(args: tuple[object, ...]) -> object:
        started.release()
        await release.wait()
        return "worked"

    nodes = [
        Node("first", ("in",), work),
        Node("supplied", ("in",), returning("unreached")),
        Node("second", ("in",), work),
    ]

    async def collect() -> dict[str, object]:
        return {key: value async for key, value in drive(Plan.of(nodes), {"in": None, "supplied": "restored"}, None)}

    run = asyncio.create_task(collect())
    await started.acquire()
    await started.acquire()  # `second` only starts if the supplied node did not end the pass

    release.set()

    assert await run == {"first": "worked", "second": "worked"}


async def test_drive_ends_when_every_node_is_already_supplied() -> None:
    # The whole graph restored from a checkpoint: nothing runs, so no completion is
    # ever queued and the run must end on the sorter alone rather than wait for one.
    nodes = [
        Node("head", ("in",), returning("head-value")),
        Node("tail", ("head",), returning("tail-value")),
    ]
    supplied = {"in": None, "head": "restored-head", "tail": "restored-tail"}

    collected = [pair async for pair in drive(Plan.of(nodes), supplied, limit=2)]

    assert collected == []


async def test_a_plan_is_reused_across_runs_with_different_inputs() -> None:
    async def double(args: tuple[object, ...]) -> object:
        value = args[0]
        assert isinstance(value, int)
        return value * 2

    compiled = Plan.of([Node("out", ("in",), double)])

    assert await evaluate(compiled, "out", inputs={"in": 3}, limit=1) == 6
    assert await evaluate(compiled, "out", inputs={"in": 10}, limit=1) == 20
