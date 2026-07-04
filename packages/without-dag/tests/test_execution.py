from __future__ import annotations

import asyncio
import gc
import weakref
from collections.abc import Awaitable
from collections.abc import Callable
from graphlib import CycleError

import pytest
from without_dag import Node
from without_dag import execute


class Sentinel:
    """A weakref-able value, so a test can observe when it is released."""


def returning(value: object) -> Callable[[tuple[object, ...]], Awaitable[object]]:
    async def run(args: tuple[object, ...]) -> object:
        return value

    return run


async def test_execute_returns_the_targets_result() -> None:
    async def double(args: tuple[object, ...]) -> object:
        value = args[0]
        assert isinstance(value, int)
        return value * 2

    doubled = Node(key="out", dependencies=("in",), run=double)

    result = await execute([doubled], target="out", inputs={"in": 21}, limit=4)

    assert result == 42


async def test_execute_returns_an_input_target_without_running_anything() -> None:
    result = await execute([], target="entry", inputs={"entry": 7}, limit=1)

    assert result == 7


async def test_execute_runs_a_shared_ancestor_exactly_once() -> None:
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

    nodes = [
        Node("root", ("in",), root),
        Node("left", ("root",), await plus(1)),
        Node("right", ("root",), await plus(2)),
        Node("join", ("left", "right"), returning(None)),
    ]

    async def join(args: tuple[object, ...]) -> object:
        return (args[0], args[1])

    nodes[-1] = Node("join", ("left", "right"), join)

    result = await execute(nodes, target="join", inputs={"in": None}, limit=4)

    assert calls == 1
    assert result == (11, 12)


async def test_execute_skips_a_branch_the_output_does_not_need() -> None:
    ran: list[str] = []

    async def unused(args: tuple[object, ...]) -> object:
        ran.append("unused")
        return "unused"

    nodes = [
        Node("used", ("in",), returning("used")),
        Node("unused", ("in",), unused),
        Node("out", ("used",), returning("out-value")),
    ]

    result = await execute(nodes, target="out", inputs={"in": None}, limit=4)

    assert result == "out-value"
    assert ran == []


async def test_execute_releases_an_intermediate_result_after_its_consumer_runs() -> None:
    witness: list[weakref.ref[Sentinel]] = []

    async def produce(args: tuple[object, ...]) -> object:
        value = Sentinel()
        witness.append(weakref.ref(value))
        return value

    async def consume(args: tuple[object, ...]) -> object:
        assert isinstance(args[0], Sentinel)
        return "final"

    nodes = [
        Node("middle", ("in",), produce),
        Node("out", ("middle",), consume),
    ]

    result = await execute(nodes, target="out", inputs={"in": None}, limit=2)
    gc.collect()

    assert result == "final"
    assert witness[0]() is None


async def test_execute_never_exceeds_the_concurrency_limit() -> None:
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

    task = asyncio.create_task(execute([*middles, sink], target="sink", inputs={"in": None}, limit=limit))
    for _ in range(limit):
        await started.acquire()

    assert active == limit
    assert peak == limit

    release.set()
    result = await task

    assert result == "done"
    assert peak == limit


async def test_execute_runs_every_ready_node_at_once_when_limit_is_none() -> None:
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

    task = asyncio.create_task(execute([*middles, sink], target="sink", inputs={"in": None}, limit=None))
    for _ in range(width):
        await started.acquire()

    assert peak == width

    release.set()
    assert await task == "done"


async def test_execute_propagates_a_node_error_and_cancels_in_flight_siblings() -> None:
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
        await execute(nodes, target="join", inputs={}, limit=4)

    assert cancelled.is_set()


async def test_execute_raises_cycle_error_on_a_cyclic_graph() -> None:
    nodes = [
        Node("a", ("b",), returning(1)),
        Node("b", ("a",), returning(2)),
    ]

    with pytest.raises(CycleError):
        await execute(nodes, target="a", inputs={}, limit=2)


async def test_execute_raises_on_a_dangling_dependency() -> None:
    orphan = Node("out", ("ghost",), returning(1))

    with pytest.raises(KeyError, match="ghost"):
        await execute([orphan], target="out", inputs={}, limit=2)


async def test_execute_rejects_a_nonpositive_limit() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        await execute([], target="entry", inputs={"entry": 1}, limit=0)
