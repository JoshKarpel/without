from __future__ import annotations

from without_dag import Graph


async def test_build_produces_a_callable_from_inputs_to_output() -> None:
    graph, text = Graph.of(str)

    async def shout(value: str) -> str:
        return value.upper()

    loud = graph.node(shout, text)
    run = graph.build(output=loud, limit=2)

    assert await run("hi") == "HI"


async def test_node_threads_dependency_results_in_handle_order() -> None:
    graph, number = Graph.of(int)

    async def increment(value: int) -> int:
        return value + 1

    async def negate(value: int) -> int:
        return -value

    async def pair(first: int, second: int) -> tuple[int, int]:
        return (first, second)

    incremented = graph.node(increment, number)
    negated = graph.node(negate, number)
    combined = graph.node(pair, incremented, negated)
    run = graph.build(output=combined, limit=4)

    assert await run(5) == (6, -5)


async def test_build_seeds_each_input_for_a_multi_input_graph() -> None:
    graph, text, count = Graph.of(str, int)

    async def repeat(value: str, times: int) -> str:
        return value * times

    repeated = graph.node(repeat, text, count)
    run = graph.build(output=repeated, limit=2)

    assert await run("ab", 3) == "ababab"


async def test_a_node_with_no_dependencies_runs_as_a_source() -> None:
    graph, ignored = Graph.of(int)

    async def constant() -> str:
        return "k"

    async def tag(prefix: str, count: int) -> str:
        return f"{prefix}{count}"

    made = graph.node(constant)
    tagged = graph.node(tag, made, ignored)
    run = graph.build(output=tagged, limit=2)

    assert await run(7) == "k7"


async def test_stream_yields_each_node_result_from_typed_inputs() -> None:
    graph, number = Graph.of(int)

    async def double(value: int) -> int:
        return value * 2

    async def negate(value: int) -> int:
        return -value

    doubled = graph.node(double, number)
    negated = graph.node(negate, number)
    run = graph.build(output=doubled, limit=4)

    collected = {key: value async for key, value in run.stream(5)}

    assert collected[doubled.key] == 10
    assert collected[negated.key] == -5


async def test_build_defaults_to_unbounded_concurrency() -> None:
    graph, number = Graph.of(int)

    async def double(value: int) -> int:
        return value * 2

    doubled = graph.node(double, number)
    run = graph.build(output=doubled)

    assert run.limit is None
    assert await run(21) == 42


async def test_build_with_output_equal_to_an_input_returns_that_input() -> None:
    graph, number = Graph.of(int)
    run = graph.build(output=number, limit=1)

    assert await run(42) == 42
