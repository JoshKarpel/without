from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from without_dag import Graph


async def double(value: int) -> int:
    return value * 2


async def never_runs(value: int) -> int:  # pragma: no cover - the point of the tests that wire it in
    raise AssertionError(f"a node whose result the checkpoint supplies was run, with {value!r}")


async def sink_into(checkpoint: dict[str, object], results: AsyncIterator[tuple[str, object]]) -> None:
    """Record each completion as it lands, the way a durable sink would."""
    async for key, value in results:
        checkpoint[key] = value  # noqa: PERF403 - a comprehension discards the partial mapping a failed run leaves, which is what resumption needs


async def test_of_opens_a_graph_with_zero_inputs() -> None:
    graph, () = Graph.of()

    async def constant() -> str:
        return "k"

    made = graph.node("made", constant)
    run = graph.build(output=made)

    assert await run() == "k"


async def test_build_produces_a_callable_from_inputs_to_output() -> None:
    graph, (text,) = Graph.of(str)

    async def shout(value: str) -> str:
        return value.upper()

    loud = graph.node("loud", shout, text)
    run = graph.build(output=loud, limit=2)

    assert await run("hi") == "HI"


async def test_node_threads_dependency_results_in_handle_order() -> None:
    graph, (number,) = Graph.of(int)

    async def increment(value: int) -> int:
        return value + 1

    async def negate(value: int) -> int:
        return -value

    async def pair(first: int, second: int) -> tuple[int, int]:
        return (first, second)

    incremented = graph.node("incremented", increment, number)
    negated = graph.node("negated", negate, number)
    combined = graph.node("combined", pair, incremented, negated)
    run = graph.build(output=combined, limit=4)

    assert await run(5) == (6, -5)


async def test_build_seeds_each_input_for_a_multi_input_graph() -> None:
    graph, (text, count) = Graph.of(str, int)

    async def repeat(value: str, times: int) -> str:
        return value * times

    repeated = graph.node("repeated", repeat, text, count)
    run = graph.build(output=repeated, limit=2)

    assert await run("ab", 3) == "ababab"


async def test_a_node_with_no_dependencies_runs_as_a_source() -> None:
    graph, (ignored,) = Graph.of(int)

    async def constant() -> str:
        return "k"

    async def tag(prefix: str, count: int) -> str:
        return f"{prefix}{count}"

    made = graph.node("made", constant)
    tagged = graph.node("tagged", tag, made, ignored)
    run = graph.build(output=tagged, limit=2)

    assert await run(7) == "k7"


async def test_stream_yields_each_node_result_under_the_key_it_was_given() -> None:
    graph, (number,) = Graph.of(int)

    async def negate(value: int) -> int:
        return -value

    doubled = graph.node("doubled", double, number)
    negated = graph.node("negated", negate, number)
    run = graph.build(output=doubled, limit=4)

    collected = {key: value async for key, value in run.stream(5)}

    assert collected == {"doubled": 10, "negated": -5}
    assert collected[doubled.key] == 10
    assert collected[negated.key] == -5


async def test_build_defaults_to_unbounded_concurrency() -> None:
    graph, (number,) = Graph.of(int)

    doubled = graph.node("doubled", double, number)
    run = graph.build(output=doubled)

    assert run.limit is None
    assert await run(21) == 42


async def test_build_with_output_equal_to_an_input_returns_that_input() -> None:
    graph, (number,) = Graph.of(int)
    run = graph.build(output=number, limit=1)

    assert await run(42) == 42


async def test_build_rejects_a_limit_below_one() -> None:
    graph, (number,) = Graph.of(int)

    with pytest.raises(ValueError, match="limit must be at least 1 or None"):
        graph.build(output=number, limit=0)


async def test_node_rejects_a_key_another_node_already_took() -> None:
    graph, (number,) = Graph.of(int)

    graph.node("doubled", double, number)

    with pytest.raises(ValueError, match="'doubled' is already the key of a node"):
        graph.node("doubled", double, number)


async def test_node_rejects_a_key_belonging_to_an_input() -> None:
    graph, (_text, number) = Graph.of(str, int)

    with pytest.raises(ValueError, match="'input:1' is the key of one of this graph's inputs"):
        graph.node(number.key, double, number)


async def test_a_checkpointed_node_is_not_run_and_its_result_feeds_its_dependents() -> None:
    graph, (number,) = Graph.of(int)

    async def increment(value: int) -> int:
        return value + 1

    doubled = graph.node("doubled", never_runs, number)
    incremented = graph.node("incremented", increment, doubled)
    run = graph.build(output=incremented, limit=2)

    assert await run(5, checkpoint={"doubled": 100}) == 101


async def test_a_checkpointed_output_is_returned_without_being_recomputed() -> None:
    graph, (number,) = Graph.of(int)

    doubled = graph.node("doubled", never_runs, number)
    run = graph.build(output=doubled, limit=1)

    assert await run(5, checkpoint={"doubled": 99}) == 99


async def test_a_resumed_run_streams_only_the_nodes_it_computed() -> None:
    graph, (number,) = Graph.of(int)

    async def increment(value: int) -> int:
        return value + 1

    doubled = graph.node("doubled", double, number)
    incremented = graph.node("incremented", increment, doubled)
    run = graph.build(output=incremented, limit=2)

    collected: dict[str, object] = {}
    await sink_into(collected, run.stream(5, checkpoint={"doubled": 100}))

    assert collected == {"incremented": 101}


async def test_a_checkpoint_collected_from_a_failed_run_resumes_the_work_left() -> None:
    # The durable-workflow round trip in miniature: sink `stream`'s pairs as they
    # land, fail partway, then hand the accumulated mapping back. Only the step
    # that did not finish runs again, so its predecessor's effects happen once.
    graph, (number,) = Graph.of(int)
    ran: list[str] = []
    healed = False

    async def increment(value: int) -> int:
        ran.append("incremented")
        return value + 1

    async def double_once_healed(value: int) -> int:
        ran.append("doubled")
        if not healed:
            raise RuntimeError("boom")
        return value * 2

    incremented = graph.node("incremented", increment, number)
    doubled = graph.node("doubled", double_once_healed, incremented)
    run = graph.build(output=doubled, limit=2)

    checkpoint: dict[str, object] = {}
    with pytest.raises(RuntimeError, match="boom"):
        await sink_into(checkpoint, run.stream(1))

    assert checkpoint == {"incremented": 2}
    assert ran == ["incremented", "doubled"]

    healed = True
    ran.clear()

    assert await run(1, checkpoint=checkpoint) == 4
    assert ran == ["doubled"]


async def test_a_checkpoint_key_naming_no_node_is_rejected() -> None:
    graph, (number,) = Graph.of(int)

    doubled = graph.node("doubled", double, number)
    run = graph.build(output=doubled, limit=1)

    with pytest.raises(KeyError, match=r"\['renamed'\] name no node"):
        await run(5, checkpoint={"doubled": 10, "renamed": 20})


async def test_a_checkpoint_key_naming_an_input_is_rejected() -> None:
    graph, (number,) = Graph.of(int)

    doubled = graph.node("doubled", double, number)
    run = graph.build(output=doubled, limit=1)

    with pytest.raises(KeyError, match=r"\['input:0'\] name no node"):
        [pair async for pair in run.stream(5, checkpoint={number.key: 3})]
