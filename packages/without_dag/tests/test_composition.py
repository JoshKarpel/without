from __future__ import annotations

from without_dag import Graph
from without_streams import collect
from without_streams import from_map
from without_streams import stream_from_iterable


async def test_a_compiled_graph_lifts_into_a_processor_run_once_per_event() -> None:
    graph, (number,) = Graph.of(int)

    async def square(value: int) -> int:
        return value * value

    squared = graph.node("squared", square, number)
    run = graph.build(output=squared, limit=2)

    processor = from_map(run)
    results = await collect(processor(stream_from_iterable([2, 3, 4])))

    assert results == [4, 9, 16]
