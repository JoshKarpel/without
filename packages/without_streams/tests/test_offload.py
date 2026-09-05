from __future__ import annotations

from collections.abc import Iterator

import pytest
from without_streams import offload
from without_streams import stream_from_iterable


async def test_offload_delivers_every_item_to_the_worker_in_order() -> None:
    received: list[int] = []

    def work(batches: Iterator[list[int]]) -> None:
        for batch in batches:
            received.extend(batch)

    async with offload(work) as sink:
        await sink(stream_from_iterable([7, 3, 9, 5]))

    assert received == [7, 3, 9, 5]


async def test_offload_delivers_a_lone_item_as_a_single_element_burst() -> None:
    # One item means the worker's `get()` empties the queue, so the burst-drain's `range(qsize())`
    # is `range(0)`: the deterministic "nothing more queued" path.
    received: list[list[int]] = []

    def work(batches: Iterator[list[int]]) -> None:
        received.extend(batches)

    async with offload(work) as sink:
        await sink(stream_from_iterable([42]))

    assert received == [[42]]


async def test_offload_surfaces_a_worker_failure_when_the_block_exits() -> None:
    def work(batches: Iterator[list[int]]) -> None:
        raise RuntimeError("worker could not open its resource")

    with pytest.raises(RuntimeError, match="worker could not open its resource"):
        async with offload(work) as sink:
            await sink(stream_from_iterable([1]))
