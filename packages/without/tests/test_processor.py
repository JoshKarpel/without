import asyncio

from without import Transition, from_mapper, from_reducer
from without.testing import collect, stream


async def test_from_reducer_threads_state_and_emits_each_output() -> None:
    async def add_to_running_total(event: int, total: int) -> Transition[int, str]:
        updated = total + event
        return Transition(state=updated, output=f"total={updated}")

    running_total = from_reducer(100, add_to_running_total)

    outputs = await collect(running_total(stream([3, 4, 5])))

    assert outputs == ["total=103", "total=107", "total=112"]


async def test_from_reducer_awaits_contained_io_in_each_step() -> None:
    async def fetch_then_accumulate(event: int, total: int) -> Transition[int, str]:
        await asyncio.sleep(0)
        updated = total + event
        return Transition(state=updated, output=f"total={updated}")

    running_total = from_reducer(1000, fetch_then_accumulate)

    outputs = await collect(running_total(stream([3, 4, 5])))

    assert outputs == ["total=1003", "total=1007", "total=1012"]


async def test_from_mapper_transforms_each_event_independently() -> None:
    async def label(event: int) -> str:
        return f"value={event}"

    labeller = from_mapper(label)

    outputs = await collect(labeller(stream([7, 8, 9])))

    assert outputs == ["value=7", "value=8", "value=9"]


async def test_from_mapper_awaits_contained_io_per_event() -> None:
    async def fetch_then_double(event: int) -> int:
        await asyncio.sleep(0)
        return event * 2

    doubler = from_mapper(fetch_then_double)

    outputs = await collect(doubler(stream([3, 4, 5])))

    assert outputs == [6, 8, 10]
