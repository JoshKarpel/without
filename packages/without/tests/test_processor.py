import asyncio

from without import Transition, from_reducer
from without.testing import collect, stream


async def test_from_reducer_threads_state_and_emits_each_output() -> None:
    async def add_to_running_total(event: int, total: int) -> Transition[int, str]:
        updated = total + event
        return Transition(state=updated, outputs=(f"total={updated}",))

    running_total = from_reducer(100, add_to_running_total)

    outputs = await collect(running_total(stream([3, 4, 5])))

    assert outputs == ["total=103", "total=107", "total=112"]


async def test_from_reducer_can_emit_zero_or_many_outputs_per_event() -> None:
    async def repeat_positive(event: int, _: None) -> Transition[None, int]:
        return Transition(state=None, outputs=(event,) * event if event > 0 else ())

    repeater = from_reducer(None, repeat_positive)

    outputs = await collect(repeater(stream([2, -1, 3])))

    assert outputs == [2, 2, 3, 3, 3]


async def test_from_reducer_awaits_contained_io_in_each_step() -> None:
    async def fetch_then_accumulate(event: int, total: int) -> Transition[int, str]:
        await asyncio.sleep(0)
        updated = total + event
        return Transition(state=updated, outputs=(f"total={updated}",))

    running_total = from_reducer(1000, fetch_then_accumulate)

    outputs = await collect(running_total(stream([3, 4, 5])))

    assert outputs == ["total=1003", "total=1007", "total=1012"]
