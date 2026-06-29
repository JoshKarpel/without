import asyncio

from without import Transition
from without import collect
from without import from_fold
from without import from_map
from without import from_scan
from without import from_sink
from without import stream


async def _fetch(value: int) -> int:
    # Stand in for awaited contained I/O (a DB read, an RPC): await an already-resolved
    # result, so the step genuinely suspends-and-resumes without a timing guess.
    fetched: asyncio.Future[int] = asyncio.get_running_loop().create_future()
    fetched.set_result(value)
    return await fetched


async def test_from_scan_threads_state_and_emits_each_output() -> None:
    async def add_to_running_total(event: int, total: int) -> Transition[int, str]:
        updated = total + event
        return Transition(state=updated, output=f"total={updated}")

    running_total = from_scan(100, add_to_running_total)

    outputs = await collect(running_total(stream([3, 4, 5])))

    assert outputs == ["total=103", "total=107", "total=112"]


async def test_from_scan_awaits_contained_io_in_each_step() -> None:
    async def fetch_then_accumulate(event: int, total: int) -> Transition[int, str]:
        updated = total + await _fetch(event)
        return Transition(state=updated, output=f"total={updated}")

    running_total = from_scan(1000, fetch_then_accumulate)

    outputs = await collect(running_total(stream([3, 4, 5])))

    assert outputs == ["total=1003", "total=1007", "total=1012"]


async def test_from_map_transforms_each_event_independently() -> None:
    async def label(event: int) -> str:
        return f"value={event}"

    labeller = from_map(label)

    outputs = await collect(labeller(stream([7, 8, 9])))

    assert outputs == ["value=7", "value=8", "value=9"]


async def test_from_map_awaits_contained_io_per_event() -> None:
    async def fetch_then_double(event: int) -> int:
        return await _fetch(event) * 2

    doubler = from_map(fetch_then_double)

    outputs = await collect(doubler(stream([3, 4, 5])))

    assert outputs == [6, 8, 10]


async def test_from_fold_reduces_the_stream_to_a_final_state() -> None:
    async def accumulate(event: int, total: int) -> int:
        return total + event

    sum_into = from_fold(100, accumulate)

    assert await sum_into(stream([3, 4, 5])) == 112


async def test_from_sink_runs_each_event_for_its_effect() -> None:
    seen: list[int] = []

    async def record(event: int) -> None:
        seen.append(event)

    drain = from_sink(record)

    await drain(stream([7, 8, 9]))

    assert seen == [7, 8, 9]
