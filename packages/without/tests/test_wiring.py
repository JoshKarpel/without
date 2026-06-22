import asyncio
from dataclasses import dataclass

import pytest
from without import (
    Transition,
    broadcast,
    distribute,
    from_scan,
    merge,
    pipe,
    route,
    sample,
    stream_from_queue,
    tee,
)
from without.testing import collect, stream, tick


async def test_pipe_feeds_every_output_into_the_processor() -> None:
    async def double(event: int, _: None) -> Transition[None, int]:
        return Transition(state=None, output=event * 2)

    doubled = pipe(stream([6, 7, 8]), from_scan(None, double))

    assert await collect(doubled) == [12, 14, 16]


async def test_stream_from_queue_yields_pushed_values_in_order() -> None:
    queue: asyncio.Queue[int] = asyncio.Queue()
    for value in (5, 6, 7):
        queue.put_nowait(value)

    pushed = stream_from_queue(queue)

    received = [await anext(pushed) for _ in range(3)]

    assert received == [5, 6, 7]


async def test_sample_starts_at_the_first_value() -> None:
    async with sample(stream([11, 22, 33])) as latest:
        assert latest.current() == 11


async def test_sample_tracks_the_latest_value() -> None:
    async with sample(stream([11, 22, 33])) as latest:
        await tick()
        assert latest.current() == 33


async def test_distribute_handles_every_event_exactly_once() -> None:
    async def square(event: int, _: None) -> Transition[None, int]:
        return Transition(state=None, output=event * event)

    events = [2, 3, 4, 5, 6, 7]
    outputs = await collect(distribute(stream(events), from_scan(None, square), workers=3))

    assert sorted(outputs) == sorted(value * value for value in events)


async def test_distribute_caps_concurrency_at_the_worker_count() -> None:
    in_flight = 0
    peak = 0
    release = asyncio.Event()

    async def hold_until_released(event: int, _: None) -> Transition[None, int]:
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await release.wait()
        in_flight -= 1
        return Transition(state=None, output=event)

    async def release_once_saturated() -> None:
        while peak < 4:
            await asyncio.sleep(0)
        release.set()

    worker = from_scan(None, hold_until_released)

    outputs = await asyncio.gather(
        collect(distribute(stream(range(20)), worker, workers=4)),
        release_once_saturated(),
    )

    assert sorted(outputs[0]) == list(range(20))
    assert peak == 4


async def test_merge_folds_every_source_into_one_stream() -> None:
    merged = merge(stream([1, 2, 3]), stream([10, 20]), stream([100]))

    assert sorted(await collect(merged)) == [1, 2, 3, 10, 20, 100]


async def test_tee_gives_every_branch_every_value_in_order() -> None:
    async with tee(stream([1, 2, 3]), branches=3) as branches:
        drained = await asyncio.gather(*(collect(branch) for branch in branches))

    assert drained == [[1, 2, 3], [1, 2, 3], [1, 2, 3]]


async def test_tee_buffer_lets_a_fast_branch_run_ahead_of_a_slow_one() -> None:
    async with tee(stream([1, 2, 3, 4]), branches=2, buffer=4) as (fast, slow):
        ahead = await collect(fast)
        behind = await collect(slow)

    assert ahead == [1, 2, 3, 4]
    assert behind == [1, 2, 3, 4]


async def test_broadcast_feeds_every_event_to_every_processor() -> None:
    async def double(event: int, _: None) -> Transition[None, str]:
        return Transition(state=None, output=f"double={event * 2}")

    async def negate(event: int, _: None) -> Transition[None, str]:
        return Transition(state=None, output=f"negate={-event}")

    outputs = await collect(broadcast(stream([5, 6]), from_scan(None, double), from_scan(None, negate)))

    assert sorted(outputs) == ["double=10", "double=12", "negate=-5", "negate=-6"]


@dataclass(frozen=True, slots=True)
class Response:
    body: str


@dataclass(frozen=True, slots=True)
class KickOff:
    job: str


async def test_route_sends_each_value_to_the_branch_for_its_type() -> None:
    events: list[Response | KickOff] = [
        Response("ok"),
        KickOff("reindex"),
        Response("created"),
    ]

    async with route(stream(events), Response, KickOff) as (responses, kickoffs):
        drained = await asyncio.gather(collect(responses), collect(kickoffs))

    assert drained == [[Response("ok"), Response("created")], [KickOff("reindex")]]


async def test_route_raises_on_a_value_matching_no_listed_type() -> None:
    with pytest.raises(TypeError):
        async with route(stream([Response("ok"), KickOff("nope")]), Response) as (responses,):
            await collect(responses)
