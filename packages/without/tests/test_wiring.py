import asyncio

import pytest
from without import Transition
from without import collect
from without import compose
from without import from_scan
from without import sample
from without import stream
from without import stream_from_queue
from without.testing import yield_once


async def test_compose_runs_first_then_second() -> None:
    async def double(event: int, _: None) -> Transition[None, int]:
        return Transition(state=None, output=event * 2)

    async def increment(event: int, _: None) -> Transition[None, int]:
        return Transition(state=None, output=event + 1)

    chained = compose(from_scan(None, double), from_scan(None, increment))

    assert await collect(chained(stream([6, 7, 8]))) == [13, 15, 17]


async def test_compose_adapts_the_join_type() -> None:
    async def measure(event: str, _: None) -> Transition[None, int]:
        return Transition(state=None, output=len(event))

    async def label(event: int, _: None) -> Transition[None, str]:
        return Transition(state=None, output=f"len={event}")

    chained = compose(from_scan(None, measure), from_scan(None, label))

    assert await collect(chained(stream(["ab", "cdef"]))) == ["len=2", "len=4"]


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


async def test_sample_updated_returns_the_next_published_value() -> None:
    async with sample(stream([11, 22, 33])) as latest:
        assert await latest.updated() == 22


async def test_sample_tracks_the_latest_value() -> None:
    # The synchronous source drains in one go, so one update lands current on the last value.
    async with sample(stream([11, 22, 33])) as latest:
        await latest.updated()
        assert latest.current() == 33


async def test_sample_publish_skips_a_waiter_cancelled_before_it_resolves() -> None:
    # Cancelling a task awaiting `updated` cancels its future in place; the next
    # publish must skip that dead waiter rather than crash on it, and still
    # resolve a live one.
    queue: asyncio.Queue[int] = asyncio.Queue()
    queue.put_nowait(1)
    async with sample(stream_from_queue(queue)) as latest:
        doomed = asyncio.create_task(latest.updated())
        live = asyncio.create_task(latest.updated())
        await yield_once()  # one turn so both updated() calls register their waiters
        doomed.cancel()
        with pytest.raises(asyncio.CancelledError):
            await doomed

        queue.put_nowait(2)

        assert await live == 2
        assert latest.current() == 2


async def test_sample_rejects_an_empty_stream() -> None:
    with pytest.raises(ValueError, match="at least one value"):
        async with sample(stream([])):
            pass  # pragma: no cover - sample raises on enter, so the body never runs
