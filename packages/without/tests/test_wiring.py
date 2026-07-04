import asyncio
from collections.abc import AsyncIterator

import pytest
from without import Transition
from without import buffer
from without import collect
from without import compose
from without import from_scan
from without import sample
from without import stream_from_iterable
from without import stream_from_queue
from without.testing import yield_once


async def test_compose_runs_first_then_second() -> None:
    async def double(event: int, _: None) -> Transition[None, int]:
        return Transition(state=None, output=event * 2)

    async def increment(event: int, _: None) -> Transition[None, int]:
        return Transition(state=None, output=event + 1)

    chained = compose(from_scan(None, double), from_scan(None, increment))

    assert await collect(chained(stream_from_iterable([6, 7, 8]))) == [13, 15, 17]


async def test_compose_adapts_the_join_type() -> None:
    async def measure(event: str, _: None) -> Transition[None, int]:
        return Transition(state=None, output=len(event))

    async def label(event: int, _: None) -> Transition[None, str]:
        return Transition(state=None, output=f"len={event}")

    chained = compose(from_scan(None, measure), from_scan(None, label))

    assert await collect(chained(stream_from_iterable(["ab", "cdef"]))) == ["len=2", "len=4"]


async def test_stream_from_queue_yields_pushed_values_in_order() -> None:
    queue: asyncio.Queue[int] = asyncio.Queue()
    for value in (5, 6, 7):
        queue.put_nowait(value)

    pushed = stream_from_queue(queue)

    received = [await anext(pushed) for _ in range(3)]

    assert received == [5, 6, 7]


async def test_buffer_yields_every_item_from_the_source() -> None:
    assert await collect(buffer(stream_from_iterable([1, 2, 3]), maxsize=2)) == [1, 2, 3]


async def test_buffer_rejects_a_nonpositive_maxsize() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        await anext(buffer(stream_from_iterable(["x"]), maxsize=0))


async def test_buffer_lets_the_producer_run_ahead_of_the_consumer() -> None:
    all_produced = asyncio.Event()

    async def source() -> AsyncIterator[str]:
        for value in ("a", "b", "c"):
            yield value
        all_produced.set()

    buffered = buffer(source(), maxsize=5)

    assert await anext(buffered) == "a"
    await all_produced.wait()

    assert [item async for item in buffered] == ["b", "c"]


async def test_buffer_surfaces_a_source_error_after_draining_buffered_items() -> None:
    async def source() -> AsyncIterator[str]:
        yield "ok"
        raise RuntimeError("boom")

    buffered = buffer(source(), maxsize=5)

    assert await anext(buffered) == "ok"
    with pytest.raises(RuntimeError, match="boom"):
        await anext(buffered)


async def test_sample_starts_at_the_first_value() -> None:
    async with sample(stream_from_iterable([11, 22, 33])) as latest:
        assert latest.current() == 11


async def test_sample_updated_returns_the_next_published_value() -> None:
    async with sample(stream_from_iterable([11, 22, 33])) as latest:
        assert await latest.updated() == 22


async def test_sample_tracks_the_latest_value() -> None:
    # The synchronous source drains in one go, so one update lands current on the last value.
    async with sample(stream_from_iterable([11, 22, 33])) as latest:
        await latest.updated()
        assert latest.current() == 33


async def test_sample_publish_skips_a_waiter_cancelled_before_it_resolves() -> None:
    # In the window between cancelling a waiter and its own deregistration, the
    # dead future is still in the set when the next publish runs: publish must skip
    # it (not crash on set_result) and still resolve the live waiter.
    queue: asyncio.Queue[int] = asyncio.Queue()
    queue.put_nowait(1)
    async with sample(stream_from_queue(queue)) as latest:
        doomed = asyncio.create_task(latest.updated())
        live = asyncio.create_task(latest.updated())
        await yield_once()  # one turn so both updated() calls register their waiters
        queue.put_nowait(2)  # queue the publish first, so the drain wakes before doomed...
        doomed.cancel()  # ...then cancel doomed: its future is now done but not yet deregistered

        assert await live == 2
        assert latest.current() == 2
        with pytest.raises(asyncio.CancelledError):
            await doomed


async def test_sample_cancels_a_pending_updated_waiter_on_exit() -> None:
    # A task still awaiting `updated` when the context closes must be cancelled,
    # not left hanging on a sample whose drain has stopped.
    queue: asyncio.Queue[int] = asyncio.Queue()
    queue.put_nowait(1)
    async with sample(stream_from_queue(queue)) as latest:
        waiting = asyncio.create_task(latest.updated())
        await yield_once()  # the task is parked awaiting the next publish

    with pytest.raises(asyncio.CancelledError):
        await waiting


async def test_updated_raises_the_source_error_to_a_pending_waiter() -> None:
    proceed = asyncio.Event()

    class SourceFailed(Exception):
        pass

    async def source() -> AsyncIterator[int]:
        yield 1
        await proceed.wait()
        raise SourceFailed("source failed")

    async def await_next_update() -> int:
        async with sample(source()) as latest:
            waiting = asyncio.create_task(latest.updated())
            await yield_once()  # the waiter registers; the drain is parked on `proceed`
            proceed.set()  # the source now raises on the next drain pull
            return await waiting  # the pending wait sees the source error, not a hang

    with pytest.raises(SourceFailed, match="source failed"):
        await await_next_update()


async def test_updated_fails_fast_after_the_source_has_already_failed() -> None:
    class SourceFailed(Exception):
        pass

    async def source() -> AsyncIterator[int]:
        yield 1
        raise SourceFailed("source failed")

    async def update_after_failure() -> int:
        async with sample(source()) as latest:
            await yield_once()  # let the drain pull again and fail, storing the error
            return await latest.updated()  # raises the stored error at once, never registering a doomed waiter

    with pytest.raises(SourceFailed, match="source failed"):
        await update_after_failure()


async def test_sample_rejects_an_empty_stream() -> None:
    with pytest.raises(ValueError, match="at least one value"):
        async with sample(stream_from_iterable([])):
            pass  # pragma: no cover - sample raises on enter, so the body never runs
