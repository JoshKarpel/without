import asyncio
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from contextlib import suppress

import pytest
from without import as_async_iterator
from without import background_task
from without import cancel_futures
from without import limit_concurrency
from without import sleep_forever
from without.testing import resolved_next_turn
from without.testing import yield_once


async def test_background_task_runs_during_the_block_then_cancels_on_exit() -> None:
    started = asyncio.Event()

    async def worker() -> None:
        started.set()
        await sleep_forever()

    async with background_task(worker()) as task:
        await started.wait()
        assert not task.done()

    assert task.cancelled()


async def test_background_task_surfaces_a_worker_exception_on_exit() -> None:
    failing = asyncio.Event()

    async def worker() -> None:
        failing.set()
        raise ValueError("worker failed")

    with pytest.raises(ValueError, match="worker failed"):
        async with background_task(worker()):
            await failing.wait()  # the worker has run to its failure; exit must surface it


async def test_sleep_forever_blocks_until_cancelled() -> None:
    task = asyncio.create_task(sleep_forever())
    await yield_once()  # one turn so the task reaches its await; sleep_forever can never finish there

    assert not task.done()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_cancel_futures_cancels_then_awaits_every_future() -> None:
    torn_down = 0
    parked = asyncio.Semaphore(0)

    async def worker() -> None:
        nonlocal torn_down
        try:
            parked.release()
            await sleep_forever()
        finally:
            torn_down += 1

    tasks = [asyncio.create_task(worker()) for _ in range(4)]
    for _ in tasks:
        await parked.acquire()  # every worker is inside its try, parked on sleep_forever

    await cancel_futures(tasks)

    assert all(task.cancelled() for task in tasks)
    assert torn_down == 4


async def test_cancel_futures_skips_none_entries() -> None:
    torn_down = 0
    parked = asyncio.Event()

    async def worker() -> None:
        nonlocal torn_down
        try:
            parked.set()
            await sleep_forever()
        finally:
            torn_down += 1

    task = asyncio.create_task(worker())
    await parked.wait()  # the worker is inside its try, parked on sleep_forever

    await cancel_futures([None, task, None])

    assert task.cancelled()
    assert torn_down == 1


async def test_cancel_futures_propagates_a_non_cancellation_teardown_error() -> None:
    parked = asyncio.Event()

    async def worker() -> None:
        try:
            parked.set()
            await sleep_forever()
        except asyncio.CancelledError:
            raise ValueError("teardown failed") from None

    task = asyncio.create_task(worker())
    await parked.wait()  # the worker is parked on sleep_forever, so cancellation hits there

    with pytest.raises(ValueError, match="teardown failed"):
        await cancel_futures([task])


async def test_as_async_iterator_wraps_a_sync_iterable() -> None:
    collected = [value async for value in as_async_iterator([3, 1, 4, 1, 5])]

    assert collected == [3, 1, 4, 1, 5]


async def test_as_async_iterator_passes_through_an_async_iterable() -> None:
    async def counts() -> AsyncIterator[int]:
        for value in (9, 8, 7):
            yield value

    collected = [value async for value in as_async_iterator(counts())]

    assert collected == [9, 8, 7]


async def test_limit_concurrency_runs_every_awaitable_and_yields_its_result() -> None:
    async def work(value: int) -> int:
        return (await resolved_next_turn(value)) * 10

    results = [future.result() async for future in limit_concurrency((work(n) for n in range(1, 6)), limit=2)]

    assert sorted(results) == [10, 20, 30, 40, 50]


async def test_limit_concurrency_keeps_at_most_limit_in_flight() -> None:
    active = 0
    peak = 0

    async def work() -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1

    async for _ in limit_concurrency((work() for _ in range(9)), limit=3):
        pass

    assert peak == 3


async def test_limit_concurrency_pulls_the_source_lazily_only_below_the_limit() -> None:
    pulled = 0
    blocked = asyncio.Event()

    async def wait_blocked() -> None:
        await blocked.wait()

    async def source() -> AsyncIterator[Awaitable[None]]:
        nonlocal pulled
        while True:
            pulled += 1
            yield wait_blocked()

    async def consume() -> None:
        # The in-flight items block forever, so the loop never yields a value and
        # never exits normally; consume is cancelled instead.
        async for _ in limit_concurrency(source(), limit=4):  # pragma: no cover
            pass

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.05)
    try:
        # The four in-flight items never complete, so the source is never advanced
        # past the limit: it was pulled exactly four times, not a fifth.
        assert pulled == 4
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def test_limit_concurrency_cancels_in_flight_awaitables_on_early_exit() -> None:
    cancelled = 0

    async def work() -> None:
        nonlocal cancelled
        try:
            await sleep_forever()
        except asyncio.CancelledError:
            cancelled += 1
            raise

    async def consume() -> None:
        # The in-flight work blocks forever, so the loop never yields a value and
        # never exits normally; consume is cancelled instead.
        async for _ in limit_concurrency((work() for _ in range(7)), limit=3):  # pragma: no cover
            pass

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.05)
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

    assert cancelled == 3


async def test_limit_concurrency_surfaces_an_awaitable_failure_through_the_future() -> None:
    async def boom() -> None:
        raise ValueError("work failed")

    async for future in limit_concurrency([boom()], limit=2):
        with pytest.raises(ValueError, match="work failed"):
            future.result()


@pytest.mark.parametrize("limit", [0, -3])
async def test_limit_concurrency_rejects_a_non_positive_limit(limit: int) -> None:
    empty: list[Awaitable[int]] = []
    with pytest.raises(ValueError, match="limit must be at least 1"):
        async for _ in limit_concurrency(empty, limit=limit):
            pass  # pragma: no cover - limit_concurrency raises before the first iteration
