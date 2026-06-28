import asyncio
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from contextlib import suppress

import pytest
from without import background_task
from without import limit_concurrency
from without import sleep_forever


async def test_background_task_runs_during_the_block_then_cancels_on_exit() -> None:
    started = asyncio.Event()

    async def worker() -> None:
        started.set()
        await asyncio.sleep(3600)

    async with background_task(worker()) as task:
        await started.wait()
        assert not task.done()

    assert task.cancelled()


async def test_background_task_surfaces_a_worker_exception_on_exit() -> None:
    async def worker() -> None:
        raise ValueError("worker failed")

    with pytest.raises(ValueError, match="worker failed"):
        async with background_task(worker()):
            await asyncio.sleep(0)


async def test_sleep_forever_blocks_until_cancelled() -> None:
    task = asyncio.create_task(sleep_forever())
    await asyncio.sleep(0)

    assert not task.done()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_limit_concurrency_runs_every_awaitable_and_yields_its_result() -> None:
    async def work(value: int) -> int:
        await asyncio.sleep(0)
        return value * 10

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
        async for _ in limit_concurrency(source(), limit=4):
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
        async for _ in limit_concurrency((work() for _ in range(7)), limit=3):
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
