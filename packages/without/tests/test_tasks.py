import asyncio

import pytest
from without import background_task
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
