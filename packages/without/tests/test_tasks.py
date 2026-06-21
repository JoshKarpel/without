import asyncio

from without import background_task


async def test_background_task_runs_during_the_block_then_cancels_on_exit() -> None:
    started = asyncio.Event()

    async def worker() -> None:
        started.set()
        await asyncio.sleep(3600)

    async with background_task(worker()) as task:
        await started.wait()
        assert not task.done()

    assert task.cancelled()
