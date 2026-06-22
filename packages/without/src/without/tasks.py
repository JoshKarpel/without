# Run a coroutine as a task scoped to a `with` block: started on entry,
# cancelled on exit, so it cannot outlive the block. The behavior-edge `sample`
# connector uses this, and stream sources and servers will too.

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Coroutine
from contextlib import asynccontextmanager, suppress


@asynccontextmanager
async def background_task(coro: Coroutine[object, object, object]) -> AsyncIterator[asyncio.Task[object]]:
    """Run ``coro`` as a task for the duration of the ``with`` block.

    The task is started on entry and cancelled (then awaited) on exit, so it is
    bounded by the block and never leaks. If it finishes on its own with an
    exception, that surfaces when the block exits.
    """
    task = asyncio.create_task(coro)
    try:
        yield task
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
