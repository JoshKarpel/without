# Run a coroutine as a task scoped to a `with` block: started on entry,
# cancelled on exit, so it cannot outlive the block. The behavior-edge `sample`
# connector uses this, and stream sources and servers will too.

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Coroutine
from collections.abc import Iterable
from contextlib import asynccontextmanager
from contextlib import suppress


async def sleep_forever() -> None:
    """Suspend the current task until it is cancelled.

    The idiom for a coroutine whose job is to stay alive until its surrounding
    scope tears it down: a server's run loop holding a bound socket open, a
    process that should idle until signalled. It awaits a future that never
    resolves, so it consumes nothing and ends only on cancellation.
    """
    await asyncio.get_running_loop().create_future()


@asynccontextmanager
async def background_task(coro: Coroutine[object, object, object]) -> AsyncIterator[asyncio.Task[object]]:
    """Run `coro` as a task for the duration of the `with` block.

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


async def _as_async[T](aws: AsyncIterable[T] | Iterable[T]) -> AsyncIterator[T]:
    if isinstance(aws, AsyncIterable):
        async for item in aws:
            yield item
    else:
        for item in aws:
            yield item


async def limit_concurrency[T](
    aws: AsyncIterable[Awaitable[T]] | Iterable[Awaitable[T]],
    limit: int,
) -> AsyncIterator[asyncio.Future[T]]:
    """Run awaitables from `aws` with at most `limit` in flight, yielding each as it finishes.

    A bounded-concurrency driver: it pulls the next awaitable from `aws` only
    while fewer than `limit` are already running. So a *lazy* source (an async
    generator that produces each unit of work on demand) is never advanced past
    the limit. That is what lets it gate a side-effecting source: an accept loop
    whose generator awaits `socket.accept()` only when pulled will never accept
    more connections than it can serve.

    Each completed awaitable is yielded as a `Future`; call `.result()` on it to
    read the value or re-raise its exception. On early exit or cancellation, any
    still-running awaitables are cancelled and awaited, so none outlive the
    iteration.

    Adapted from [Limiting concurrency in
    asyncio](https://death.andgravity.com/limit-concurrency).
    """
    source = _as_async(aws)
    ended = False
    pending: set[asyncio.Future[T]] = set()
    try:
        while pending or not ended:
            while len(pending) < limit and not ended:
                try:
                    aw = await anext(source)
                except StopAsyncIteration:
                    ended = True
                else:
                    pending.add(asyncio.ensure_future(aw))
            if not pending:
                return
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            while done:
                yield done.pop()
    finally:
        for task in pending:
            task.cancel()
        for task in pending:
            with suppress(asyncio.CancelledError):
                await task
