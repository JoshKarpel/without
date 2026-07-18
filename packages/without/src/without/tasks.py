from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Coroutine
from collections.abc import Iterable
from contextlib import asynccontextmanager
from contextlib import suppress
from datetime import timedelta


@asynccontextmanager
async def timeout(duration: timedelta | None) -> AsyncIterator[None]:
    """
    Bound the `with` block by `duration`, or leave it unbounded when `None`.

    A `timedelta`-typed, nullable wrapper over `asyncio.timeout`: `None` disables the
    bound (an always-open context), and a duration raises `TimeoutError` if the block
    outlives it. Modelling "no limit" as `None` keeps that choice a first-class value at
    the call site, rather than a sentinel float threaded through the same parameter.
    """
    if duration is None:
        yield
        return
    async with asyncio.timeout(duration.total_seconds()):
        yield


async def sleep_forever() -> None:
    """
    Suspend the current task until it is cancelled.

    The idiom for a coroutine whose job is to stay alive until its surrounding
    scope tears it down: a server's run loop holding a bound socket open, a
    process that should idle until signalled. It awaits a future that never
    resolves, so it consumes nothing and ends only on cancellation.
    """
    await asyncio.get_running_loop().create_future()


async def cancel_futures[T](futures: Iterable[asyncio.Future[T] | None]) -> None:
    """
    Cancel every future, then await them all so their teardown completes.

    Two phases on purpose: cancelling the whole set *before* awaiting any of them
    lets them tear down concurrently, instead of serially cancelling and waiting
    for one at a time. `None` entries are skipped, so a caller holding an optional
    task (`task: asyncio.Task | None`) can pass it without a guard. The futures are
    materialized first, so a caller may pass a live set the awaits will mutate. Each
    future's own `CancelledError` is suppressed; any other exception it raises during
    teardown propagates.
    """
    present = [future for future in futures if future is not None]
    for future in present:
        future.cancel()
    for future in present:
        with suppress(asyncio.CancelledError):
            await future


@asynccontextmanager
async def background_task[T](coro: Coroutine[object, object, T]) -> AsyncIterator[asyncio.Task[T]]:
    """
    Run `coro` as a task for the duration of the `with` block.

    The task is started on entry and cancelled (then awaited) on exit, so it is
    bounded by the block and never leaks. If it finishes on its own with an
    exception, that surfaces when the block exits.
    """
    task = asyncio.create_task(coro)
    try:
        yield task
    finally:
        await cancel_futures([task])


async def as_async_iterator[T](items: AsyncIterable[T] | Iterable[T]) -> AsyncIterator[T]:
    """
    Normalize a sync or async iterable into a single async iterator.

    Lets code that consumes via `async for`/`anext` accept either kind without
    branching on the iteration protocol at every use.
    """
    if isinstance(items, AsyncIterable):
        async for item in items:
            yield item
    else:
        for item in items:
            yield item


async def limit_concurrency[T](
    aws: AsyncIterable[Awaitable[T]] | Iterable[Awaitable[T]],
    limit: int,
) -> AsyncIterator[asyncio.Future[T]]:
    """
    Run awaitables from `aws` with at most `limit` in flight, yielding each as it finishes.

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

    `limit` must be at least 1; a non-positive limit is a `ValueError`, since it
    could only ever stall the source rather than run it.

    Adapted from [Limiting concurrency in
    asyncio](https://death.andgravity.com/limit-concurrency).
    """
    if limit < 1:
        raise ValueError(f"limit must be at least 1, but got {limit}")
    source = as_async_iterator(aws)
    ended = False  # pragma: no mutate - only ever read as a bool, never identity-compared
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
        await cancel_futures(pending)
