# Helpers for testing processors without real I/O: turn a plain iterable into a
# Stream, and drain a Stream back into a list to assert on.

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable

from without.contracts import Stream


async def stream[T](values: Iterable[T]) -> AsyncIterator[T]:
    """An in-memory `Stream` of fixed values, to feed a processor under test."""
    for value in values:
        yield value


async def tick() -> None:
    """Let the event loop run ready tasks one step, e.g. to nudge a `sample` drain.

    TODO: this advances the loop only a single step, so it relies on the source
    under test draining in one activation. Replace callers with an explicit
    "await next update" signal on the sampled context so tests assert on
    post-update state deterministically instead of by yielding once.
    """
    await asyncio.sleep(0)


async def collect[T](source: Stream[T]) -> list[T]:
    """Drain a `Stream` into a list, to assert on what a processor emitted."""
    return [value async for value in source]
