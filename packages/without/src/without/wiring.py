# How processors connect. Two edge types, matching the two halves of the model:
# `pipe` consumes every output as the next processor's input (event edge), and
# `sample` exposes a stream's latest value as a Context (behavior edge). `pipe`
# is pure composition; `sample` is the one piece that needs something running,
# so it is the thin boundary where the imperative shell shows up.

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from without.contracts import Context, Processor, Stream
from without.tasks import background_task


def pipe[In, Out](source: Stream[In], processor: Processor[In, Out]) -> Stream[Out]:
    """Connect a source to a processor on the event edge: every value flows on.

    This is just function composition (``processor(source)``); it exists to name
    the event edge and to read left-to-right. Chaining is nesting: a downstream
    processor takes this result as its own source.
    """
    return processor(source)


@dataclass(slots=True)
class Sample[T]:
    _value: T

    def current(self) -> T:
        return self._value


@asynccontextmanager
async def sample[T](source: Stream[T]) -> AsyncIterator[Context[T]]:
    """Connect to a stream on the behavior edge: read its latest value, not each.

    The first value is sampled eagerly, so the context is never "not ready". A
    background task keeps the held value current while the ``with`` block is open,
    dropping intermediate values (latest-wins, no backpressure). The held value is
    mutated only by the drain; readers see it only through ``current``.
    """
    iterator = source.__aiter__()
    sampled = Sample(await anext(iterator))

    async def drain() -> None:
        async for value in iterator:
            sampled._value = value

    async with background_task(drain()):
        yield sampled
