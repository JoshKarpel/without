from __future__ import annotations

import asyncio
import queue
from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast

from without.contracts import Sink
from without.contracts import Stream

# Sentinel pushed onto the queue to end the worker's blocking iterator. A distinct
# object (not None or any real item) so it can never collide with a logged value.
_DONE = object()


@asynccontextmanager
async def offload[T](work: Callable[[Iterator[T]], None]) -> AsyncIterator[Sink[T]]:
    """
    Run a blocking `work` on a dedicated thread, fed by the yielded async `Sink`.

    The point is efficient blocking I/O under heavy logging. An async file library
    (`aiofiles`, `anyio`) hops to a worker thread *per operation*, so a busy log
    stream pays that round-trip on every write. Here a single long-lived thread
    owns the resource and does *all* the I/O: `work` is plain blocking Python that
    consumes an `Iterator[T]` (open a file, loop writing, close), and the yielded
    `Sink` just drops each item onto a thread-safe queue the worker drains. No
    per-item thread hop, and the "processor body" stays ordinary synchronous code.

    Lifecycle is bounded by the `with` block: the thread starts on entry, and on
    exit a sentinel ends the worker's iterator so it flushes and releases its
    resource, then the thread is joined (so a file is closed before the block
    returns). If `work` raises, that surfaces when the block exits.

    Nest this *outside* the consumer that drives the sink, so the worker outlives
    the draining. With `capture`, that means `async with offload(...) as writer:`
    around `async with capture(..., writer):`, not the other way round.

    The queue is unbounded in this first cut: the async side never blocks or
    drops, at the cost of growing memory if the worker cannot keep up with a
    sustained burst (a stalled disk). A bounded, drop-counting variant is a
    deliberate follow-up. The bidirectional case (a full `Processor` bridged onto
    a thread, with an output queue as well) is intentionally out of scope; this
    covers the terminal-sink need (writing) without that extra state.
    """
    items: queue.Queue[T | object] = queue.Queue()

    def drain() -> Iterator[T]:
        while True:
            item = items.get()
            if item is _DONE:
                return
            yield cast("T", item)

    worker = asyncio.create_task(asyncio.to_thread(work, drain()))

    async def sink(inputs: Stream[T]) -> None:
        async for item in inputs:
            items.put_nowait(item)

    try:
        yield sink
    finally:
        items.put_nowait(_DONE)
        await worker


def write_lines(path: Path) -> Callable[[Iterator[str]], None]:
    """
    A blocking worker (for `offload`) that appends each string as a newline-delimited line.

    It writes *strings*, not records: rendering a `Record` to text (a JSON line,
    logfmt, a console format) is the app's encoding boundary, so it belongs in a
    `from_map(Record -> str)` composed in front of this writer, not baked into it.
    That keeps the writer itself encoding-agnostic. Opens `path` in append mode,
    writes each incoming string followed by a newline (the line framing this owns,
    so a render upstream should not add its own), and closes (flushing) when the
    stream ends. Rotation and flush-frequency are deliberately *not* bundled in:
    those are separate concerns to compose as their own workers, not decisions
    baked into the writer.
    """

    def work(lines: Iterator[str]) -> None:
        with path.open("a", encoding="utf-8") as file:
            for line in lines:
                file.write(line)
                file.write("\n")

    return work
