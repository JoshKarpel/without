from __future__ import annotations

import asyncio
import os
import queue
from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from datetime import time
from datetime import timedelta
from datetime import tzinfo
from pathlib import Path
from typing import TextIO

from without.contracts import Sink
from without.contracts import Stream


@asynccontextmanager
async def offload[T](work: Callable[[Iterator[list[T]]], None]) -> AsyncIterator[Sink[T]]:
    """
    Run a blocking `work` on a dedicated thread, fed by the yielded async `Sink`.

    The point is efficient blocking I/O under heavy logging. An async file library
    (`aiofiles`, `anyio`) hops to a worker thread *per operation*, so a busy log
    stream pays that round-trip on every write. Here a single long-lived thread
    owns the resource and does *all* the I/O: `work` is plain blocking Python that
    consumes the items, and the yielded `Sink` just drops each onto a thread-safe
    queue the worker drains. No per-item thread hop, and the "processor body" stays
    ordinary synchronous code.

    Items arrive in *bursts*: each element of the iterator is everything available
    on the queue at that instant (at least one item, blocking for the first). A
    burst boundary is therefore exactly the moment the worker has caught up, which
    is where a writer flushes: under load bursts are large and flushes are few, and
    when idle each burst is a single item flushed at once. So durability needs no
    flush-frequency knob; it falls out of the queue's own backlog.

    Lifecycle is bounded by the `with` block: the thread starts on entry, and on
    exit the queue is shut down so the worker drains what remains and then ends,
    and the thread is joined (so a file is closed before the block returns). If
    `work` raises, that surfaces when the block exits.

    Nest this *outside* the consumer that drives the sink, so the worker outlives
    the draining. With `capture`, that means `async with offload(...) as writer:`
    around `async with capture(..., writer):`, not the other way round.

    The queue is unbounded in this first cut: the async side never blocks or drops,
    at the cost of growing memory if the worker cannot keep up with a sustained
    burst (a stalled disk). A bounded, drop-counting variant is a deliberate
    follow-up. The bidirectional case (a full `Processor` bridged onto a thread,
    with an output queue as well) is intentionally out of scope; this covers the
    terminal-sink need (writing) without that extra state.
    """
    items: queue.Queue[T] = queue.Queue()

    def drain() -> Iterator[list[T]]:
        while True:
            try:
                batch = [items.get()]
            except queue.ShutDown:
                return
            # Take everything queued at this instant into the burst. No Empty to catch and no
            # get_nowait-in-a-try for control flow: the worker is the queue's *only* consumer, so
            # qsize() is an exact count of items still present to take (a producer may add more,
            # which simply waits for the next burst; nobody else removes), and each get succeeds.
            batch.extend(items.get_nowait() for _ in range(items.qsize()))
            yield batch

    worker = asyncio.create_task(asyncio.to_thread(work, drain()))

    async def sink(inputs: Stream[T]) -> None:
        async for item in inputs:
            items.put_nowait(item)

    try:
        yield sink
    finally:
        items.shutdown()
        await worker


def now_utc() -> datetime:
    return datetime.now(UTC)


def at_times(*times: time, tz: tzinfo = UTC) -> Callable[[datetime], datetime]:
    """
    A `schedule` for `to_rotating_file`: the next occurrence of any of these wall-clock times.

    Given a moment, returns the soonest of `times` strictly after it, interpreted in
    timezone `tz`. So `at_times(time(0, 0))` rotates daily at midnight `tz`, and
    `at_times(time(0, 0), time(12, 0))` twice a day. The list of times is the
    recurring cycle of rotation boundaries, resolved to a concrete datetime against
    the *actual* current time each call, so nothing goes stale and a boundary missed
    while idle collapses to a single rotation. Strictly-after (not at-or-after) is
    what lets the writer advance past a boundary it just hit without looping on it.
    """
    if not times:
        raise ValueError("at_times requires at least one time-of-day")
    ordered = sorted(times)

    def schedule(after: datetime) -> datetime:
        local = after.astimezone(tz)
        for moment in ordered:
            candidate = datetime.combine(local.date(), moment, tzinfo=tz)
            if candidate > local:
                return candidate
        return datetime.combine(local.date() + timedelta(days=1), ordered[0], tzinfo=tz)

    return schedule


def to_rotating_file(
    name: Callable[[int, datetime], Path],
    *,
    max_bytes: int | None = None,
    max_age: timedelta | None = None,
    schedule: Callable[[datetime], datetime] | None = None,
    now: Callable[[], datetime] = now_utc,
) -> Callable[[Iterator[list[str]]], None]:
    """
    A blocking worker (for `offload`) that appends lines to a file, rotating by size and/or time.

    Rotation lives in the worker deliberately: it is the one place with all the
    information to decide, timed with its own writes. Size is a function of the
    bytes written, so a size limit cannot be an independent, decoupled trigger; the
    worker keeps an exact byte count (writing UTF-8 bytes directly) rather than
    polling a lagging on-disk size, and reads the clock the same way. So `max_bytes`
    (size), `max_age` (elapsed since the file opened, a *relative* interval),
    `schedule` (the next *absolute* wall-clock boundary, e.g. midnight, via a
    next-boundary function such as `at_times`), and any combination (rotate on
    whichever trips first) all work here, which the stdlib's separate size and time
    handlers cannot do at once. With no limit at all the file never rotates, but
    that is a footgun (an unbounded file), so it is not the affordance the name
    suggests.

    `schedule(t)` returns the next rotation boundary strictly after `t`; the writer
    samples it at each open and rotates once `now()` reaches it (missed boundaries
    collapse to one rotation). It is the pure, thread-synchronous form of "a stream
    of rotation datetimes": the worker already owns the clock, so it generates the
    stream itself rather than sampling an async one.

    `name` turns a rotation index and the rotation time into a path
    (`lambda i, when: directory / f"app.{i}.log"`); index 0 is the initial file,
    appended to if it already exists, and the time is that file's open time drawn
    from the same injected clock, so timestamped names stay consistent with the
    rotation decision. `now` is the clock, injected so time-based rotation is
    deterministic under test. This writes strings
    (render a `Record` to text in a `from_map` in front), owns the newline framing,
    and flushes at each burst boundary. The rotation decision is made *before* each
    write (so a size limit is not overshot, bar a single line larger than
    `max_bytes`), and it is inlined and short-circuiting: `now()` is read only when a
    size check has not already forced the rotation and an age limit is set.
    """

    def next_boundary(opened: datetime) -> datetime | None:
        return schedule(opened) if schedule is not None else None

    def work(batches: Iterator[list[str]]) -> None:
        index = 0
        opened = now()
        boundary = next_boundary(opened)
        file = name(index, opened).open("ab")
        written = file.seek(0, os.SEEK_END)
        try:
            for batch in batches:
                for line in batch:
                    data = f"{line}\n".encode()
                    if written > 0 and (
                        (max_bytes is not None and written + len(data) > max_bytes)
                        or (max_age is not None and now() - opened >= max_age)
                        or (boundary is not None and now() >= boundary)
                    ):
                        file.close()
                        index += 1
                        opened = now()
                        boundary = next_boundary(opened)
                        file = name(index, opened).open("ab")
                        written = file.seek(0, os.SEEK_END)
                    written += file.write(data)
                file.flush()
        finally:
            file.close()

    return work


def to_stream(stream: TextIO) -> Callable[[Iterator[list[str]]], None]:
    """
    A blocking worker (for `offload`) that appends each string as a line to an open text stream.

    The destination-shaped sibling of `to_rotating_file`, for a stream the caller
    already owns: `sys.stderr`, a socket's text wrapper, an in-memory buffer. It
    writes strings (render a `Record` to text with a `from_map` in front), owns the
    newline framing, and flushes at each burst boundary. Unlike `to_rotating_file` it
    does *not* open or close the stream: the caller's stream outlives this and may be
    shared, so closing it (`sys.stderr`, say) is not this worker's to do.
    """

    def work(batches: Iterator[list[str]]) -> None:
        for batch in batches:
            for line in batch:
                stream.write(line)
                stream.write("\n")
            stream.flush()

    return work
