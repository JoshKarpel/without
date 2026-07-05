from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import asynccontextmanager

from without.contracts import Sink
from without.wiring import stream_from_queue

from without_logging.record import Record
from without_logging.record import parse_record


class CaptureHandler(logging.Handler):
    """
    A stdlib handler that parses each `LogRecord` and offers it to an asyncio queue.

    This is the one-way bridge at the ingestion edge. Stdlib `logging` is the
    de-facto narrow waist every third-party library already writes to, so making
    it a *source* (rather than fighting it) is the whole trick: a pushed
    `LogRecord` is parsed to a `Record` here and dropped onto the queue that
    `stream_from_queue` turns into the pull-based stream the pipeline consumes.
    Control only ever flows outward from stdlib into `without`, never back.

    A log call MAY happen off the event loop thread, so the parsed value is handed
    to the loop with `call_soon_threadsafe`. The queue is bounded, so a burst that
    outruns the sink is dropped rather than growing memory without limit;
    `dropped` counts how many so an operator can see it. The overflow policy is a
    boundary decision the app owns: raise `capacity`, or pass `capacity=None` for
    an unbounded queue that never drops (at the cost of the bound).
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        queue: asyncio.Queue[Record],
        parse: Callable[[logging.LogRecord], Record],
    ) -> None:
        super().__init__()
        self._loop = loop
        self._queue = queue
        self._parse = parse
        self.dropped = 0

    def emit(self, log_record: logging.LogRecord) -> None:
        record = self._parse(log_record)
        self._loop.call_soon_threadsafe(self._offer, record)

    def _offer(self, record: Record) -> None:
        try:
            self._queue.put_nowait(record)
        except asyncio.QueueFull, asyncio.QueueShutDown:
            self.dropped += 1


@asynccontextmanager
async def capture(
    sink: Sink[Record],
    *,
    logger: logging.Logger | None = None,
    level: int = logging.INFO,
    capacity: int | None = 1024,
    parse: Callable[[logging.LogRecord], Record] = parse_record,
) -> AsyncIterator[CaptureHandler]:
    """
    Capture stdlib log records into `sink` for the duration of the block.

    The imperative shell of the pipeline, and the only impure piece: it attaches a
    `CaptureHandler` to `logger` (the root logger by default, so third-party logs
    are captured too), runs `sink(stream_from_queue(queue))` in a background task,
    and on exit detaches, flushes pending records, shuts the queue so the stream
    ends gracefully, and joins the task. `sink` is a fully wired pipeline, e.g.
    `compose(from_selector(at_least(Level.WARNING)), from_sink(write))`;
    everything upstream of the queue in it is pure and testable on its own.

    `level` sets the capture threshold. stdlib applies *two* gates: the origin
    logger's effective level (checked where a record is created, and defaulting to
    WARNING on the root, which is what usually swallows INFO) and each handler's
    own level. The logger gate is the load-bearing one: a sub-threshold record is
    dropped at creation before any handler runs, so setting the handler level
    alone would capture nothing new. So this temporarily lowers `logger`'s level
    to `level` (restoring it on exit, scoped to the block) *and* sets the handler
    to `level`, the latter keeping the threshold exact for records that propagate
    up from a descendant logger pinned to its own lower level.

    The handler itself is yielded so a caller can read `handler.dropped` after
    the block to see how many records overflowed the queue.
    """
    target = logger if logger is not None else logging.getLogger()
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[Record] = asyncio.Queue(capacity if capacity is not None else 0)
    handler = CaptureHandler(loop, queue, parse)
    handler.setLevel(level)

    async def run_sink() -> None:
        await sink(stream_from_queue(queue))

    previous_level = target.level
    target.setLevel(level)
    target.addHandler(handler)
    drain = asyncio.create_task(run_sink())
    try:
        yield handler
    finally:
        target.removeHandler(handler)
        target.setLevel(previous_level)
        await asyncio.sleep(0)  # let already-scheduled emits reach the queue before it shuts
        queue.shutdown()
        await drain
