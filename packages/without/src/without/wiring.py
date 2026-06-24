# How processors connect. Two edge types, matching the two halves of the model.
# Event edge (every value flows on): `pipe` (1 to 1), `distribute` (1 to N
# competing), `tee`/`broadcast` (1 to N fan-out), `route` (1 to N by variant),
# and `merge` (N to 1 fan-in). Behavior edge: `sample` exposes a stream's latest
# value as a Context. `pipe` is pure composition; every other connector needs
# something running (a `background_task`), so those are the thin boundaries where
# the imperative shell shows up. `stream_from_queue` sits at the same boundary
# from the other side: not a connector between processors but a source adapter
# that turns a push-based queue into a pull-based stream to feed the rest.
# `stream` (from a fixed iterable) and `collect` (drain to a list) are the
# in-memory source and terminal at that same edge.

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass

from without.contracts import Context
from without.contracts import Processor
from without.contracts import Stream
from without.tasks import background_task


def pipe[In, Out](source: Stream[In], processor: Processor[In, Out]) -> Stream[Out]:
    """Connect a source to a processor on the event edge: every value flows on.

    This is just function composition (`processor(source)`); it exists to name
    the event edge and to read left-to-right. Chaining is nesting: a downstream
    processor takes this result as its own source.
    """
    return processor(source)


async def stream_from_queue[T](queue: asyncio.Queue[T]) -> AsyncIterator[T]:
    """Expose a queue as a Stream: the bridge from a push source to a pull stream.

    A source that *pushes* (a server's accept loop, a callback-based client, a
    pub/sub subscriber) drops values into a queue; this turns that queue into the
    pull-based `Stream` the rest of `without` consumes. It ends gracefully when
    the queue is shut down (`queue.shutdown()`): remaining items still drain, then
    `get` raises `QueueShutDown` and the stream ends, letting a downstream fold
    return its final value. Shutting the queue down is thus the closable-stream
    signal; without it the stream never ends on its own and must be driven inside
    a `background_task` or otherwise cancelled by its consumer.
    """
    while True:
        try:
            yield await queue.get()
        except asyncio.QueueShutDown:
            return


async def stream[T](values: Iterable[T]) -> AsyncIterator[T]:
    """Expose a fixed iterable as a `Stream`: the simplest source.

    Turns already-in-hand values into the pull-based `Stream` the rest of
    `without` consumes, e.g. to emit a fixed reply or to feed a processor under
    test. `stream_from_queue` is the push-source counterpart.
    """
    for value in values:
        yield value


async def collect[T](source: Stream[T]) -> list[T]:
    """Drain a `Stream` into a list: the terminal that materializes every value.

    The dual of `stream`. It runs until the source ends, so it suits bounded
    streams (a finished request, a shut-down queue); an endless source never
    returns.
    """
    return [value async for value in source]


class _Stop:
    """Sentinel marking the end of an internal queue, distinct from any value."""


async def merge[T](*sources: Stream[T]) -> AsyncIterator[T]:
    """Interleave several sources into one stream, in completion order (fan-in).

    The N-to-1 dual of the fan-out connectors: where `distribute` and
    `broadcast` split or copy one stream across many consumers and merge the
    results back, `merge` folds many independent sources into a single stream.
    One forwarding task drains each source into a shared bounded outbox; the
    merged stream ends once every source has. The bound gives backpressure (a
    slow consumer stalls the forwarders rather than buffering an unbounded
    backlog), and the `finally` guarantees the stop sentinel is emitted even if
    a source raises, so the error surfaces here instead of hanging the consumer.
    """
    outbox: asyncio.Queue[T | _Stop] = asyncio.Queue(maxsize=len(sources) or 1)

    async def forward(source: Stream[T]) -> None:
        async for value in source:
            await outbox.put(value)

    async def run() -> None:
        try:
            async with asyncio.TaskGroup() as group:
                for source in sources:
                    group.create_task(forward(source))
        finally:
            await outbox.put(_Stop())

    async with background_task(run()):
        while not isinstance(item := await outbox.get(), _Stop):
            yield item


async def distribute[In, Out](
    source: Stream[In],
    processor: Processor[In, Out],
    workers: int,
) -> AsyncIterator[Out]:
    """Spread one stream across `workers` competing consumers, merging outputs.

    The distribute edge: each event is handled *once*, by whichever worker is
    free, not broadcast to all of them. This is the bounded-concurrency
    primitive: `workers` independent instances of `processor` (each with its
    own state) pull from a shared inbox, and their outputs merge into the single
    stream returned here, in completion order.

    Concurrency is capped at `workers` and backpressure is end-to-end: the
    queues are bounded, so a slow consumer stalls the workers, which stalls the
    feeder pulling from `source`. The source is never drained faster than the
    workers retire it.
    """
    if workers < 1:
        raise ValueError(f"workers must be at least 1, got {workers}")

    inbox: asyncio.Queue[In | _Stop] = asyncio.Queue(maxsize=workers)

    async def feed() -> None:
        try:
            async for event in source:
                await inbox.put(event)
        finally:
            for _ in range(workers):
                await inbox.put(_Stop())

    async def take() -> AsyncIterator[In]:
        while not isinstance(event := await inbox.get(), _Stop):
            yield event

    async with background_task(feed()):
        async for output in merge(*(processor(take()) for _ in range(workers))):
            yield output


@asynccontextmanager
async def tee[T](source: Stream[T], branches: int, buffer: int = 1) -> AsyncIterator[tuple[Stream[T], ...]]:
    """Fan a stream out into `branches` independent copies (the broadcast edge).

    Every branch sees every value, in order: the structural counterpart to
    `distribute` (which splits events *between* consumers). One pump reads the
    source once and pushes each value to every branch. Every branch MUST be
    consumed, and *concurrently*: a full branch queue blocks the pump, so
    draining one branch to exhaustion before starting another deadlocks.

    `buffer` is how many values each branch may run ahead, trading memory for
    slack between a fast and a slow branch: the default `1` keeps memory
    O(branches) and the slowest branch gating the rest, a larger value lets a
    fast branch pull ahead, and `0` is unbounded (no backpressure: a slow
    branch buffers without limit).
    """
    if branches < 1:
        raise ValueError(f"branches must be at least 1, got {branches}")
    if buffer < 0:
        raise ValueError(f"buffer must be at least 0, got {buffer}")

    queues: list[asyncio.Queue[T | _Stop]] = [asyncio.Queue(maxsize=buffer) for _ in range(branches)]

    async def pump() -> None:
        try:
            async for value in source:
                for queue in queues:
                    await queue.put(value)
        finally:
            for queue in queues:
                await queue.put(_Stop())

    async def drain(queue: asyncio.Queue[T | _Stop]) -> AsyncIterator[T]:
        while not isinstance(value := await queue.get(), _Stop):
            yield value

    async with background_task(pump()):
        yield tuple(drain(queue) for queue in queues)


async def broadcast[In, Out](source: Stream[In], *processors: Processor[In, Out]) -> AsyncIterator[Out]:
    """Feed every event to every processor (fan-out), merging their outputs.

    The processor-level convenience over `tee`: `broadcast` is `tee` then
    `merge`. Each processor sees the whole input stream and runs on its own
    branch; their outputs interleave into the single stream returned here. The
    fan-out twin of `distribute`.
    """
    if not processors:
        raise ValueError("broadcast requires at least one processor")

    async with tee(source, len(processors)) as branches:
        merged = merge(*(processor(branch) for processor, branch in zip(processors, branches, strict=True)))
        async for output in merged:
            yield output


def _route_for[T](
    value: T, types: tuple[type[T], ...], queues: list[asyncio.Queue[T | _Stop]]
) -> asyncio.Queue[T | _Stop]:
    for type_, queue in zip(types, queues, strict=True):
        if isinstance(value, type_):
            return queue
    raise TypeError(f"no route for value of type {type(value).__name__!r}")


@asynccontextmanager
async def route[T](source: Stream[T], *types: type[T]) -> AsyncIterator[tuple[Stream[T], ...]]:
    """Partition a stream by the runtime type of each value, one branch per type.

    Unlike `tee` (every branch sees every value), `route` sends each value to
    exactly one branch: the first `types` entry it is an instance of. This is
    how a processor that emits a heterogeneous union (say a response and a
    background-kickoff) splits its output to distinct sinks. A value matching no
    listed type raises rather than being silently dropped, so an unhandled
    variant fails loudly. Branches carry the source type; narrow each downstream.
    """
    if not types:
        raise ValueError("route requires at least one variant type")

    queues: list[asyncio.Queue[T | _Stop]] = [asyncio.Queue(maxsize=1) for _ in types]

    async def pump() -> None:
        try:
            async for value in source:
                await _route_for(value, types, queues).put(value)
        finally:
            for queue in queues:
                await queue.put(_Stop())

    async def drain(queue: asyncio.Queue[T | _Stop]) -> AsyncIterator[T]:
        while not isinstance(value := await queue.get(), _Stop):
            yield value

    async with background_task(pump()):
        yield tuple(drain(queue) for queue in queues)


@dataclass(slots=True)
class Sample[T]:
    _value: T

    def current(self) -> T:
        return self._value


@asynccontextmanager
async def sample[T](source: Stream[T]) -> AsyncIterator[Context[T]]:
    """Connect to a stream on the behavior edge: read its latest value, not each.

    The first value is sampled eagerly, so the context is never "not ready". A
    background task keeps the held value current while the `with` block is open,
    dropping intermediate values (latest-wins, no backpressure). The held value is
    mutated only by the drain; readers see it only through `current`.
    """
    iterator = source.__aiter__()
    sampled = Sample(await anext(iterator))

    async def drain() -> None:
        async for value in iterator:
            sampled._value = value

    async with background_task(drain()):
        yield sampled
