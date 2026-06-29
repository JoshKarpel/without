# How processors connect. `compose` chains one processor into the next on the
# event edge (pure composition, nothing running). `stack` composes middleware
# (handler-wrapping functions) into one. `sample` is the behavior edge: it
# exposes a stream's latest value as a Context, which needs a `background_task`
# running, the thin boundary where the imperative shell shows up.
# `stream_from_queue` sits at that same boundary from the other side: not a
# connector between processors but a source adapter that turns a push-based queue
# into a pull-based stream to feed the rest. `stream` (from a fixed iterable) and
# `collect` (drain to a list) are the in-memory source and terminal at that edge.

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import field

from without.contracts import Processor
from without.contracts import Stream
from without.tasks import background_task


def compose[A, B, C](first: Processor[A, B], second: Processor[B, C]) -> Processor[A, C]:
    """
    Compose two processors on the event edge: `first` then `second`.

    The join type `B` may differ from `A` and `C`, so this adapts as well as
    chains. Pure composition (the only event-edge connector that needs nothing
    running); nest for three or more stages.
    """

    def composed(inputs: Stream[A]) -> Stream[C]:
        return second(first(inputs))

    return composed


type Endo[T] = Callable[[T], T]


def stack[H, *Ctx](*middleware: Callable[[H, *Ctx], H]) -> Callable[[H, *Ctx], H]:
    """
    Compose middleware into one, first argument outermost; `stack()` is identity.

    A *middleware* is `(handler, *context) -> handler`: it wraps a handler, given some
    fixed context, into a new handler of the same type. The context is whatever the
    setting threads through unchanged: nothing for a client exchange (`Endo[H]`), the
    connection state and scope for a server handler. `stack` threads the *same* context
    into every middleware and chains the handler through them, first outermost, so
    `stack(f, g)(handler, *context)` is `f(g(handler, *context), *context)`.

    Generic over the handler `H` (the value each middleware transforms) and the context
    pack `*Ctx`, which is bound once per call: every middleware in one `stack(...)` must
    therefore share a shape, and mixing shapes is a type error. The pack passes through
    untouched (never wrapped element-wise), which is exactly why one variadic generic
    covers every arity here where a heterogeneous ladder would be needed.
    """

    def composed(handler: H, *context: *Ctx) -> H:
        for one in reversed(middleware):
            handler = one(handler, *context)
        return handler

    return composed


async def stream_from_queue[T](queue: asyncio.Queue[T]) -> AsyncIterator[T]:
    """
    Expose a queue as a Stream: the bridge from a push source to a pull stream.

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
    """
    Expose a fixed iterable as a `Stream`: the simplest source.

    Turns already-in-hand values into the pull-based `Stream` the rest of
    `without` consumes, e.g. to emit a fixed reply or to feed a processor under
    test. `stream_from_queue` is the push-source counterpart.
    """
    for value in values:
        yield value


async def collect[T](source: Stream[T]) -> list[T]:
    """
    Drain a `Stream` into a list: the terminal that materializes every value.

    The dual of `stream`. It runs until the source ends, so it suits bounded
    streams (a finished request, a shut-down queue); an endless source never
    returns.
    """
    return [value async for value in source]


@dataclass(slots=True)
class Sample[T]:
    _value: T
    _waiters: set[asyncio.Future[T]] = field(default_factory=set)

    def current(self) -> T:
        return self._value

    async def updated(self) -> T:
        """
        Wait for the drain to publish the next value, then return it.

        The deterministic counterpart to `current` on the behavior edge: where
        `current` reads the latest value and never blocks, `updated` blocks until
        the background drain consumes and publishes the *next* value from the
        source, then returns it. It is the "await next update" signal a reader
        waits on (a test asserting on post-reload state, a control loop reacting
        to a config change) instead of guessing how long the background task
        needs.

        Each call registers its own one-shot future resolved by the next publish,
        so concurrent waiters are independent and cancelling one never disturbs
        another. Like `current`, it inherits latest-wins: a waiter sees only
        publishes after it starts waiting, and a source that publishes faster
        than the reader re-arms collapses the values it missed. So `updated` is a
        "the state has moved on" signal, not a way to observe every value;
        consume the stream for that.
        """
        waiter: asyncio.Future[T] = asyncio.get_running_loop().create_future()
        self._waiters.add(waiter)
        return await waiter

    def _publish(self, value: T) -> None:
        self._value = value
        while self._waiters:
            waiter = self._waiters.pop()
            if not waiter.done():
                waiter.set_result(value)


@asynccontextmanager
async def sample[T](source: Stream[T]) -> AsyncIterator[Sample[T]]:
    """
    Connect to a stream on the behavior edge: read its latest value, not each.

    The first value is sampled eagerly, so the context is never "not ready". A
    background task keeps the held value current while the `with` block is open,
    dropping intermediate values (latest-wins, no backpressure). A reader reads
    the held value through `current` (latest, non-blocking) or waits for the next
    one through `updated` (the deterministic "await next update" signal); the held
    value is mutated only by the drain. The yielded `Sample` is a `Context`, so a
    caller that only reads `current` can treat it as one.
    """
    iterator = source.__aiter__()
    try:
        first = await anext(iterator)
    except StopAsyncIteration:
        raise ValueError("sample requires at least one value, but the source stream was empty") from None
    sampled = Sample(first)

    async def drain() -> None:
        async for value in iterator:
            sampled._publish(value)  # noqa: SLF001 - the drain is the sole publisher of the sampled value by design

    async with background_task(drain()):
        yield sampled
