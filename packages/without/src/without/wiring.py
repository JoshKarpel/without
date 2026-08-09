# How processors connect. `compose` chains one processor into the next on the
# event edge (pure composition, nothing running), or a processor onto a terminal
# `Sink` to yield a `Sink`. `tee` is the terminal fan-out: it splits one stream
# across several `Sink` branches, each with its own tail, so a shared prefix runs
# once and each branch consumes its own copy. `stack` composes middleware
# (handler-wrapping functions) into one. `sample` is the behavior edge: it
# exposes a stream's latest value as a Context, which needs a `background_task`
# running, the thin boundary where the imperative shell shows up.
# `stream_from_queue` sits at that same boundary from the other side: not a
# connector between processors but a source adapter that turns a push-based queue
# into a pull-based stream to feed the rest. `spool` uses that same queue to drive
# a pull source ahead of its consumer in a background task, decoupling their pace
# (read-ahead). `stream_from_iterable` (from a fixed iterable) and `collect`
# (drain to a list) are the in-memory source and terminal at that edge.

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from typing import overload

from without.interfaces import Processor
from without.interfaces import Sink
from without.interfaces import Stream
from without.tasks import background_task


async def stream_from_iterable[T](values: Iterable[T]) -> AsyncIterator[T]:
    """
    Expose a fixed iterable as a `Stream`: the simplest source.

    Turns already-in-hand values into the pull-based `Stream` the rest of
    `without` consumes, e.g. to emit a fixed reply or to feed a processor under
    test. `stream_from_queue` is the push-source counterpart.
    """
    for value in values:
        yield value


def utc_now() -> datetime:
    return datetime.now(UTC)


async def ticks(every: timedelta, *, now: Callable[[], datetime] = utc_now) -> AsyncGenerator[datetime]:
    """
    A `Stream` of moments, one now and one every `every` after: the clock as a source.

    The source periodic work runs off, so that *when* something happens is a stream a
    caller supplies rather than a loop inside the thing being done. A cache sweep, a
    config refresh, a queue's housekeeping: each becomes a `Sink` that says only what
    happens per event, and composing it with this says how often. A `while True` with a
    `sleep` in it can only ever be a timer, and it buries the schedule inside the work;
    a sink over a stream runs off this, off `stream_from_queue` when an operator pokes
    it, or off `stream_from_iterable` in a test that chooses the instants.

    Each tick *carries* its moment, so a consumer needs no clock of its own and a test
    controls time by choosing values rather than by patching one.

    It yields before it sleeps, so the first event lands at once rather than one interval
    later, and it never ends on its own: drive it inside a `background_task`, a task
    group, or anything else that will cancel it.

    The sleep goes *after* the yield rather than being measured from it, so the period is
    `every` plus however long the consumer took, and the moments drift later by that much
    each time. That is the right trade for the work this drives: a sweep that runs on a
    fixed period instead of a fixed cadence can never overlap itself, where a scheduler
    that chased a wall-clock grid would fire back-to-back to catch up after one slow pass,
    which is exactly the wrong response to a dependency that has gone slow. What it means
    for a caller is that `every` is a floor on the gap between events rather than a
    promise about when each one lands, so a consumer that needs the true elapsed time
    reads the moment it is handed rather than counting ticks.

    An interval that is not positive is refused rather than run, for the same reason
    `drive` refuses a `limit` below one: it has no sensible reading, and taken literally
    it is a loop that yields as fast as the sink can consume, which pins a core to do
    housekeeping. A zero arrives from a configured duration whose setting was never set,
    so it is worth one comparison here.
    """
    if every <= timedelta():
        raise ValueError(f"every must be a positive duration, but got {every}")
    every_seconds = every.total_seconds()
    while True:
        yield now()
        await asyncio.sleep(every_seconds)


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


async def spool[T](source: Stream[T], ahead: int) -> AsyncIterator[T]:
    """
    Drive a source ahead of its consumer through a bounded queue: read-ahead.

    A background task pulls from `source` as fast as backpressure allows and
    drops each value into a queue of at most `ahead` items; the returned stream
    yields from that queue. So the source is *driven* independently of how fast
    the consumer pulls: a pull-based producer (an accept loop, a file's chunks, a
    DAG's `executed` iterator) keeps making progress while a slower consumer
    catches up, up to `ahead` items of slack before `put` blocks and backpressure
    reaches the producer. That overlaps the producer's work with the consumer's,
    e.g. reading the next file chunk while the current one is still being written
    to a socket.

    `ahead` must be at least 1: the bound *is* the backpressure, so an unbounded
    spool (which could let a fast producer grow memory without limit) is a
    `ValueError` rather than a silent default. When `source` ends the queue is
    shut down and the stream ends once drained; if `source` raises, the spooled
    items still drain and then the error surfaces. Closing the stream early
    cancels the background task, so the producer never outlives its consumer.
    """
    if ahead < 1:
        raise ValueError(f"ahead must be at least 1, but got {ahead}")

    queue: asyncio.Queue[T] = asyncio.Queue(ahead)

    async def pump() -> None:
        try:
            async for item in source:
                await queue.put(item)
        finally:
            queue.shutdown()

    async with background_task(pump()):
        async for item in stream_from_queue(queue):
            yield item


async def collect[T](source: Stream[T]) -> list[T]:
    """
    Drain a `Stream` into a list: the terminal that materializes every value.

    The dual of `stream_from_iterable`. It runs until the source ends, so it suits bounded
    streams (a finished request, a shut-down queue); an endless source never
    returns.
    """
    return [value async for value in source]


@overload
def compose[A, B, C](first: Processor[A, B], second: Processor[B, C]) -> Processor[A, C]: ...


@overload
def compose[A, B](first: Processor[A, B], second: Sink[B]) -> Sink[A]: ...


def compose[A, B, C](first: Processor[A, B], second: Processor[B, C] | Sink[B]) -> Processor[A, C] | Sink[A]:
    """
    Compose two processors on the event edge: `first` then `second`.

    The join type `B` may differ from `A` and `C`, so this adapts as well as
    chains. Pure composition (the only event-edge connector that needs nothing
    running); nest for three or more stages. When `second` is a `Sink` rather than
    a `Processor` the result is a `Sink` too: the same wiring, terminated, which is
    how a middleware chain (a filter, an enrichment) is prefixed onto a terminal
    consumer such as a writer.
    """

    def composed(inputs: Stream[A]) -> Stream[C] | Awaitable[None]:
        return second(first(inputs))

    return cast("Processor[A, C] | Sink[A]", composed)  # pragma: no mutate - cast is a runtime no-op


def tee[T](*sinks: Sink[T], buffer: int = 1) -> Sink[T]:  # pragma: no mutate - default depth; effect is timing-only
    """
    Fan one stream out to every `sink`: the terminal counterpart to `compose`.

    Where `compose` chains a processor onto a *single* sink, `tee` splits the stream
    across several, after the Unix tool that writes one input to many destinations.
    Every sink sees every event, in order, and the input is consumed exactly once.
    The caller controls the split point purely by placement, since each argument is
    itself a `Sink`: whatever is composed *before* the `tee` is the shared prefix
    (parsed and enriched once), and each branch is its own `Sink`, carrying its own
    filtering, rendering, and terminal. A branch MAY itself be another `tee`, so
    "one input, several sink groups" nests without new machinery.

    One pump reads the source once and pushes each value onto every branch's bounded
    queue; each sink drains its own queue concurrently, and when the source ends the
    queues are shut so each branch's stream ends and its sink returns. Every branch
    MUST be consumed to completion and concurrently: a sink that stops early leaves
    its queue to fill and stalls the pump (real sinks, a filter that drops events
    included, drain every input). A sink failure tears the whole `tee` down and
    surfaces as an `ExceptionGroup`, so a broken branch fails loud rather than
    silently starving the rest.

    `buffer` is how far a branch may run ahead: the queues are bounded to it, so the
    slowest branch gates the pump (and thus backpressure onto the source), while a
    larger value trades memory for slack so a fast branch need not wait on a slow
    one. The default `1` keeps memory `O(sinks)`. At least one sink is REQUIRED, and
    `buffer` MUST be at least 1 (an unbounded branch could grow memory without limit).
    """
    if not sinks:
        raise ValueError("tee requires at least one sink")
    if buffer < 1:
        raise ValueError(f"buffer must be at least 1, but got {buffer}")

    async def teeing(inputs: Stream[T]) -> None:
        queues: list[asyncio.Queue[T]] = [asyncio.Queue(maxsize=buffer) for _ in sinks]

        async def pump() -> None:
            try:
                async for value in inputs:
                    for queue in queues:
                        await queue.put(value)
            finally:
                for queue in queues:
                    queue.shutdown()

        async def drive(sink: Sink[T], queue: asyncio.Queue[T]) -> None:
            await sink(stream_from_queue(queue))

        async with asyncio.TaskGroup() as group:
            group.create_task(pump())
            for sink, queue in zip(sinks, queues, strict=True):  # pragma: no mutate - lengths always equal
                group.create_task(drive(sink, queue))

    return teeing


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


@dataclass(slots=True)
class Sample[T]:
    _value: T
    _waiters: set[asyncio.Future[T]] = field(default_factory=set, init=False, repr=False, compare=False)
    _error: BaseException | None = field(default=None, init=False, repr=False, compare=False)

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
        needs. If the source raises instead of yielding, the wait raises that
        error rather than hanging, and the failure is terminal: once the source
        has failed, every later call re-raises it rather than registering a
        waiter that can never resolve. If the context closes first, the wait is
        cancelled.

        Each call registers its own one-shot future resolved by the next publish,
        so concurrent waiters are independent: cancelling one deregisters it at
        once and never disturbs another. Like `current`, it inherits latest-wins: a waiter sees only
        publishes after it starts waiting, and a source that publishes faster
        than the reader re-arms collapses the values it missed. So `updated` is a
        "the state has moved on" signal, not a way to observe every value;
        consume the stream for that.
        """
        if self._error is not None:  # the source already failed; fail fast rather than wait forever
            raise self._error
        waiter: asyncio.Future[T] = asyncio.get_running_loop().create_future()
        self._waiters.add(waiter)
        try:
            return await waiter
        finally:
            self._waiters.discard(waiter)

    def _publish(self, value: T) -> None:
        self._value = value
        self._settle(lambda waiter: waiter.set_result(value))

    def _fail(self, error: BaseException) -> None:
        self._error = error
        self._settle(lambda waiter: waiter.set_exception(error))

    def _settle(self, outcome: Callable[[asyncio.Future[T]], None]) -> None:
        while self._waiters:
            waiter = self._waiters.pop()
            if not waiter.done():  # skip a waiter cancelled in the window before its own cleanup ran
                outcome(waiter)

    def _close(self) -> None:
        while self._waiters:
            self._waiters.pop().cancel()


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
    caller that only reads `current` can treat it as one. When the block exits,
    any still-pending `updated` waits are cancelled, so a task awaiting one is not
    left hanging on a context that has closed.
    """
    iterator = source.__aiter__()
    try:
        first = await anext(iterator)
    except StopAsyncIteration:
        raise ValueError("sample requires at least one value, but the source stream was empty") from None
    sampled = Sample(first)

    async def drain() -> None:
        try:
            async for value in iterator:
                sampled._publish(value)  # noqa: SLF001 - the drain is the sole publisher of the sampled value by design
        except Exception as error:
            sampled._fail(error)  # noqa: SLF001 - the drain hands a source failure to anyone awaiting `updated`
            raise

    async with background_task(drain()):
        try:
            yield sampled
        finally:
            sampled._close()  # noqa: SLF001 - the context owns the sample's lifecycle, so it releases the waiters
