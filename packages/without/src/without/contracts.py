# The narrow waist of `without`. Vocabulary: a Stream carries events; a
# Processor transforms a stream of inputs into a stream of outputs; a Context is
# a stream viewed as its latest sampled value (a "behavior"). Normative keywords
# (MUST, SHOULD, MAY) are per RFC 2119. The design rationale (events vs.
# behaviors, why there is no privileged executor) lives in PHILOSOPHY.md.

from __future__ import annotations

from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol
from typing import runtime_checkable


@dataclass(frozen=True, slots=True)
class Transition[S, Out]:
    """The result of folding one event into a scan's state.

    A value, never a place: a step returns the next `state` and the single
    `output` it emits, and mutates nothing the caller can observe. Splitting
    one event into several outputs is a wiring-style concern, not a per-step
    one, so a transition carries one output rather than a collection.
    """

    state: S
    output: Out


@runtime_checkable
class Stream[T](Protocol):
    """An asynchronous sequence of values.

    A stream is the single shape every connection has. Sources that touch the
    outside world (a socket, a file watcher, a clock) are streams too: a stream
    is just the one shape every connection takes, whoever does the I/O.
    """

    def __aiter__(self) -> AsyncIterator[T]: ...


class Processor[In, Out](Protocol):
    """A transformation from a stream of inputs to a stream of outputs.

    This is the only thing a user writes, and the only node type: a processor's
    output stream becomes another processor's input stream, all the way down.

    I/O is *decoupled, not forbidden*. A processor MAY `await` I/O while
    handling an event (a database query, a closed-lifespan sub-request), reading
    its dependencies from injected `Context` values; this is why a scan's
    `step` is async. The point is not to ban I/O but to separate it into the
    right abstractions so the parts stay reusable: sources at the edge, behaviors
    via `sample`, effects contained in the step. The one rule: an effect MUST
    NOT escape the entrypoint. A processor awaits its I/O to completion and MUST
    NOT hand a half-open resource (an open socket, an unfinished task it does not
    own) back to the runtime. Testing injects fake `Context` dependencies.
    """

    def __call__(self, inputs: Stream[In]) -> Stream[Out]: ...


@runtime_checkable
class Context[T](Protocol):
    """A stream viewed as its latest value: the "behavior" half of the model.

    Where consuming a stream sees *every* event, `current` samples the *latest*
    and never blocks. This is how long-lived state (config, a connection pool) is
    read: a context is just another processor's output that a reader samples
    rather than consumes. `current` MUST return a value; a context is never
    "not ready". The reader only ever gets a value, never a writable place.
    """

    def current(self) -> T: ...


def from_scan[In, S, Out](
    initial: S,
    step: Callable[[In, S], Awaitable[Transition[S, Out]]],
) -> Processor[In, Out]:
    """Build a processor from a stateful step that emits an output every event.

    `step` is the kernel: given an event and the current state it returns the
    next state and the output it emits. It is `async` so it MAY `await` contained I/O
    (reading dependencies from `Context` values captured by closure), but a
    `step` that does no I/O is just an `async def` that never awaits.
    `from_scan` supplies the loop that threads state across the input stream,
    emitting one output per event: a scan, not a reduce (the collapse-to-one-
    value form is `from_fold`). The effect MUST complete within each call (see
    `Processor`).
    """

    def processor(inputs: Stream[In]) -> Stream[Out]:
        return _scan(inputs, initial, step)

    return processor


async def _scan[In, S, Out](
    inputs: Stream[In],
    initial: S,
    step: Callable[[In, S], Awaitable[Transition[S, Out]]],
) -> AsyncIterator[Out]:
    state = initial
    async for event in inputs:
        transition = await step(event, state)
        state = transition.state
        yield transition.output


def from_map[In, Out](
    step: Callable[[In], Awaitable[Out]],
) -> Processor[In, Out]:
    """Build a processor from a stateless step: each event maps to one output.

    The counterpart to `from_scan` for a processor that holds no state.
    Each event is handled independently of every other, so there is no
    `initial` to seed and no `Transition` to thread: `step` maps an event
    straight to its single output. Like `from_scan` the step is `async`
    so it MAY `await` contained I/O, and the effect MUST complete within each
    call (see `Processor`). Splitting one event into several outputs is a
    separate, wiring-style concern, not a per-step one, so the step returns a
    single value rather than a collection.
    """

    def processor(inputs: Stream[In]) -> Stream[Out]:
        return _map(inputs, step)

    return processor


async def _map[In, Out](
    inputs: Stream[In],
    step: Callable[[In], Awaitable[Out]],
) -> AsyncIterator[Out]:
    async for event in inputs:
        yield await step(event)


type Sink[In] = Callable[[Stream[In]], Awaitable[None]]
type Fold[In, S] = Callable[[Stream[In]], Awaitable[S]]


def from_sink[In](step: Callable[[In], Awaitable[None]]) -> Sink[In]:
    """Build a leaf that consumes a stream for its effects and emits nothing.

    The stateless terminus, dual to `from_map`: where a map turns each event
    into an output, a sink turns each event into an effect and yields no output
    stream at all. Awaiting it drains the stream to completion (or runs forever,
    for an unbounded source driven inside a `background_task`). The `step`
    MAY `await` contained I/O.
    """

    async def sink(inputs: Stream[In]) -> None:
        async for event in inputs:
            await step(event)

    return sink


def from_fold[In, S](initial: S, step: Callable[[In, S], Awaitable[S]]) -> Fold[In, S]:
    """Build a leaf that folds a stream of events into a single final state.

    The stateful terminus, dual to `from_scan`: where `from_scan` threads
    state and emits an output every step (a scan), `from_fold` threads state
    and yields only the final accumulated value when the stream ends (a true
    reduce). The `step` MAY `await` contained I/O, so a fold whose result you
    ignore is also how you run a stateful consumer for its effects.
    """

    async def folded(inputs: Stream[In]) -> S:
        state = initial
        async for event in inputs:
            state = await step(event, state)
        return state

    return folded
