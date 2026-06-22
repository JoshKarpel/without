# The narrow waist of `without`. Vocabulary: a Stream carries events; a
# Processor transforms a stream of inputs into a stream of outputs; a Context is
# a stream viewed as its latest sampled value (a "behavior"). Normative keywords
# (MUST, SHOULD, MAY) are per RFC 2119. The design rationale (events vs.
# behaviors, why there is no privileged executor) lives in plans/REVIEW_BIG_IDEA.md.

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class Transition[S, Out]:
    """The result of folding one event into a reducer's state.

    A value, never a place: a step returns the next ``state`` and the ``outputs``
    it emits (zero or more), and mutates nothing the caller can observe.
    """

    state: S
    outputs: tuple[Out, ...] = field(default=())


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

    I/O is *decoupled, not forbidden*. A processor MAY ``await`` I/O while
    handling an event (a database query, a closed-lifespan sub-request), reading
    its dependencies from injected ``Context`` values; this is why a reducer's
    ``step`` is async. The point is not to ban I/O but to separate it into the
    right abstractions so the parts stay reusable: sources at the edge, behaviors
    via ``sample``, effects contained in the step. The one rule: an effect MUST
    NOT escape the entrypoint. A processor awaits its I/O to completion and MUST
    NOT hand a half-open resource (an open socket, an unfinished task it does not
    own) back to the runtime. Testing injects fake ``Context`` dependencies.
    """

    def __call__(self, inputs: Stream[In]) -> Stream[Out]: ...


@runtime_checkable
class Context[T](Protocol):
    """A stream viewed as its latest value: the "behavior" half of the model.

    Where consuming a stream sees *every* event, ``current`` samples the *latest*
    and never blocks. This is how long-lived state (config, a connection pool) is
    read: a context is just another processor's output that a reader samples
    rather than consumes. ``current`` MUST return a value; a context is never
    "not ready". The reader only ever gets a value, never a writable place.
    """

    def current(self) -> T: ...


def from_reducer[In, S, Out](
    initial: S,
    step: Callable[[In, S], Awaitable[Transition[S, Out]]],
) -> Processor[In, Out]:
    """Build a processor from a reducer.

    ``step`` is the kernel: given an event and the current state it returns the
    next state and any outputs. It is ``async`` so it MAY ``await`` contained I/O
    (reading dependencies from ``Context`` values captured by closure), but a
    ``step`` that does no I/O is just an ``async def`` that never awaits.
    ``from_reducer`` supplies the scan: the loop that threads state across the
    input stream. The effect MUST complete within each call (see ``Processor``).
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
        for output in transition.outputs:
            yield output
