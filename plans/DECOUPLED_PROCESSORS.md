# Decoupled processors: input and output at independent cadences

A design note (not a decision) on a gap surfaced while thinking about
stream-in/stream-out HTTP endpoints: how to write a processor whose input
consumption and output production advance at *different rates*, joined by a
buffer, rather than in lockstep.

## The concern

In a stream-in/stream-out endpoint it is easy to emit several outputs per input,
but it looks hard to consume the input as fast as possible while emitting output
as fast as possible with a queue (buffer) between the two ends running at
independent rates.

The diagnosis: the coupling lives in the *idiom*, not in the `Processor` type.
`Processor[In, Out]` is just `Stream[In] -> Stream[Out]`; nothing in that
contract says outputs must be produced in step with input consumption. But the
natural way to write one is a single async generator:

```python
async def processor(inputs: Stream[In]) -> AsyncIterator[Out]:
    async for event in inputs:
        yield a(event)
        yield b(event)
```

That body is one coroutine, and it is pull-driven: the downstream consumer pulls
the next `Out`, which drives the frame forward, which pulls the next `In`. So
input-consumption rate is welded to output-consumption rate. You can only
`yield` from the top-level generator frame, so two independent loops (one
draining input, one emitting output) cannot coexist in one generator body.

## What we have today (three tiers)

The "yield several outputs per input" worry was a red herring; that already
works. The real picture is three tiers, only the last of which is missing.

- **1:1** — `from_scan` / `from_map` (`without/contracts.py`). A pure step, one
  output per input. The `Transition[S, Out]` carries exactly one `output`, and
  `from_map`'s step returns a single `Out`, deliberately. The docstrings say so:
  *"Splitting one event into several outputs is a wiring-style concern, not a
  per-step one."*
- **1:N, lockstep** — a raw async generator with multiple `yield`s per input.
  Available now, no helper needed. In `without-web` this is the
  `@post.stream` + `async def ... yield` arm: the handler *is* the
  `Stream[Inbound] -> Stream[Outbound]` processor, and `_emit`
  (`without-web/handlers.py`) relays every yielded `Outbound` event by event.
  Even the "buffered" `Response` path emits multiple outbound events
  (`encode_response` yields a `ResponseStart` plus `ResponseBody`s).
- **decoupled (M:N, independent cadence)** — the gap. Input and output advance
  at independent rates through a buffer. This is the only tier with no first-class
  support, because it is the only one where the two ends genuinely run
  concurrently.

So `without-web` already covers everything except the independent-cadence case.

## We already broke this coupling three times

The decoupling pattern is not new to the codebase; it is exactly how every
event-edge connector in `without/wiring.py` works. `merge`, `distribute`, and
`tee` each run a `background_task` that drains a source into a *bounded queue*,
and the returned stream yields from that queue independently. That is the same
producer/consumer-with-a-buffer shape, applied *between* processors.

A decoupled handler is that same pattern applied *inside* one processor: a
background task consumes `inputs` into a buffer, and the generator yields from
the buffer. Nothing in the abstraction blocks it; it just lacks a name, so users
would hand-roll the background task and queue each time.

## Sketch: the reusable connector

Because it needs something running (a background task), this is a wiring concern
and belongs next to `merge` / `distribute` / `tee` in `without/wiring.py`, not in
`contracts.py` (which stays the pure, nothing-running narrow waist). An
`emit`-callback shape lets the ingest side read like a sink that can fire
outputs:

```python
type Emit[Out] = Callable[[Out], Awaitable[None]]


def pipe[In, Out](
    ingest: Callable[[Stream[In], Emit[Out]], Awaitable[None]],
    *,
    buffer: int = 1,
) -> Processor[In, Out]:
    """Decouple input consumption from output production with a bounded buffer.

    `ingest` drains the input at its own pace and calls `emit` to push outputs;
    the processor yields them independently. The two run concurrently joined only
    by the bounded queue, so neither advances in lockstep with the other.
    """

    async def piped(inputs: Stream[In]) -> AsyncIterator[Out]:
        queue: asyncio.Queue[Out] = asyncio.Queue(maxsize=buffer)

        async def run() -> None:
            try:
                await ingest(inputs, queue.put)
            finally:
                queue.shutdown()

        async with background_task(run()):
            async for out in stream_from_queue(queue):
                yield out

    return piped
```

Notes on the sketch:

- **`finally: queue.shutdown()` is load-bearing.** It mirrors `merge`'s
  `finally: put(_Stop())`. Without it, an `ingest` that raises before shutdown
  leaves the consumer hanging on `queue.get()`. With it, the output stream ends
  cleanly and `background_task` then re-raises the error on block exit (a
  finished-with-exception task is not cancelled, so `await task` re-raises the
  stored exception rather than suppressing `CancelledError`). This is exactly the
  error-propagation property `merge` already relies on.
- **Backpressure is automatic and bidirectional.** A full buffer stalls
  `ingest`; an empty one stalls emission. `stream_from_queue` already does the
  push-to-pull bridge and ends on `QueueShutDown`, so it composes directly.
- **`buffer` is the same knob as `tee`'s.** `1` keeps memory O(1) with the
  slower end gating the faster; a larger value lets one end run ahead; `0`
  (unbounded) is true "consume as fast as possible" at the cost of memory. Keep
  the name and semantics aligned with `tee` for consistency.
- **Early downstream exit is handled.** If the consumer stops pulling,
  `background_task` cancels `run()` on block exit, so the ingest task cannot leak.

## Sketch: the fully-independent-cadence case

The strongest form is when the producer is not driven by each input at all (a
heartbeat, batched flushes, periodic snapshots of accumulated state). Then
`ingest` runs *two* coroutines over shared local state. That state never escapes
the processor, so the values-over-places rule is satisfied: the mutation is
contained, not observable to any other holder.

```python
async def ingest(inputs: Stream[In], emit: Emit[Out]) -> None:
    state = WorkingSet()

    async def consume() -> None:
        async for event in inputs:
            state.absorb(event)            # pure ingest, no output

    async def produce() -> None:
        while not state.done():
            await emit(state.snapshot())   # own cadence: timer, batch-ready, ...
            await asyncio.sleep(INTERVAL)

    async with asyncio.TaskGroup() as group:
        group.create_task(consume())
        group.create_task(produce())
```

This composes with `pipe` unchanged: `pipe` owns the buffer and the
push-to-pull bridge; this `ingest` just happens to feed `emit` from a loop
independent of the input loop.

## Open questions / things to settle before building

- **Name.** `pipe` reads well but is generic; alternatives: `decouple`,
  `buffered` (taken in `without-web`/`without-asgi` for the body-buffering
  helper, so avoid the collision), `bridge`, `pump`. Lean toward a name that says
  "the two ends run at independent cadences."
- **Shape of `ingest`.** The `emit`-callback form is one option. An alternative
  is to express ingest and produce as two separate arguments (a `Sink[In]`-like
  consumer and a `Stream[Out]` producer) sharing a constructed buffer, but that
  pushes the shared-state plumbing onto the caller; the single-`ingest` form
  keeps the shared working set as an ordinary local. Prefer the single-`ingest`
  form unless a concrete use wants the split.
- **Whether it belongs in core `without` or only as a documented recipe.** It is
  a thin combinator over `background_task` + `asyncio.Queue` +
  `stream_from_queue`, all already public. Argument for landing it in
  `wiring.py`: it is the same pattern as `merge`/`distribute`/`tee` and users
  should not re-derive the `finally: shutdown()` subtlety. Argument against:
  keep the surface small and let it stay a recipe until a real endpoint needs
  it. Lean toward landing it once a concrete `without-web` or `without-http` use
  appears (make the change easy, then make the easy change).
- **Discoverability from the 1:1 builders.** Once `pipe` exists, add a one-line
  pointer in `from_scan`/`from_map` docstrings ("for output at a cadence
  independent of input, see `pipe`") so the path from the easy case to the
  decoupled one is visible.
- **`without-web` surface.** Should there be a `@post.pipe(...)` sibling to
  `@post.stream(...)`, or is the bare streaming handler plus a call to `pipe`
  enough? Probably the latter first: `@post.stream` already hands the handler the
  live inbound stream, and the handler can call `pipe` itself.

## Verification (when built)

- Unit tests in the `without/wiring.py` test style: feed a known input stream,
  assert the output sequence and that buffering actually decouples the rates
  (e.g. a slow consumer with a fast finite producer still receives every item;
  an `ingest` that raises ends the output stream and surfaces the error on block
  exit; early downstream exit cancels the background task).
- Property: with `buffer=0` (unbounded) and a bounded input, output is a
  permutation/relabeling consistent with `ingest`'s emissions, and no item is
  lost or duplicated.
- An integration example: a `without-web` `@post.stream` endpoint that ingests a
  streaming upload into a working set and emits periodic progress snapshots on
  its own cadence, demonstrating M:N decoupling end-to-end.
</content>
</invoke>
