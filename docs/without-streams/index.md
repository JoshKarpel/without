# without-streams

The narrow waist of the project: the interfaces every plugin speaks, plus the
connectors that wire them together. See the
[Philosophy](../philosophy.md) for why the model is shaped this way, and the
[`without_streams` API reference](reference.md) for the full surface.

The asyncio primitives underneath (`background_task`, `timeout`,
`limit_concurrency`, and the `Seconds` / `Milliseconds` counts) live one layer
down in [`without-async`](../without-async/index.md), which speaks only the
standard library and can be taken without this package.

## The substrate (`without_streams.interfaces`)

Three types carry the whole model:

- A `Stream[T]` is an asynchronous sequence of values: the one shape every
  connection takes, whoever does the I/O (a socket, a file watcher, an in-memory
  list).
- A `Processor[In, Out]` transforms a stream of inputs into a stream of outputs.
  It is the only node type and the only thing a user writes; one processor's
  output is another's input, all the way down.
- A `Context[T]` is a stream viewed as its latest sampled value: `current()`
  reads the *latest* and never blocks, the way long-lived state (config, a pool)
  is read.

Processors are built, not subclassed. Four builders cover a 2×2 of
stateful-vs-stateless and emitting-vs-terminal:

| | emits one output per event | collapses to a final value |
|---|---|---|
| **stateless** | `from_map(step)` | `from_sink(step)` (effects only) |
| **stateful** | `from_scan(initial, step)` (a *scan*) | `from_fold(initial, step)` (a reduce) |

A scan emits at every step; a fold yields only the final accumulated value when
the stream ends. The `step` is always `async`, so it MAY `await` contained I/O
(reading dependencies from injected `Context` values), but MUST complete the
effect within the call rather than handing a half-open resource back.

Two more builders subset a stream by a predicate without transforming it:
`from_selector(keep)` re-emits the events matching `keep` and drops the rest (the
keep-the-matches sense of Python's built-in `filter`), and `from_filter(reject)`
is its polarity-dual, dropping the matches. They are the zero-or-one case the
`Processor` protocol always allowed, so no new machinery. Their predicate is
`async` like every other builder step (one color of function throughout, so a
decision that needs to `await` I/O composes without ceremony; a pure one just
never awaits). Emitting *several* outputs per event, by contrast, is a wiring
concern, not a builder: fan-out to several sinks is `tee` (below), and the
fan-in family (`broadcast`, `merge`) is reserved in issue #13.

## Wiring (`without_streams.wiring`)

`compose` chains one processor into the next on the event edge: pure composition,
the only connector that needs nothing running. When its second argument is a
`Sink` rather than a `Processor`, the result is a `Sink` too, which is how a
middleware chain (a filter, an enrichment) is prefixed onto a terminal consumer.
`tee` is its terminal fan-out counterpart: `tee(*sinks)` returns one `Sink` that
splits a stream across several branches, each with its own tail, so a shared
prefix composed ahead of it runs once and every branch consumes its own copy.
`sample` is the behavior edge: it
exposes a stream's latest value as a `Context` (latest-wins, no backpressure),
driven by a `background_task` for the life of its `with` block. The source and
terminal adapters sit alongside: `stream_from_iterable` lifts a fixed iterable
into a `Stream`, `collect` drains one to a list, `stream_from_queue` turns a
push-based queue (an accept loop, a callback client) into the pull-based stream
the rest of the system consumes, and `spool` drives a source ahead of its
consumer (read-ahead) by pumping it into a bounded queue on a background task.
`close_stream` is how a consumer releases a stream it *abandons*: a `Stream` is
`__aiter__`-only, so a source may be a generator holding a `finally` (a file, a
task, a connection) or an object with nothing to release, and this is that
difference in one place rather than in every consumer that can stop early.
Without it the cleanup waits on garbage collection, so a long-lived source
outlives its consumer by an indeterminate amount. `stack`
composes middleware (any `(handler, *context) -> handler`) into one, serving both
server handlers and HTTP clients.

### Crossing the sync boundary

Not everything that has to reach a stream is async, and the two directions across
that boundary are `stream_from_blocking` and `offload`.

`stream_from_blocking` is the source side: `stream_from_iterable` pulls each value
on the loop's own thread, so a source that *blocks* between items (a pipe,
`sys.stdin`, a driver with no async client) parks every other task while it waits.
This runs the whole iteration on one worker thread and hands values across a
bounded queue, so the loop stays free and the producer pipelines ahead by at most
`ahead` items before backpressure reaches it. Handing over the whole loop rather
than awaiting one `next` at a time is what makes it pipeline, at the cost of
holding a thread for the source's lifetime.

`offload` is the terminal side: it yields a `Sink` whose items a plain blocking
`work` function drains on a thread of its own, in bursts of whatever is queued at
that instant. So a file writer stays ordinary synchronous Python and pays no
per-item thread hop, which is what an async file library charges.

They share a mechanism (one long-lived thread on the blocking side, a queue
bridging) and are deliberately *not* mirror images in their interface. A source
has to be pulled, so `stream_from_blocking` bounds its queue and pushes
backpressure into the producer; a sink is pushed, so bounding `offload` would mean
choosing what happens when the worker falls behind (block the async side, or
drop), and it takes neither yet. Both also inherit the one thing a thread cannot
do: a blocked thread cannot be cancelled, so `stream_from_blocking`'s worker is a
daemon thread and abandoning its stream releases the producer's backpressure so it
wakes to notice, rather than parking on a semaphore nobody will post to.

`ticks` is the clock as a source: a stream of moments, one now and one every
interval after, so *when* periodic work happens is a stream a caller supplies
rather than a loop buried inside the work.
