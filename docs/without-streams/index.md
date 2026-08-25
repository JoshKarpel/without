# without-streams

The narrow waist of the project: the interfaces every plugin speaks, plus the
stream connectors, a `with`-scoped background task helper, and the duration
counts the boundaries share. See the
[Philosophy](../philosophy.md) for why the model is shaped this way, and the
[`without_streams` API reference](reference.md) for the full surface.

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

## Durations that cross an integer boundary (`without_streams.durations`)

A `timedelta` is the right type for a duration everywhere in this project,
because it names its unit and nothing downstream has to guess whether a bare
number meant seconds or milliseconds. What it cannot say is that a duration
*survives* the boundary it is about to cross. A TCP keepalive knob carries whole
seconds, an SSE `retry:` line carries whole milliseconds, SQLite's `busy_timeout`
carries whole milliseconds: each one truncates whatever it is handed, and
truncation is at its worst where it is least visible. Half a millisecond of
`retry:` becomes `retry: 0`, which does not mean "almost no wait", it means
"reconnect immediately".

`Seconds` and `Milliseconds` are what such a parameter declares instead, and what
makes them worth having is what they *cannot* hold:

```python
tcp_keepalive(idle=Seconds(60), interval=Seconds(10))
yield Retry(Milliseconds(30_000))
```

Each is a count, not a duration, so there is no argument to either constructor
that names a finer unit. `duration` is the `timedelta` back out, for arithmetic
and for anything taking a plain duration, and `of` is the one way in from one:

```python
Seconds.of(settings.keepalive_idle)  # raises on a duration finer than a second
```

That is the only place the question "does this divide?" is ever asked, which is
the point: past construction there is nothing left for a boundary to check, and
no truncation left for it to do.

## Tasks (`without_streams.tasks`)

The async task helpers: `sleep_forever`; the `with`-scoped `background_task`
(starts a task on entry, cancels-then-awaits it on exit, so nothing leaks);
`timeout`, a `timedelta | None`-typed wrapper over `asyncio.timeout` that models
"no limit" as `None` (an always-open context) rather than a sentinel float;
`limit_concurrency`, a bounded-concurrency driver that pulls work from a source
only while below the limit (so a lazy source is never advanced past it); and its
building blocks `cancel_futures` (cancel a set, then await them all) and
`as_async_iterator` (normalize a sync or async iterable into one async iterator).
