# without-logging

A `without` pipeline for logs: logger calls produce a stream of records, which
you process (filter, enrich, sample) and drain into a sink. See the
[`without_logging` API reference](../reference/without_logging.md) for the full
surface.

This guide doubles as the design narrative for the package, because the
interesting part is not the code (it is small) but *why the shape fits*, and
where it deliberately stops.

## The bet: a log record is a value

`structlog` and stdlib `logging` are hard to reason about for a few related
reasons: the configuration is bespoke, noun-heavy, and mutable; a `LogRecord` is
a mutable bag that filters and formatters rewrite in place; and the handlers are
monolithic, bundling unrelated decisions (when to flush, how to rotate, how to
encode) into one object you configure rather than compose.

`without` already has the substrate to take these apart. A logger call ultimately
produces a stream of records. So:

- A **record is a value** (`Record`), immutable, always meaning the same thing.
  It can be filtered, enriched, fanned out, and sunk without any stage disturbing
  another's copy. Enrichment returns a *new* record (`with_fields`), never
  mutating one in place.
- **Middleware is composed processors.** Level filtering, field enrichment, and
  sampling are each a `Processor[Record, Record]`, chained with `compose`. There
  is no `Filter` / `Formatter` / `Handler` class hierarchy, just stream-to-stream
  functions.
- A **sink is `from_sink`.** The monolithic-handler complaint dissolves
  structurally: rotation is one processor, batching is another, encoding is
  another, the write is the terminal sink. You compose the combination you want
  instead of picking from a fixed menu of bundled handlers.

## The ingestion edge cannot be pure, so name it and contain it

The whole `without` bet is a pure interior with effects at the edge. But a
`log.info(...)` call happens *anywhere*, often deep inside code that is otherwise
pure, and it has to reach the stream without threading a logger argument through
every call. That is exactly the ambient-global-place the philosophy rejects.

The resolution is not to fight it but to name it as the edge and contain it. The
blessed tool already exists: `stream_from_queue` adapts a push source into a pull
stream, and a logger *is* a push source. So a logger becomes a source stream at
the rim of the shell, and everything downstream of the queue is pure composed
processors.

`capture` is that edge, and the only impure piece in the package. Inside its
block it attaches a `CaptureHandler` to a logger, and each pushed `LogRecord` is
parsed to a `Record` and offered to an `asyncio.Queue`:

```python
async with capture(pipeline, level=Level.INFO):
    ...  # any code in here that logs is captured
```

Because a log call MAY happen off the event loop thread, the handler hands the
parsed value to the loop with `call_soon_threadsafe`. On exit, `capture`
detaches the handler, flushes pending records, shuts the queue so the stream
ends gracefully, and joins the sink task.

## Third-party capture: stdlib as a one-way source

You cannot escape stdlib `logging` at the edge, because it *is* Python's de-facto
narrow waist for library logs: every package calls `logging.getLogger(__name__)`.
The usual `structlog` confusion is that control ping-pongs across the stdlib
boundary. Here it does not. `capture` installs exactly one handler at the root
logger, and from then on stdlib is strictly *upstream*: a leaf source that feeds
the queue and is never called back into. Control flows one way, out of stdlib
into `without`.

That is also why `capture` temporarily lowers the target logger's level (and
restores it on exit): stdlib gates records at the *logger* as well as the
handler, so "capture at INFO and above" only works if the logger passes INFO.
The change is scoped to the block, so it reads as a value the block owns rather
than a mutation left behind.

## Parse, don't validate: `LogRecord` to `Record`

`parse_record` is the boundary. It is a pure function, `LogRecord -> Record`, so
the whole translation is testable without any logging machinery:

- The message is rendered with its `%`-args (`getMessage()`), so the record
  carries the final text.
- `fields` is every attribute outside stdlib's standard envelope (`RESERVED`),
  which is exactly what `logging`'s `extra=` merges in, exposed read-only.
- `level` stays a plain `int`. A `Level` `IntEnum` exists for writing readable
  predicates (`at_least(Level.WARNING)`), but the record carries the number, so a
  third-party library logging at a non-standard numeric level is carried through,
  not rejected. This is the "fail loud for the author, recover for the remote"
  rule: a library's odd level is not ours to reject.

## Filtering is an ordinary processor, so it lives in core

A `Processor` is any `Stream -> Stream` function, so a step is free to yield *any*
number of outputs per input: twice to split one event into two, once to map, or
zero to drop it. Nothing in the protocol privileges the one-in-one-out case; the
count-preserving 2x2 builders (`from_map` / `from_scan` / `from_sink` /
`from_fold`) just happen to cover the common shapes. A filter is the same
protocol with a zero-or-one body, the async generator that yields on a match and
otherwise skips to the next input.

That is ordinary enough that it belongs in `without` core, not here: needing to
filter a log stream is exactly what motivated adding
[`from_selector` and `from_filter`](../reference/without.md) to the core
builders. `from_selector(keep)` re-emits the matching records; `from_filter`
drops them (the polarity-dual). So without-logging ships only the *predicates*
that are specific to logs, and composes them with the core selector:

```python
from without import compose, from_selector, from_sink
from without_logging import Level, at_least

pipeline = compose(from_selector(at_least(Level.WARNING)), from_sink(write))
```

`at_least(level)` is an `async` predicate over a `Record` (predicates are one
color of function throughout `without`, so it matches what `from_selector`
expects even though the decision just compares integers). Splitting one record
across *several* sinks is the other direction, and it stays a wiring concern (the
fan-out family, issue #13); see below.

Enrichment, by contrast, *is* count-preserving, so `add_fields` is a plain
`from_map`. Dynamic enrichment (stamping a request id sampled from a `Context`)
is the same shape, reading the value via `current()` inside the step.

## Overflow is a boundary decision the app owns

Logging must not stall the app when a sink is slow, and it must not silently lose
everything either. The queue `capture` feeds is bounded, so a burst that outruns
the sink is dropped rather than growing memory without limit, and
`handler.dropped` counts how many so an operator can see it:

```python
async with capture(pipeline) as handler:
    ...
if handler.dropped:
    ...  # surface it; a silent drop reads as "we logged everything"
```

Raise `capacity` for more slack, or pass `capacity=None` for an unbounded queue
that never drops (trading away the bound). The policy is the app's to choose, the
same way `without-web` leaves response encoding to the app.

## Writing to a file, off the event loop

File writes are blocking, and under heavy logging that matters. The async file
libraries (`aiofiles`, `anyio`) hop to a worker thread *per operation*, so a busy
log stream pays a thread-pool round-trip on every write. The cheaper shape is a
*single* long-lived thread that owns the file and does all the I/O, fed by a
queue: the async side just enqueues, with no per-write hop.

`offload` is that bridge: a context manager that yields a `Sink`, running a
blocking `work` function on a dedicated thread. `work` is ordinary synchronous
Python that consumes the items; the yielded sink drops each onto a thread-safe
queue the worker drains. Items arrive in *bursts* (everything queued at that
instant), so the worker batches writes and flushes at each burst boundary, which
is exactly the moment it has caught up: under load bursts are large and flushes
few, and when idle each burst is a single line flushed at once. So durability
needs no flush-frequency knob; it falls out of the queue's own backlog.

The writers are named by *destination*: `to_rotating_file` writes to a file,
`to_stream` writes to a text stream the caller owns (`sys.stderr`, a socket).
There is deliberately no plain single-file writer, because an unbounded log file is
a footgun; you write to a rotating file and choose how it rotates. Both write
*strings*, not records (rendering a `Record` to text is the app's encoding boundary,
so it is a `from_map(Record -> str)` composed in front), own the `\n` framing, and
flush per burst; the difference is lifecycle: `to_rotating_file` opens, rotates, and
closes files it owns, while `to_stream` never closes the caller's stream:

```python
from datetime import timedelta

from without import compose, from_map, from_selector
from without_logging import Level, at_least, capture, offload, to_rotating_file


async def render(record):
    return f"{record.timestamp:%H:%M:%S} {record.level_name} {record.message}"


writer = to_rotating_file(
    lambda i, when: directory / f"app.{i}.log",   # (index, open time) -> path; 0 is the current file
    max_bytes=64 * 1024 * 1024,                   # rotate at 64 MiB, and/or ...
    max_age=timedelta(hours=1),                   # ... rotate hourly, whichever comes first
)
async with (
    offload(writer) as sink,
    capture(compose(from_selector(at_least(Level.WARNING)), compose(from_map(render), sink))),
):
    ...  # WARNING+ records are rendered and written on the worker thread
```

The `compose(processor, sink)` there is the core builder's terminal form: a
processor chain (the filter, the `Record -> str` render) prefixed onto a sink
yields a sink. Points to hold onto:

- **Nesting order.** `offload` goes *outside* `capture`, so the worker thread
  outlives the draining. On exit the queue is shut down so the worker drains what
  remains, flushes, and closes the file, then the thread is joined before the
  block returns.
- **Sink-only, first cut.** Bridging a full `Processor` onto a thread (an output
  queue as well) is much more stateful and writing does not need it, so it is out
  of scope. The queue is unbounded (the async side never blocks or drops, at the
  cost of memory if a stalled disk lets a burst accumulate); a bounded,
  drop-counting variant is a follow-up.

### Rotation lives in the worker

It is tempting to make rotation decoupled: rotate the writer on a stream of
external triggers. That works for *time* (a clock is independent of the writes)
but breaks for *size*, and the reason is the same one the stdlib's rotating
handlers bundle this. A size threshold is a function of the bytes written, so an
accurate size limit has to live in the write path; and once size is there, joint
size-and-time has to be too (a time rotation changes which file is current, so a
separate size trigger would be measuring a file no longer being written). That one
coupling is essential, not incidental.

So rotation lives in the worker, the one place with all the information:
`to_rotating_file` keeps an exact byte count (writing UTF-8 bytes directly rather
than polling a lagging on-disk size) and reads an injected clock, rotating on any
combination of `max_bytes` (size), `max_age` (a *relative* interval since the file
opened), and `schedule` (the next *absolute* wall-clock boundary), whichever trips
first. Everything *else* the stdlib handler fuses (formatting, filtering, flush
timing, transport) still composes out; only the genuinely-coupled size-and-time
decision is bundled, and unlike the stdlib's separate size and time handlers, one
writer does the joint case. The decision is made *before* each write, so a size
limit is not overshot (bar a single line larger than `max_bytes`), and it is
inlined and short-circuiting: the clock is read only when a size check has not
already forced the rotation and a time condition is set. `now` is injectable, so
time-based rotation is deterministic under test.

`schedule(t)` returns the next rotation boundary strictly after `t`, and the
writer rotates once `now()` reaches it. It is the pure, thread-synchronous form of
"a stream of rotation datetimes": the worker already owns the clock, so it
generates the stream itself rather than sampling an async one, which also means a
boundary missed while idle collapses to a single rotation and nothing goes stale.
`at_times` builds one from wall-clock times, so daily-at-midnight is a value:

```python
from datetime import time

from without_logging import at_times, to_rotating_file

writer = to_rotating_file(
    lambda i, when: directory / f"app.{when:%Y%m%d}.{i}.log",
    max_bytes=64 * 1024 * 1024,               # rotate at 64 MiB, or ...
    schedule=at_times(time(0, 0)),            # ... at midnight UTC, whichever comes first
)
```

## Where fan-out slots in (issue #13)

A real logging setup wants more than one sink: console *and* a file, or a file
*and* the network. That is fan-out, one record stream tee'd into several
independent sinks, and it is exactly the
[fan-out/fan-in connectors](https://github.com/JoshKarpel/without/issues/13)
(`tee`, `broadcast`, `merge`) that `without` core does not yet ship, having
removed them as speculative surface.

`without-logging` is the concrete use case that motivates bringing them back, and
it motivates the *hard* half, not just static `tee`:

- **Fan-out** (`tee` / `broadcast`): one `Record` stream into a console sink and a
  file sink at once.
- **Dynamic fan-in** (dynamic `merge`): app records, stdlib-captured records, and
  per-request sub-streams that appear and vanish, merged into one pipeline. A
  merge over a *changing* set of sources is the primitive issue #13 flags as the
  one the old static `merge` never covered.

Until those land, a `capture` block drives a single composed sink. That is the
honest state of the package: a single-sink pipeline today, and the concrete
pressure that turns issue #13 from speculative into motivated.

## What is deliberately not here

- **No formatter.** Rendering a `Record` to bytes (JSON, logfmt, a console line)
  is a boundary the app owns, so it is your `from_map(Record -> bytes)` ahead of
  the sink, not a shipped `format_json`. This mirrors `without-web` shipping no
  `json_response`.
- **No rotation, batching, or network sinks yet.** Each is an ordinary processor
  or sink you compose; the package ships the substrate (the value, the edge, the
  filter), not a batteries-included handler zoo.
- **No exception rendering yet.** `parse_record` carries the message but not the
  formatted traceback from `exc_info`; that is a known gap for a later pass.
