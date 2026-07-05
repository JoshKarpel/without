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
- The exception, if any, is captured here too (`exception`, a
  `TracebackException`, `None` when there is none). A live traceback is a
  *place*, not a value: it pins its frames and their locals in memory and cannot
  ride the queue to another thread (stdlib's own `QueueHandler` flattens it to a
  string for exactly this reason). So it is turned into a value at the edge,
  while it is still live. But unlike the message, which has one sensible
  rendering, a traceback has many (locals, chaining, style), so this keeps the
  *structure* rather than pre-rendering a string: the app formats it however it
  likes downstream (`"".join(record.exception.format())` for the stdlib text, or
  walk `record.exception.stack` for JSON frames), the same encoding boundary it
  owns for the record as a whole. A record logged with `logger.exception(...)` or
  `exc_info=True` therefore stays a self-contained value with nothing live
  attached, and the traceback format is still yours to choose.
- `fields` is every attribute outside stdlib's standard envelope (`RESERVED`),
  which is exactly what `logging`'s `extra=` merges in, exposed read-only.
- `level` stays a plain `int`. A `Level` `IntEnum` exists for writing readable
  predicates (`at_least(Level.WARNING)`), but the record carries the number, so a
  third-party library logging at a non-standard numeric level is carried through,
  not rejected. This is the "fail loud for the author, recover for the remote"
  rule: a library's odd level is not ours to reject.

### What `parse_record` deliberately drops, and how to keep it

A `LogRecord` carries more than the above: source location
(`pathname`/`lineno`/`funcName`), and `thread`/`process`/`taskName` identity. It
is tempting to read the default parser as *losing* these, but the line is
deliberate. Those attributes are not caller-supplied content the way `msg`,
`exc_info`, and `extra=` are: they are synthesized by stdlib's (swappable)
`LogRecord` factory, gated by module flags (`logThreads`, `logProcesses`,
`logAsyncioTasks`), so any of them may be `None` or absent, and they exist to feed
the *formatter's* `%(pathname)s` / `%(thread)d` placeholders rather than as a
guaranteed per-event contract. Promoting them to typed `Record` fields would couple
the value to stdlib factory internals and force every field to model "maybe
missing". `created` is the one factory value kept as `timestamp`, because it is
always present and unconditional.

So `parse_record` promises only the stable, caller-supplied core, and the ambient
metadata is opt-in through `capture`'s injectable `parse=`, lifted into `fields`
where a caller who wants it (and whose factory provides it) can put it:

```python
import logging

from without_logging import capture, merge_context, parse_record


def parse_with_source(log_record: logging.LogRecord):
    return merge_context(
        parse_record(log_record).with_fields(
            file=log_record.filename,
            line=log_record.lineno,
            func=log_record.funcName,
            task=log_record.taskName,
        )
    )


async with capture(pipeline, parse=parse_with_source):
    ...  # every record now carries file/line/func/task in its fields
```

`capture`'s default parser already composes `merge_context` on top of
`parse_record` (see call-site context below). `merge_context` is a plain
`Record -> Record`, so a custom parser composes it the same way to keep bound
context; drop it to opt out.

### Call-site context: bind at the edge, not downstream

structlog's most-used feature is binding per-request context: `log.bind(request_id=...)`,
or `bind_contextvars(...)` merged by a first processor. The reason it works is
timing: that merge runs *synchronously, in the caller's execution context*, at the
moment of the log call.

`without-logging` splits the timeline. The pipeline processors run in the *sink
task*, after the record has crossed the queue, so they have left the caller's
context: a task-local `ContextVar` set around a request reads its default there,
and `current()` samples a single *shared* behavior (fine for config or a sampling
rate, but there is no one "current request" in a concurrent server). So call-site
context cannot be recovered by a processor. It is not gone, though: the
`CaptureHandler`'s `emit` (and thus the parser) runs synchronously on the
*caller's* task, exactly where structlog's merge runs, so that is where it is
read. `bind` writes the context and `capture` reads it by default:

```python
from without_logging import bind, capture

async def handle(scope):
    with bind(request_id=scope["request_id"]):   # scoped to this block, on this task
        log.info("handling")                     # emit runs here; the bind is read here
        ...                                       # every log call inside carries request_id


async with capture(pipeline):   # default parser merges the bind, so it just works
    ...
```

`bind(**fields)` is a context manager, so the scope is explicit and lexical: the
fields apply inside the block and are gone after, with nothing left mutated.
Nested binds accumulate and unwind in step. It writes a task-local `ContextVar`
that `merge_context`, a plain `Record -> Record`, folds onto each record. `capture`'s
default parser composes it on top of `parse_record`, at the edge where the bind is
still live, so `bind` needs no wiring to take effect. A field the call sets via
`extra=` wins over a bound one of the same name, so `bind` supplies defaults rather
than overriding. To add other edge enrichment and still pick up binds, compose
`merge_context` on top of your own parser (see the source-location example above).

This is where the shape of the whole package comes into focus: there are **two
enrichment phases, split by the queue**, and `capture(sink=..., parse=...)` is one
handle on each. The **emit phase** runs *synchronously* in the handler's `emit`,
on the logging call's own task, because that is the shape stdlib hands us. It
composes `Record -> Record` steps by ordinary function composition
(`merge_context(parse_record(log_record))`), and it is the only phase that can read
call-site context, because it still holds the caller's task. The **sink phase**
runs *asynchronously*, draining the queue as a `Stream`: it composes `Processor`s
with `compose` (`add_fields`, filters, renders), may `await` I/O, and samples
shared behaviors via `current()`.

|  | Emit phase | Sink phase |
| --- | --- | --- |
| when | before the queue | after the queue |
| runs | synchronously, in `emit`, on the caller's task | async, draining the queue as a `Stream` |
| enrichment | `Record -> Record` by function composition | `Processor` by `compose` |
| can | read call-site context (contextvars) | `await` I/O, sample shared behaviors via `current()` |
| `capture` handle | `parse=` | `sink=` |

The sync-vs-async difference is not an asymmetry to paper over; it is each phase
wearing the color of where it runs, on either side of the queue. So the rule for
*where* a piece of enrichment goes falls out of what it needs: call-site context
(a bind) before the queue, in the sync emit phase; async I/O and shared behaviors
after it, in the streaming sink phase. `merge_context` is a sync `Record -> Record`
precisely because its whole job, reading the bound `ContextVar`, only means
anything in the emit phase.

Because the value is stamped into the `Record` (an immutable value) at the edge, it
then rides the queue correctly per-record: two requests interleaving on different
tasks each get their own `request_id`, with no shared place to race on, cleaner
than the mutable-`ContextVar` read structlog does downstream. What
`without-logging` does *not* wrap is a logger object that carries context
(`log.bind(...)` returning a new logger); binding is scoped by the `with` block
instead of by a logger's lifetime.

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
across *several* sinks is the other direction, and it stays a wiring concern:
`tee` from `without` core; see below.

Enrichment, by contrast, *is* count-preserving, so `add_fields` is a plain
`from_map`. Enriching from a *shared behavior* (a value the whole pipeline sees
the latest of: the current config revision, a sampling rate) is the same shape,
reading it via `current()` inside the step. Per-call-site context (a request id)
is a different matter, and has its own section below.

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
    line = f"{record.timestamp:%H:%M:%S} {record.level_name} {record.message}"
    if record.exception is not None:                  # format the captured traceback here
        line += "\n" + "".join(record.exception.format()).rstrip("\n")
    return line


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

## Rendering: `render_json` and `render_console`

Rendering a `Record` to text is the app's encoding boundary, so the core ships no
*mandatory* formatter. But the two everyone reaches for come as *optional*,
opt-in `Processor[Record, str]` helpers you compose in front of a string sink:
`render_json()` for structured logging and `render_console()` for a
human-readable line. Both surface the record's `fields` (the structured `extra=`
data, plus anything `add_fields`/`bind` enriched) as first-class output:

```python
import sys

from without import compose
from without_logging import capture, offload, render_console, render_json, to_stream

async with (
    offload(to_stream(sys.stdout)) as out,
    capture(compose(render_json(), out)),   # one JSON object per line
):
    ...
```

A renderer is a `Processor[Record, str]`, so it runs *on the event loop*, as part
of the sink-phase pipeline, while `offload`'s worker thread does the blocking
*write*. Under CPython's GIL that is the right split: a file or socket write
releases the GIL, so offloading it lets the loop run during the I/O, whereas
rendering is CPU-bound Python that holds the GIL and so gains nothing from a
thread. (Moving rendering onto the worker thread becomes a real parallelism win
only under a free-threaded build; it is a noted follow-up, not needed today.) An
async sink is the same on the loop without a thread at all: `compose(render_json(),
network_sink)`, since its `await`ed I/O yields the loop on its own.

`render_json()` puts the envelope (`timestamp`, `level`, `logger`, `message`) and
every field flat at the top level, where a log aggregator can index them, and the
envelope wins a name clash so a field cannot shadow `level`. It leans on the
structured exception: because `Record.exception` is a `TracebackException` rather
than a pre-rendered string, the traceback is encoded as *data*, not an opaque blob.
So `log.exception("charge failed", extra={"order_id": "ord-7788"})` inside a
`raise ValueError(...) from KeyError(...)` renders to:

```json
{
  "timestamp": "2026-07-05T14:56:09.468581+00:00",
  "level": "ERROR",
  "logger": "billing",
  "message": "charge failed",
  "order_id": "ord-7788",
  "exception": {
    "type": "ValueError",
    "message": "ValueError: token expired",
    "frames": [{"file": "app.py", "line": 49, "function": "charge", "code": "raise ValueError(...) from missing"}],
    "cause": {
      "type": "KeyError",
      "message": "KeyError: 'region'",
      "frames": [{"file": "app.py", "line": 47, "function": "charge", "code": "raise KeyError('region')"}]
    }
  }
}
```

The `cause` follows Python's own chaining rule (an explicit `raise ... from`, else
the implicit context unless suppressed). How the exception is encoded is *the
caller's* choice, injected: `render_json(exception=exception_to_text)` emits the
flat traceback string instead of structured frames, and any
`TracebackException -> <json value>` works. The timestamp is injectable the same way
(`render_json(timestamp=lambda when: when.timestamp())` for an epoch number; default
`iso_timestamp`), and `exception_to_dict`/`exception_to_text` are exported so a
custom schema can reuse them. (Non-serializable *field* values coerce with `str`,
not `repr`: JSON is machine-consumed, so a clean indexable value wins, and `str`
still degrades to the `repr` form for an object with no `__str__`.)
`render_console()` takes the *same* record and instead emits
`TIMESTAMP [LEVEL] logger: message key=value ...`, appending stdlib's own
multi-line traceback (`"".join(record.exception.format())`) when present. One
value, two encodings, both the app's choice: this is exactly why the exception is
captured as structure and not flattened to a string at the edge.

## Fan-out to several sinks

A real logging setup wants more than one sink: console *and* a file, or a file
*and* the network. That is fan-out, one record stream split into several
independent sinks, and it is [`tee`](../reference/without.md) from `without`
core. `tee(*sinks)` returns a single `Sink` that feeds every event to every
branch, consuming the input exactly once, so there is one `capture` and one pass
through the shared middleware.

The split point is yours to place. Whatever you `compose` *before* the `tee` is
the shared prefix (parsed and enriched once); each branch is its own `Sink`,
carrying its own filtering, rendering, and terminal:

```python
import logging
import sys

from without import compose, from_selector, tee
from without_logging import (
    Level, add_fields, at_least, capture, offload,
    render_console, render_json, to_rotating_file, to_stream,
)

async with (
    offload(to_stream(sys.stderr)) as console,
    offload(to_rotating_file(name, max_bytes=64 * 1024 * 1024)) as file,
    capture(
        compose(
            add_fields(service="api"),                        # shared: parsed + enriched once
            tee(
                compose(render_console(), console),              # console: everything, human-readable
                compose(                                          # file: WARNING+, as JSON
                    from_selector(at_least(Level.WARNING)),
                    compose(render_json(), file),
                ),
            ),
        ),
        level=logging.DEBUG,
    ),
):
    ...
```

`parse_record` runs once and `add_fields` runs once; then the stream splits. The
console branch renders every record with the shipped `render_console`, the file
branch keeps `WARNING`+ and renders `render_json` (each branch owns its encoding;
a custom `from_map(Record -> str)` slots in the same place). Each branch owns its own worker
thread through its `offload`, so a slow disk never blocks the console. A branch
MAY itself be another `tee`, so "one input, several sink groups" nests. With one
`capture` at `DEBUG`, the per-branch `from_selector` does the level filtering, so
each sink keeps exactly what it wants without a second pass over the records.

The *fan-in* half is still ahead. `tee` covers splitting one stream; merging
several back (app records, stdlib-captured records, and per-request sub-streams
that appear and vanish, folded into one pipeline) wants a `merge` over a
*changing* set of sources, the primitive
[issue #13](https://github.com/JoshKarpel/without/issues/13) flags as the one the
old static `merge` never covered. `broadcast` (fan-out-*fan-in*, a `tee` whose
branches emit and are merged back into one stream) is reserved there for the same
day. Neither is needed to drive several sinks today.

## What is deliberately not here

- **No *mandatory* formatter.** Rendering a `Record` to text is a boundary the app
  owns, composed as a `from_map(Record -> str)` ahead of the sink, never forced by
  the core path (mirroring `without-web` shipping no `json_response`). The two
  common choices, `render_json` and `render_console`, ship as *optional* opt-in
  renderers you compose yourself; logfmt, msgpack, or a bespoke schema stay a
  `from_map` you write.
- **No rotation, batching, or network sinks yet.** Each is an ordinary processor
  or sink you compose; the package ships the substrate (the value, the edge, the
  filter), not a batteries-included handler zoo.
- **No formatted-exception convenience, and no `stack_info` yet.** `parse_record`
  captures the `exc_info` traceback as a structured `TracebackException` on
  `Record.exception`, so formatting it (text or JSON) is your downstream
  `from_map`, mirroring the no-formatter stance above. The separate call-stack
  capture from `stack_info=True` is still dropped; folding it in is the same move
  on a rarer field.
- **No thread-side rendering.** A renderer runs on the loop (see above), which is
  the right split under the GIL: only the blocking write benefits from a thread,
  and rendering is GIL-bound CPU. Under a free-threaded build, rendering *inside*
  the `offload` worker would parallelize; that wants a sync `Record -> str` core
  plus a "compose for the worker world" combinator (the offload analog of
  `compose(from_map(render), sink)`), an additive follow-up when free-threading
  makes it pay.
