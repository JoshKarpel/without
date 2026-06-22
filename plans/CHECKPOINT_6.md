# Checkpoint 6

A snapshot of where `without` stands, succeeding `CHECKPOINT_5.md`. For the
original pitch see `BIG_IDEA.md`; for the critical review and open design
questions see `REVIEW_BIG_IDEA.md`.

## What changed since Checkpoint 5

This session reshaped the `kv` shell twice over. First it grew graceful
shutdown; then, prompted by the complexity that draining exposed, it was
rebuilt around a single structural move that dissolved most of that complexity
and, in passing, resolved the long-standing ASGI fork. The headline is in the
next section; the concrete changes:

- **Closable streams (`without` core).** `stream_from_queue` now ends when its
  queue is shut down: `queue.shutdown()` lets the queue drain its remaining
  items, then `get` raises `asyncio.QueueShutDown` and the stream ends, so a
  downstream `from_fold`/`from_sink` returns its final value. Shutting the queue
  down is the closable-stream signal. This is the primitive every graceful
  teardown below leans on, and it is the same primitive the deferred
  dynamic-merge connector will want.
- **`serve` is now a shared `consumer` plus a `session` per connection.** The
  generic line transport (`without_integration.kv.shell.serve`) takes two
  pieces: `consumer`, the single serial owner of shared state, and `session`, a
  per-connection processor factory. Each connection runs its session over its
  own line stream, so a connection's lifecycle *is* a stream's lifecycle (see
  the rationale below). The old `decode`/`encode` keyword codec moved into the
  session, which owns the protocol boundary.
- **`make_responder` split into `make_keyspace` + `make_session`.** The shared
  half (`make_keyspace`) is the keyspace `from_fold`, unchanged in spirit: every
  connection funnels into it and it answers each request on the channel it
  arrived with. The per-connection half (`make_session`) threads a per-connection
  request counter with `from_scan` and owns `parse_request`/`encode_reply`.
- **Connection-scoped state demonstrated alongside shared state.** Each reply
  line is now `"{n} {reply}"`: `n` is the connection-local request number
  (threaded in that session's `from_scan`), `reply` comes from the shared
  keyspace fold. A test pins the distinction: a second connection counts from 1
  (independent) yet sees the first connection's write (shared).
- **Config via a `Context`, showing off `without-env`.** `ServeConfig`
  (`BaseSettings`, `KV_` env prefix: `host`, `port`, `max_pending`,
  `drain_timeout`, `idle_timeout`) reaches `serve` through a
  `Context[ServeConfig]`. Production uses `EnvContext.load(ServeConfig)`; tests
  pass `EnvContext(settings=ServeConfig(...))` as a fixed-value context, the same
  `current()` seam with no signature change. (But see the open question on
  whether a *static* config needs the `Context` machinery at all.)
- **Graceful drain on shutdown within one global budget.** On exit `serve` stops
  accepting, then inside a single `asyncio.timeout(drain_timeout)` waits for
  accepted sessions to finish on their own (clients hanging up, the consumer
  answering their asks) and for the consumer to drain the `inbox`. If that whole
  phase overruns the one budget it hard-stops: `Server.close_clients()` (3.13)
  force-closes remaining clients, the consumer is cancelled, and any session
  still parked (e.g. awaiting a reply a stopped consumer will never send) is
  cancelled, so shutdown terminates in roughly `drain_timeout` without leaking a
  task. The budget is global (one timeout around the graceful phase), not applied
  per-step.
- **Idle-connection reaping.** A connection silent longer than `idle_timeout`
  (`None` disables it) is reaped on the same mechanism as everything else: the
  per-connection read is bounded by `asyncio.wait_for`, and a timeout just ends
  that connection's line stream, returning the session and closing the writer.
  This is the steady-state analogue of the shutdown straggler force-close, and a
  slow-loris defense.
- **One earned `except ConnectionError`.** An abortive client reset (RST) raises
  `ConnectionResetError` out of the session loop; it is contained so one rude
  client cannot poison the server. A test guards it by capturing loop exceptions
  and forcing GC (it fails without the handler).
- **All docstrings are Markdown now.** Per a convention codified this session,
  RST double-backticks were swept to single-backtick Markdown across every
  package.

## Rationale: a connection's lifecycle is a stream's lifecycle

This is the structural insight that simplified the shell, and it should headline
the model from here on alongside the shell framing and the narrow-waist bet.

The draining complexity in the first pass (an in-flight counter plus a
`quiesced` event per connection, hand-rolled "is this connection done" tracking)
was a symptom of a single design flaw: **one** fold over **one** never-ending
inbox. That fold's lifetime was the whole server's, so a connection's lifecycle
had no representation in the dataflow and had to be reconstructed by hand.

Give each connection its **own** processor over its **own** line stream and the
mismatch vanishes. `from_scan`/`from_sink` return when `async for event in
inputs` runs dry, and `stream_from_queue` now runs dry on `queue.shutdown()`, so
EOF ends the line stream, the session's processor returns, and the writer
closes. The stream's end *is* the "connection done" signal. The counter, the
event, the `is_closing()` checks, and even the "no `drain()` or one slow client
stalls everyone" trade-off all dissolve: requests on a connection are sequential
(await the reply before the next line), so a slow client stalls only its own
session.

## The state-placement rule

Two kinds of state live in the shell, and *where each lives* is the lesson:

- **Shared state (the keyspace)** lives in **one** serial `from_fold`
  (`make_keyspace`) that every connection funnels into through one bounded
  `inbox`. A fold pulls its next event only after the current step's coroutine
  completes, so the store's read-modify-write never interleaves even though
  `apply` may `await`: the sequential `async for` *is* the mutual exclusion. The
  store stays a threaded value, never a shared place behind a lock.
- **Connection-scoped state (the request counter)** is threaded in that
  connection's own `from_scan`. It is safe to keep local precisely because it is
  unshared, so connections run fully concurrently.

The rule: thread state *down* only when it is scoped to that level; funnel *up*
to a singular fold for anything shared. A connection reaches the shared core
through `ask` (put a request on the `inbox`, await the reply on its own
channel), which is contained I/O, the per-event analogue of ASGI's
receive/send and the actor model's ask-pattern (see the open question below). This is also where stateful-vs-stateless dictates shape: the
stateful processor (`from_fold`) is the singular, long-lived serial owner of its
state; stateless processors (`from_sink`/`from_map`) are plural, concurrent, and
ephemeral.

## The ASGI fork, resolved

Checkpoint 5's open question #2 framed a fork for HTTP: the KV server's *single
shared fold* (keyspace as a threaded value) versus *per-request processors plus
shared state behind them* in a lifespan-scoped `Context` (the ASGI shape, but
reintroducing a shared resource). The reshape shows the fork was a false choice.
You can have the ASGI per-connection (and, fractally, per-request) processor
shape **and** keep shared mutable state out of a place: the shared state lives in
a single serial fold that all the per-connection processors funnel into, instead
of a locked resource in a lifespan `Context`. Same concurrency, no lock, state
stays a value. The HTTP step inherits this directly: server handles a stream of
connections, each connection a processor over its stream of requests, each
request (for bodies) a processor over its stream of events, with shared
application state funneled to singular folds rather than parked in places.

The honest alternative (a per-connection coroutine that reads, mutates a shared
store, and writes in one call stack, like a `socketserver` handler but async)
was considered and rejected: it is simpler only because it abandons the stream
model and turns the store into a shared place needing a lock. The value of the
`without` version is that the same smoothness is reached *while keeping* the
stream-processing core.

## The 2x2 builder matrix (carried forward, now both halves in use)

The core names every combination of two axes: does the step thread **state**, and
does the processor **emit a stream** (`Processor`) or **terminate** (a leaf)?

|               | emit a stream (`Processor`) | terminate (a leaf)        |
|---------------|-----------------------------|---------------------------|
| **stateless** | `from_map`                  | `from_sink`               |
| **stateful**  | `from_scan`                 | `from_fold`               |

The `kv` shell now exercises both stateful builders against real shared/local
state: `from_fold` for the singular keyspace, `from_scan` for the per-connection
counter. `Sink`/`Fold` umbrella types and `stream_from_queue` are unchanged.

## Workspace layout

A `uv` workspace of flat, version-locked packages. The root
(`without-workspace`) is a virtual project holding shared dev tooling and config.

- `packages/without` (import `without`): the core. `contracts` (`Processor`,
  `Stream`, `Context`, `Transition`, `Sink`, `Fold`, and the four builders),
  `wiring` (`Sample`, `pipe`, `sample`, `distribute`, `tee`, `broadcast`,
  `route`, `merge`, `stream_from_queue` — the last now ends on `queue.shutdown()`),
  `tasks` (`background_task`), and `testing` (`stream`, `collect`, `tick`). Zero
  runtime dependencies.
- `packages/without-env` (import `without_env`): a static `Context` parsed from
  environment variables with `pydantic-settings`. Now also drives the `kv`
  server's `ServeConfig`.
- `packages/without-configmap` (import `without_configmap`): a behavior source
  backed by a Kubernetes ConfigMap mount.
- `packages/without-integration` (import `without_integration`): depends on
  `without` and every plugin; holds cross-plugin tests and the `kv` toy server,
  split into `kv.core` (pure keyspace) and `kv.shell`. The shell now exposes
  `serve` (shared `consumer` + per-connection `session`), `make_keyspace`,
  `make_session`, `ServeConfig`, and the `Connected`/`Send`/`Ask`/`MakeSession`
  types. Not meant to be distributed.

## Status

Done and verified (mypy strict clean across 22 source files, 58 tests passing,
ruff lint + format clean):

- Closable `stream_from_queue` (ends on `queue.shutdown()`).
- The `kv` shell rebuilt around per-connection sessions over per-connection line
  streams, with the keyspace as a single serial `from_fold` and a per-connection
  request counter as a `from_scan`, demonstrating the state-placement rule.
- Bounded graceful shutdown within one global `drain_timeout` budget, plus
  idle-connection reaping (`idle_timeout`), driven by `ServeConfig` through a
  `Context`. Tests cover: half-close reply delivery, draining an in-flight
  backlog, bounded shutdown against an idle client, no task leak when the
  consumer wedges, idle reaping (and that it leaves an active client alone), and
  containment of an abortive client reset.

## Open questions and next steps

1. **HTTP / ASGI** (the demanding validation). The fork is resolved (see above);
   what remains is making it concrete: a connection processor that spawns a
   per-request processor over that request's event stream (the third fractal
   level), HTTP framing in a `core`-style pure layer, and shared application
   state funneled to singular folds. The `send`-down / `ask` shape is
   already in place.

2. **Relationship to the actor model (resolve and understand).** The `kv` shell
   keeps reinventing actor-model vocabulary, and the naming this session made it
   explicit: the keyspace `from_fold` is a single-mailbox actor (one serial owner
   of state processing a queue of messages), the `inbox` is its mailbox, `ask` is
   the ask-pattern (request, await reply), and a bare `send` with no reply would
   be `tell` (fire-and-forget). The per-connection sessions are themselves smaller
   actors with their own local state. The state-placement rule ("shared state
   lives in one serial owner you message; local state is threaded where it is
   scoped") is essentially the actor discipline restated in stream terms.

   What to resolve: is `without` an actor framework wearing stream-processing
   clothes, or does the stream framing buy something actors do not? Candidate
   distinctions to pressure-test: (a) `without`'s state is a *threaded value*
   through a fold rather than mutable cell(s) hidden in an actor, so "no shared
   place" is structural, not a convention; (b) processors compose as stream
   transforms (`pipe`/`distribute`/`tee`/`merge`) where actors compose only by
   addressing each other; (c) backpressure is end-to-end via bounded streams
   rather than unbounded mailboxes. Conversely, where the toy *does* reach for a
   raw `asyncio.Queue` funnel and per-connection reply channel, it has effectively
   hand-rolled mailboxes and addresses, so the boundary is not yet clean. Decide
   whether to (i) lean in and name the actor concepts as first-class `without`
   vocabulary, (ii) stay deliberately stream-first and treat the actor resemblance
   as a consequence, or (iii) articulate precisely why the two differ. This bears
   directly on the dynamic-merge connector and the funnel/`inbox` primitive below,
   which are exactly the "mailbox" machinery an actor framing would name.

3. **Does a *static* `Context` (and `without-env`) pull its weight?** Because the
   shell leans on closures, `serve` reads `settings = config.current()` exactly
   once and then closes over `settings` everywhere (`handle`, `input_lines`,
   `ask`). For a value that never changes, that closure capture is the whole
   story: the `Context`/`.current()` indirection bought nothing a plain captured
   value would not, so `EnvContext` is arguably ceremony for static config. The
   abstraction earns its keep only when the value changes *dynamically*: that is
   where `.current()` must be re-read at the point of use (you cannot capture it
   once), which is exactly what `without-configmap` is for. So the live question
   is whether static config should just be a plain value passed in, with `Context`
   reserved for dynamic sources. Note the nuance: even a dynamic context is still
   *injected* via a closure (you close over the context and call `.current()` at
   the use site); what differs is whether you capture the value or the context.
   This lines up with the configuration rule (load static config once at the
   boundary; watch/refresh only what must change without a restart).

Smaller candidates carried forward:

- A **dynamic-merge** connector for a changing set of sources (the server's
  dynamic connection set; the `inbox` funnel is the current stand-in). It would
  also subsume the closable-stream machinery cleanly.
- Whether `serve`'s `Fold[..., object]` consumer parameter deserves a named
  `Leaf` umbrella type.
- Factor the generic connection-set + bounded-drain orchestration out of the toy
  into a reusable `without` piece, so the shell stops hand-rolling teardown.

Deferred, to bring back deliberately (carried forward):

- **Graph/DAG recovery and visualization**, rebuilt on `graphlib`. The
  contained-effects relaxation weakens static dependency visibility.
- Known-hard dataflow/FRP problems: glitches on diamond dependencies, feedback
  cycles, and teardown order.
- `without.testing.tick` advances the loop a single step; the deterministic fix
  is an explicit "await next update" signal on a sampled `Context`.

Documentation debt (carried forward): `BIG_IDEA.md` and the earlier checkpoints
still describe the model as an "async reducer"; it is more accurately an **async
scan**, and the functional-core / imperative-shell framing plus the
connection-as-stream insight above should be folded into `BIG_IDEA.md`'s
rationale when it is next revised.
