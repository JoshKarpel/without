# The Philosophy of `without`

A synthesis of the ideas the project has actually committed to in code, drawn
from the project's design history and the shipped packages (`without`,
`without-asgi`, `without-http`, `without-web`, `without-env`, `without-configmap`,
and the `integration` toys). It describes the philosophy *as implemented*, not an
aspirational manifesto. Where the code and an older framing disagree, the code
wins.

The throughline, if you read nothing else: name the smallest substrate that makes
independent pieces compose, keep the core pure and the shell thin, prefer values
to places, and push every boundary decision out to the application that owns it.

## The bet: name the substrate

Python has many frameworks with similar-but-incompatible shapes: ASGI apps,
Click commands, Kafka consumers, asyncio protocols, Airflow operators. They do
not compose because none of them names the layer they share. `without`'s wager
is that if you name that layer precisely enough to write down as types, the
pieces snap together.

This reframes the project from a framework into a *contract*, a narrow waist,
closer to ASGI or WSGI than to FastAPI. The original thesis ("anything can be
modeled as a stateful stream processor") is vacuous on its own, the way "anything
is a Turing machine" is vacuous. The vacuousness is the point: a single shape
general enough to host everything is exactly what buys interoperability the
ecosystem lacks. The success metric is not expressiveness but composition: can a
k8s-ConfigMap context and an HTTP stream, written independently and ignorant of
each other, be wired together. The contract is the project; everything else is a
plugin.

## The substrate: streams, processors, contexts

Three types carry the whole model (`without.contracts`):

- A `Stream[T]` is an asynchronous sequence of values. It is the one shape every
  connection takes, whoever does the I/O. A socket, a file watcher, a clock, and
  an in-memory list are all just streams.
- A `Processor[In, Out]` transforms a stream of inputs into a stream of outputs.
  It is the only node type and the only thing a user writes. One processor's
  output stream is another's input stream, all the way down.
- A `Context[T]` is a stream viewed as its latest sampled value. Where consuming
  a stream sees *every* event, `current()` reads the *latest* and never blocks.
  A context is never "not ready"; it always hands back a value, never a writable
  place.

The split between `Stream` and `Context` is Conal Elliott's distinction between
*events* (every occurrence matters) and *behaviors* (only the current value
matters), and it is load-bearing. Long-lived state (config, a connection pool)
is not a special kind of object: it is just another processor's output that a
reader *samples* instead of *consumes*. The question "is the held state a
processor or a context?" dissolves: it is a processor; "context" names how a
reader connects to it.

## Processors all the way down: no privileged executor

An early sketch had an `Executor` type as the thing that ran everything. The
design deliberately dissolved it. There is a runtime that supplies impure source
streams and runs the loops, but it is a thin interpreter of the wiring, not a
concept the user models with, so it gets no type and no peer status next to
`Processor`. Homogeneity of *interface* (everything is stream-to-stream) is the
goal; homogeneity of *implementation* (every node may do I/O) is explicitly not,
because that would throw away the testability that is the point.

## Functional core, imperative shell: `without` is a way to write a shell

One primary reading of the project, co-equal with the narrow-waist bet: `without`
is a *principled way to write an imperative shell*. The functional-core /
imperative-shell split is not an implementation tactic layered on top; it is what
the library is *for*.

You write domain logic as a pure core: steps lifted into processors by the
builders. `without`'s connectors, sources, leaves, and output shapes are the
vocabulary for assembling the shell that runs that core against the world. The
`integration` toys are built to make this concrete: `kv.core` is a pure keyspace
(parse, fold, encode) and `kv.shell` is a generic line server that runs it;
`transform.core` is pure and HTTP-unaware while `transform.app` is the ASGI
shell that owns the bytes. The sharpest demonstration is `transform.cli`: a
second shell over the *same* unchanged core, drawing config from environment
variables instead of a ConfigMap, reading stdin instead of sockets. Only the I/O
at the edge and the config source differ. That portability is the narrow-waist
payoff the project is chasing.

## Sans-IO: I/O is decoupled, not forbidden

Sans-IO (proven by `h11`, `h2`) is the testability lever, and it maps cleanly
onto functional-core / imperative-shell: the processor is the pure core, the
runtime is the shell. The interior of the graph is pure stream-transformers;
only the source streams at the edge touch the world.

But the rule is *decoupled, not forbidden*. A processor's step is `async` and
MAY `await` I/O while handling an event (a database query, a closed-lifespan
sub-request), reading its dependencies from injected `Context` values. The point
is not to ban effects but to separate them into the right abstractions so the
parts stay reusable: sources at the edge, behaviors via `sample`, effects
contained inside a step. The one discipline an effect must keep: it MUST complete
within the step and MUST NOT escape the entrypoint. A processor awaits its I/O to
completion and never hands a half-open resource (an open socket, a task it does
not own) back to the runtime. Testing then needs no mocks: inject fake `Context`
values and feed a `stream_from_iterable(...)` of inputs.

## Values over places, and where state goes

The deepest commitment is Rich Hickey's values-over-places, and it is what most
sharply distinguishes `without` from the actor model it superficially resembles.

A `Transition` is a value, never a place: a step returns the next state and the
output it emits, mutating nothing the caller can observe. A `Context` hands
readers a value through `current()`, never a writable cell. State threads
*through* a fold as a value rather than living in a mutable location reached by
reference.

This yields the **state-placement rule**:

- Thread state *down* only when it is scoped to that level: a per-connection
  counter belongs in that connection's own `from_scan`.
- Funnel state *up* to one singular serial `from_fold` for anything shared: the
  keyspace, the todo list. A fold pulls its next event only after the current
  step completes, so its read-modify-write is serialized without a lock, even
  across `await`s, and the shared state stays a value rather than a place.

This resolves a false choice the design first posed (one shared serialized fold
*versus* per-request processors with shared mutable state). You can have the
per-connection (and fractally per-request) processor shape *and* keep shared
mutable state out of any place, by funneling it into one serial fold that all the
per-connection processors message. Same concurrency, no lock, state stays a
value.

The actor resemblance is real but it is a *consequence*, not the foundation. An
actor is a derivable pattern (a fold whose input is a dynamic merge of every
sender's stream), not a primitive. The decisive difference is exactly
values-over-places: an actor *is a place* (identity, an address, a hidden mutable
cell you reach by reference), whereas `without` threads state as a value, rides
the reply target in the value, and composes structurally. Both serialize
mutation through one queue, which is why they rhyme; they differ on whether the
serial owner is a place you address or a value you compose. `without` borrows the
mailbox, not the supervision/fault model, so calling it an actor framework would
over-claim.

## The builders: one small 2x2

Processors are built, not subclassed. The core ships four builders that cover a
2x2 of stateful-vs-stateless and emitting-vs-terminal:

- `from_map(step)`: stateless, emits one output per event.
- `from_scan(initial, step)`: stateful, threads state and emits one output per
  event (a *scan*, not a reduce).
- `from_sink(step)`: stateless terminus, consumes a stream for effects, emits
  nothing.
- `from_fold(initial, step)`: stateful terminus, threads state and yields only
  the final accumulated value when the stream ends (a true reduce).

A scan emits at every step; a fold collapses to one value at the end. This
distinction matters: an early framing called the model an "async reducer," but
the per-event processor is an async *scan*. The fold is the serial owner of
shared state; the scan is the per-connection processor.

The 2x2 fixes output cardinality at one-per-event (or a terminal collapse), but
the `Processor` protocol itself imposes no such bound: a step is free to emit
zero or many. Two builders take up the zero-or-one case, added when a shipped
package (`without-logging`) needed to filter a stream: `from_selector(keep)`
re-emits the events matching a predicate and `from_filter(reject)` drops them,
polarity-duals of each other. They carry no new machinery (no queue, no
background task), which is why they are builders and not wiring; dropping an event
needs neither. Their predicate is `async` like every other builder step, keeping
one color of function throughout: a decision that must `await` I/O composes with a
pure one that never does. Emitting *several* outputs per event is the other
direction, and it stays deliberately a wiring concern, because splitting one input
across many consumers is where the queue-and-background-task complexity lives:
fanning a stream out to several terminal sinks ships as `tee`, and the mid-stream
fan-out/fan-in family stays in issue #13.

## How processors connect

Wiring (`without.wiring`) is deliberately small. The load-bearing event-edge
connector is `compose`: it chains one processor into the next and is pure
composition, the only connector that needs nothing running. The other half of the
model is the **behavior edge**, `sample`, which exposes a stream's latest value as
a `Context` (latest-wins, no backpressure). Around those sit the source and
terminal adapters: `stream_from_iterable` lifts a fixed iterable into a `Stream`, `collect`
drains one to a list, and `stream_from_queue` adapts a push source (an accept
loop, a callback client) into the pull-based stream the rest of the system
consumes.

`compose` aside, a connector that needs a running task is scoped by
`background_task`, a `with`-block helper that starts the task on entry and
cancels-then-awaits it on exit, so nothing leaks past its block. `sample` is the
canonical one: it is where a stream becomes readable state. Closability is
signalled structurally: shutting down a queue (`queue.shutdown()`) ends the
stream it feeds, which lets a downstream fold return its final value.

One fan-out connector earns its place: `tee` splits a stream across several
terminal `Sink` branches, each with its own tail, so a shared prefix runs once and
every branch consumes its own copy. `without-logging`'s need to drive a console and
a file at once is the concrete use case that motivates it. The rest of the
fan-out/fan-in cluster (`distribute`, `broadcast`, `route`, `merge`) stays out of
the core: no shipped package needs it, so it would be speculative surface carrying
real queue-and-background-task complexity, and the design is recorded for when a
concrete need calls for it (see issue #13).

## Lifespan as a variable: a connection's lifecycle is a stream's

The non-obvious unification at the heart of the project: an HTTP-request handler
and a Redis replica are the *same shape* with different state lifespans. Naming
that shape (a stateful stream processor) names a *what* independent of any *how*.

This plays out directly at the HTTP boundary. A server is a stream of
connections; a connection is a stream of requests; a request is a stream of
events. Each connection is its own `Processor` over its own stream, and the
connection's lifecycle *is* the stream's lifecycle: EOF ends the stream, the
processor returns, the writer closes. This dissolved a pile of hand-rolled
"is-this-connection-done" bookkeeping. ASGI's `application(scope, receive, send)`
maps onto it exactly: a long-lived processor that spawns a short-lived processor
per request. Shared app state is funneled to a singular fold, per the
state-placement rule.

## The app owns the boundary

A recurring decision, applied in several places: the framework produces the typed
value and leaves the boundary encoding to the application.

- `without-web` ships no `json_response`/`text_response`. The router produces a
  `Response` (status, headers, bytes); the serializer, key ordering, and content
  type are the app's choice. The encoding-agnostic `buffered` adapter stays
  because it *routes* a `Response`, it does not *build* one.
- OpenAPI schema generation is injected (`schema_for`), so the core stays
  agnostic to the schema library (pydantic, a dataclass walker, a raw mapping).
- Exception-to-response mapping has no registry. `catching(recover)` is plain
  middleware; `recover` is the app's ordinary
  `(Exception) -> Awaitable[Response | None]` function, written as a `match` that
  narrows each case to its real type. The framework owns only the commit-point
  guard; the policy is the app's.

The test for a proposed helper: does it bake in a boundary decision (a serializer,
a content type, an error policy) the app should own? If so, keep the core
producing the typed value and push the decision out, or make it injectable.

## Parse, don't validate, at every layer

Alexis King's principle runs through `without-web`. A `Converter` is a `str ->
value` parser paired with the JSON Schema it parses into; rejecting a segment
(raising `ValueError`) makes the trie walk backtrack rather than erroring a
handler. An `Extractor[V]` is parsing-as-a-value: a pure `Request -> V` paired
with the OpenAPI fragment it contributes. Path params arrive at a handler already
typed (`user_id: int`), with no `assert isinstance` and no runtime introspection;
the extractor types are tied to the handler's parameters through an overload
ladder, so a mismatch is a mypy error.

This composes into a layering rule: **whichever layer parses or produces a value
is the single source of truth for that value's schema.** The router owns method,
path, and path params; the handler owns query, headers, body, and responses, each
carried by the extractor that parses it. OpenAPI is then a *merge* of those
self-descriptions recovered from structure, not a blob declared in one place. The
description is recovered from the code, not maintained alongside it.

The same role-not-shape rule governs defaults: a type the parser fills from
outside input (inbound) carries *no* defaults, so a field the parser forgot fails
loudly instead of silently defaulting; a type the app constructs (outbound)
carries defaults for ergonomic construction. Even when two types have identical
fields, they are modeled separately because they hold opposite invariants:
`RequestBody` (inbound, no defaults) versus `ResponseBody` (outbound, defaulted),
and the HTTP client's `ResponseHead` parsed from the wire versus the server's
`ResponseStart` the app builds.

## Fail loud for the author, recover for the remote

Strictness belongs at the trust boundary, not only the parse boundary. Two kinds
of unexpected value reach the code, and they earn opposite treatment.

A value the **application author** generates and hands inward (an outbound
`Response`, a typed event the app builds, a handler's return, a config value) is
under their control, so an unexpected one is a bug in code they own. The
framework fails loud: an exhaustive `match` closes with `assert_never`, an inbound
parser that forgot a field raises, an unsupported outbound extension raises
`NotImplementedError`. Crashing surfaces the bug where it can be fixed.

A value **received from a remote client over the network** (a half-framed `h11`
event, an unexpected `wsproto` frame, a DATA frame for a stream no longer tracked)
is controlled by neither the framework nor its user. Hard-failing would let any
peer take a connection down, so the network-receive side recovers instead: it
logs at `warning` so an operator sees that something off happened, then degrades
gracefully (treat it as a disconnect, drop the stray frame, close the single
connection) rather than raising.

This is why the `assert_never` exhaustiveness guards cluster in the `encode_*`
functions, which match over our own sealed unions built from values we
constructed, while the recover-and-log paths cluster in `without-http`'s
`h11`/`h2`/`wsproto` handling, which decodes raw remote bytes. The parse boundary
decides *where* raw input becomes a typed value; the trust boundary decides *how*
to react when that input breaks the contract: same defensive guard, opposite
verdict, settled by who authored the value. It pairs with the rule that a
valid-but-unhandled event from a peer (a new event kind) is not a fault to crash
on but something to ignore, with a log only when it is genuinely unexpected.

## Library, not framework: control flow stays visible

The north star is that user control flow is plain, visible Python. The package
should read like a library you call, not a framework that calls you.

- Assembly is explicit and declarative. `@get(pattern, ...)` *returns a `Route`
  value*; it registers nothing in a hidden global. The app hands routes to
  `Router(routes=(...))` itself. Decorators return the wrapped object so user
  code stays plain.
- Dependencies are injected as arguments (contexts, state), never reached through
  globals or singletons, which is what makes the core testable without mocks.

This is also why the client API is imperative and *should* be. The server
framework surrounds the user's handler (it calls inward); the client user holds
the continuation (the code after `await response` is theirs), so the client is a
script at the rim, not a processor at the center. Forcing both into one shape to
make the library look symmetric would contort the caller's code; the asymmetry is
real, so the two sides stay different while sharing the one composition tool that
genuinely generalizes (`stack`).

## Known-hard problems, faced deliberately

The project inherits the hard problems of dataflow and FRP and chooses to face
them rather than discover them. Backpressure is handled where it arises rather
than bolted on: a bounded queue makes a slow consumer stall its producer instead
of growing an unbounded backlog (the HTTP server's per-stream `WINDOW_UPDATE` flow
control and `stream_from_queue`'s shutdown signal are the live examples). The
`sample` behavior edge deliberately has *no* backpressure (latest-wins is its
whole point). Glitches on diamond dependencies, feedback cycles, and teardown
order remain open and are tracked as such. The `sample` behavior edge pairs
`current` (read the latest value, non-blocking) with `updated` (await the next
published value): a deterministic "await next update" signal that lets a reader
wait on a known event rather than racing the background drain.
