# integration

Not a real package, and never published: the name deliberately sits outside the
`without*` family that the publish workflow globs, and a `Private :: Do Not
Upload` classifier makes PyPI reject it as a backstop. It is the aggregator where
`without` and every plugin are imported together and exercised as a whole. Each
new plugin is added to this package's dependencies so its interaction with the
rest gets a home for tests.

It also hosts validation artifacts that are not meant to be distributed. `kv` is
a toy line-protocol key-value server (Redis-ish) built on `without`, proving the
contract supports long-lived processor state and request/response. It splits into
`kv.core` (the pure keyspace: parse a line, fold it into an immutable `Store`,
render a reply) and `kv.shell` (a generic line-server transport plus the wiring
that runs the core over it), a small demonstration that `without` is a principled
way to write an imperative shell.

`durable` is a durable workflow built on `without-dag`, and the demonstration that
resumption needs an engine no more than routing did. Fulfilling an order charges
the card and reserves the stock concurrently, ships, and renders a receipt;
`durable.core` is that graph plus the compensations, with every effect injected as
a `Services` callable and every node named in the source, so a result is stored
under a key that means the same thing after a crash. `durable.shell` is the whole
runner: `run_durably` loads a workflow's checkpoint, streams the graph, records
each `(key, result)` before pulling the next, and returns the output, so re-running
a finished workflow performs no effects at all. `run_saga` adds the compensating
half: on failure it parses the checkpoint into how far the run got (`Reached`) and
drives a rollback graph under its own key, itself checkpointed, so an interrupted
rollback resumes instead of refunding twice. `durable.store` is the one module that
knows Redis and JSON, a workflow being one hash and a completed step one field in
it, which is what makes the checkpoint readable with `redis-cli` and writable by a
process that shares nothing with the one that crashed.

`durable.stepwise` is the same durability without the graph, and the two sit together
deliberately: one checkpoint, two ways to spend it. A workflow is an ordinary async
function whose effects are named (`await run.step("charged", ...)`), resuming means
calling it again, and each step it reaches hands back what is already recorded instead
of running. The whole engine is a dict lookup. What it asks in return is the rule the
rest of this repo already keeps, since the code *between* the steps re-runs: effects
live in steps, the code around them is pure. Temporal and DBOS state that same rule as
workflow determinism.

Two things fall out that a fixed graph cannot express, both in `durable.payout`. The
fan-out is data-dependent at run time (one capture step per line item a *step*
returned, keyed by sku, so a crash resumes item by item), and a step that cannot
finish now raises `Suspended` instead of blocking. That last one is what buys a
settlement window waited out across crashes (`run.sleep` records the *deadline*, so a
crash on day two does not restart the clock) and a human approval (`run.awaiting`
suspends until another process writes one field into the workflow's hash, which is a
signal without a mailbox: the wait outlives the process that was waiting). What the
graph keeps in exchange is the eager check, since it knows every key before it runs
anything, and a structure you can diagram.

`durable.api` and `durable.worker` are the piece that is genuinely a service: something
that notices a suspension and comes back. They are an API server and a queue worker,
deployed separately, sharing only Redis, and between them they are about a hundred
lines.

The API never runs a workflow. Submitting an order and confirming a payout are the
same two-line move, `record` one value then `make_ready`, because both are values the
workflow is waiting on and `Run.awaiting` cannot tell which is which. That is what
replaces a client library talking to a workflow server, and it is why the API holds
nothing and can be restarted or scaled at will. The workflow id is the request's
`Idempotency-Key`, so a resubmitted order is the same workflow rather than a second
one, and `GET /orders/{id}` renders the checkpoint as the progress view, since the
durable state *is* the state.

The worker is a `Sink` over a `Stream` plus a timer, which is `without`'s own
vocabulary doing the work:

```text
deliveries ──▶ pool of N passes ──▶ Suspended with a due?  ──▶ wake_at (a clock)
      ▲                         │  Suspended without one? ──▶ nothing to do
      │                         └─▶ done (this wakeup is answered for)
reclaim one, else read one
timer ──▶ wake_due (one move, in the store)
```

The two arms of that branch are the two ways a workflow waits, and `Suspended` says
which: a deadline the workflow chose gets scheduled, a value the world owes it does
not, because the API's confirmation is what will queue it. Nothing polls a workflow to
ask whether it can proceed. `durable.wakeups` is those two structures, a Redis stream
of ready ids and a sorted set scored by deadline, and it is the direct answer to
"surely this is what Temporal's server does": yes, and once the state is a checkpoint
anyone can read, the rest is a stream, a sorted set, and one small Lua script.

That script is `wake_due`, and it earns its place twice. Taking a workflow off the
sleepers and queueing it are durable only *together*: a timer that did them as two
calls would lose the workflow whenever it died in between, leaving it in neither
structure and asleep forever. Running the move in the server closes that, and because
the script is serialized against itself it also decides which of several timers owns a
wakeup, so every worker can run one and none needs to be elected.

A *stream* rather than a list for the same reason. `BLPOP` hands an id over and forgets
it, so a worker that dies mid-pass takes the wakeup with it; `XREADGROUP` moves the
entry into that consumer's pending list, where it stays until acknowledged and where
`XAUTOCLAIM` can hand it to another worker. That is why a delivery is a value with a
receipt rather than a bare id, and why the acknowledgement comes after the pass and its
error handling, on every path the process observed. Cancellation is the one path that
skips the ack, deliberately: a worker shutting down mid-pass has not finished, so
leaving the delivery outstanding is what lets someone else reclaim it.

Concurrency falls out of the same pull-driven shape. A worker runs up to `POOL` passes
at once through `without`'s `limit_concurrency`, which advances a lazy source only when
a slot frees, and every pull takes exactly one delivery: a reclaimed one if some
workflow was abandoned, otherwise a fresh read. So "pull one at a time" and "run twenty
at a time" are the same sentence, and a worker holds precisely as many wakeups as it is
working on. At capacity it simply stops reading, and the work stays in the stream where
another worker can take it, which is backpressure without a mechanism for it.

Deploying the pair is two entrypoints over the same two stores:

```python
redis = Redis(host=..., decode_responses=True)
checkpoints, wakeups = RedisCheckpoints(redis=redis), RedisWakeups(redis=redis)

# the API process
async with serving(payments_app(Payments(checkpoints=checkpoints, wakeups=wakeups))):
    await asyncio.Event().wait()

# the worker process, however many of them
await work(checkpoints, wakeups)
```

What a real one adds, and this deliberately does not: a lease per *workflow*, so a
confirmation landing mid-pass cannot start a second pass beside the first (reclaiming
bounds that risk for a crashed worker, but nothing bounds it for a racing wakeup, and
the pool widens the window by running twenty passes at once); a retry policy, since a
workflow whose step raises is logged and acknowledged rather than backed off; and a job
that trims the stream by `MINID` behind what has been acknowledged, since capping its
*length* instead would drop the oldest entries, which are the ones nobody has run yet.

The Redis tests drive a real server rather than a fake: `just test` starts the
services in `compose.yaml` with podman, hands each published address to pytest
through the environment, and takes the stack down again from an exit trap. They
carry a `compose` mark and skip when that address is unset (no podman on the
machine, or pytest run directly), so `-m "not compose"` opts out up front.

`transform` is a text-transform service built on the `without-asgi` adapters.
`POST /transform` reads the request body, uppercases/lowercases/title-cases it
per a `?mode=` query override, and caps the size, with the default mode and the
limit both from a `without-configmap` `Context`. Each HTTP request and WebSocket
connection snapshots the config the moment it arrives, so a ConfigMap reload takes
effect on the next one rather than changing an open connection mid-flight (`GET
/modes` reports the current values, and a WebSocket `/stream` transforms each text
frame with its connect-time snapshot). It shows the
framework-shaped concerns (routing, middleware, lifespan) as plain `without`
wiring, and splits along the functional-core/imperative-shell line.
`transform.core` is pure and HTTP-unaware: the `TransformConfig` (just the default
`Mode`), the `Mode` transforms over decoded text, and the `UnknownMode` error it
raises rather than encoding a status. `transform.router` is a small
protocol-generic `Router` assembled from `without-asgi`'s routing tools, since the
adapters ship those but no router of their own. `transform.app` is the ASGI shell:
the HTTP and WebSocket handlers own the bytes (the size limit, the decode, the
query parse), call the core, and render its result or raised error as JSON; it
also holds the middleware stack applied across every route and the config-watch
kept for the server's lifetime via `without-asgi`'s `make_asgi_app`. Its config
splits in two, `Settings.transform` (the domain `TransformConfig`) and
`Settings.http` (the shell-only byte limit), so the core never sees a transport
concern. One middleware, `request_digest`, negotiates the
`http.response.trailers` extension (via `without-asgi`'s `extension` helper): it
returns the request-body digest in a response trailer when the server advertises
the extension, and falls back to a header (draining the body first so the header
is correct) when it does not.

`transform.cli` is a second shell over the *same* core, to make the portability
claim concrete. It reads lines from stdin and prints each one transformed,
drawing its config from a `without-env` `Context` rather than a ConfigMap and its
default mode and prompt prefix from the environment. The core and its
`TransformConfig` are unchanged between the two shells: only the I/O at the edge
and the config source differ, which is the narrow-waist payoff the project is
chasing.

`todos` is a user-facing exercise of the opinionated `without-web` router (where
`transform` hand-rolls one from `without-asgi`'s tools). It is the canonical
todo-list REST API, chosen because it hits the whole router design at once:
`t"/todos/{todo_id}"` is a typed path parameter (the same `todo_id` token names
the segment and is reused as the handler's `int` argument), `GET` vs `POST` on
`/todos` is method
dispatch (so a `PUT` is a `405` with `Allow`, not a `404`), `?done=` is a typed
`query_param` extractor, `/admin` is a `mount(...)` that bakes its prefix and auth
gate into the routes under it and `/legacy` an opaque `delegate(...)` (handed the
prefix-trimmed scope), `TodoNotFound`/`ValidationError` are
mapped to `404`/`422` by HTTP exception handlers, two endpoints read their input
*live* and fold it into a working list across the connection (`POST /todos/import`
as a `@post.stream` route over an NDJSON upload, and the `/todos/session`
websocket as the same fold kept open bidirectionally), and each `@get`/`@post`
handler is a plain function of typed values whose extractors also supply the
OpenAPI, so `todos_openapi()` merges the router's path/method half with each
endpoint's body/query/response half. `todos.core` is the pure, immutable
`TodoList`; `todos.app` is the `without-web` wiring, where `Router.dispatch` snaps
straight onto `make_asgi_app` because it already *is* an `HttpRouter`.
