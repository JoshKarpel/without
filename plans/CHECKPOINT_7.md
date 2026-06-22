# Checkpoint 7

A snapshot of where `without` stands, succeeding `CHECKPOINT_6.md`. For the
original pitch see `BIG_IDEA.md`; for the critical review and open design
questions see `REVIEW_BIG_IDEA.md`; for the actor-model digression see
`ACTOR_MODEL.md`.

## What changed since Checkpoint 6

This session made the long-deferred HTTP/ASGI step concrete (Checkpoint 6 open
question #1). It split into two pieces: a new distributable adapter package and a
worked example that exercises it. Crucially, the example is **stateless** and
leans on a **dynamic** config `Context`, which turns the ASGI work into direct
evidence for Checkpoint 6 open question #3 (when does a `Context` earn its keep).

- **New package `without-asgi` (import `without_asgi`): the ASGI boundary, and
  only the boundary.** It turns an ASGI application callable
  (`async def app(scope, receive, send)`) into typed `without` streams and back.
  It deliberately contains no routing, no middleware machinery, no lifespan
  policy: those are the application's to wire. Its only runtime dependency is
  `without`. Two modules, mirroring the `kv` core/shell split:
  - `core.py` (pure, sans-IO): the raw ASGI type aliases (`Scope`, `Message`,
    `Receive`, `Send`, `ASGIApp`), typed event/scope dataclasses for the HTTP +
    lifespan subset (`HttpScope`, `RequestBody`/`Disconnect`,
    `ResponseStart`/`ResponseBody`, a whole-`Response` convenience, and the
    `Startup`/`Shutdown` events with their reply variants), and the boundary
    `parse_*`/`encode_*` functions. Parsing is total at the boundary and fails
    loud: an unknown event type raises rather than being silently dropped.
  - `shell.py`: the receive/send adapters. `http_inbound(receive)` exposes
    `receive` as a `Stream` via a plain async generator (no queue: `receive` is
    already pull-based), ending on the final body chunk or a disconnect, so the
    request's lifecycle *is* the stream's lifecycle. `http_outbound(send)` and
    `lifespan_outbound(send)` are `from_sink` adapters; `lifespan_inbound`
    ends after shutdown.
- **Example `without_integration.flags`: a stateless feature-flag service.** It
  shows the FastAPI-shaped concerns as plain `without` wiring on top of the
  adapters, with no shared application state of its own. Split like `kv`:
  - `flags.core` (pure): the `Flags` model and response rendering
    (`render_all`, `render_one`, `bad_request`, `route_not_found`, `flag_name`).
  - `flags.app` (shell): a `Router` value selecting a handler by `(method,
    path)`, a `with_header` middleware that is plain processor composition, and
    a lifespan that owns the config-watch lifecycle. Handlers read
    `flags.current()` at request time, so a reload reaches in-flight requests.
- **The example wires `without-asgi` to `without-configmap`.** The lifespan opens
  a `sample(watch_config(...))` on startup and holds it until shutdown; the
  `sample` block spans the whole server lifetime. This is the first time the
  reloading `Context` from Checkpoint 6 is consumed by a server rather than a
  test, and it is what makes the handlers' `.current()` reads meaningful.
- **Packaging and docs.** `without-asgi` added to the workspace (root and
  `without-integration` `pyproject.toml` plus `uv.lock`); root `README.md` layout
  and roadmap updated (env/configmap/kv done, HTTP in progress); the
  `without-integration` README documents the `flags` artifact.

## Rationale: routing is a scope choice, lifespan owns the resource

Two structural insights drove the shape, both consistent with Checkpoint 6's
"a connection's lifecycle is a stream's lifecycle".

- **Routing is a build-time `scope -> Processor` selection, not a per-event
  split.** An HTTP request's method and path arrive once in the `scope`, before
  any event, so the handler is *chosen* by a plain table lookup (`Router.select`)
  and then run over the request's event stream. This is why `without.route`
  (which partitions a stream by runtime type, per event) is the wrong tool here:
  the decision is made once, up front, not on every event. The per-request
  handler is the ASGI analogue of `kv`'s per-connection `make_session`.
- **Lifespan is the imperative shell that owns a resource's lifecycle.** The
  `async with sample(source)` block spans the gap between the `startup` and
  `shutdown` events, so the config watch lives for exactly the server's lifetime,
  the way a FastAPI `lifespan` would. Setup/teardown of an `async with` resource
  cannot be threaded through a per-event fold (the block must straddle two
  events), so this stays imperative-shell code in the app, not a pure processor.

## The dynamic `Context`, now earning its keep

Checkpoint 6 open question #3 asked whether a `Context` (and `.current()`) pulls
its weight, noting that for *static* config a closure-captured value would do.
The `flags` example is the affirmative case for the *dynamic* half: because the
ConfigMap reloads, a handler cannot capture the value once; it must re-read
`flags.current()` at request time. A test (`test_handlers_pick_up_a_config_
reload_mid_lifetime`) pins this: a request sees the old flags, a gated reload
fires, and a later request sees the new flags through the same running app. This
is the configuration rule in practice (watch/refresh what must change without a
restart) reaching all the way into request handling.

## The one place: a shared `Context` reference

ASGI models the lifespan and each HTTP request as *separate* `app()` calls that
must nonetheless share the config `Context`. That forces exactly one shared
reference to live in the app closure (`_Holder` in `flags.app`), set once during
lifespan startup and read by every request. It is a place, but a minimal,
set-once one, and `Context` is itself the read-only-latest abstraction the model
endorses. This is the single spot where the ASGI process model (one long-lived
lifespan call, many short request calls) diverges from `kv`'s single `serve`
context manager, and it is called out in a comment. Whether a cleaner construct
than a hand-rolled holder is warranted is a cleanup question (below).

## Status

Done and verified (mypy strict clean across 32 source files, 93 tests passing,
ruff lint + format clean):

- `without-asgi`: typed HTTP + lifespan event model, total boundary
  `parse_*`/`encode_*`, and the `http_inbound`/`http_outbound`/`lifespan_inbound`/
  `lifespan_outbound` adapters. Tested at the pure boundary and at the adapter
  level (scripted `receive`, capturing `send`).
- `without_integration.flags`: the feature-flag service, tested in-memory by
  driving `app()` directly: lifespan handshake as a background task, a GET for
  all flags (with the middleware header), a single-flag GET, a 400 for a missing
  query parameter, a 404 for an unknown route, a websocket-scope rejection, and
  the dynamic-reload test above.

## Open questions and next steps

The stated next step is to **clean up this implementation**; the items below are
the cleanup and the gaps it should weigh.

1. **No real HTTP testing.** Everything is exercised by constructing `scope`, a
   scripted `receive`, and a capturing `send`, and calling `app()` directly. That
   proves the wiring but not conformance: header casing, body framing, and the
   lifespan handshake as a real ASGI server or client drives them are untested. A
   few `httpx.ASGITransport` tests (a dev-only dependency, in-memory, no socket)
   would catch divergences the hand-written `send`-capture cannot, and running it
   once under `uvicorn` would validate the end-to-end story. Deferred this
   session to keep the dependency surface minimal and the question open.
2. **The shared `_Holder` place.** It is the one hand-rolled mutable reference
   (see above). Cleanup should decide whether to keep it, generalize it, or find
   a construction that hands the lifespan-created `Context` to request handlers
   without a set-once cell.
3. **Routing fidelity.** The `Router` matches `(method, path)` exactly. Path
   patterns / path parameters (`/flags/{name}`) and a decorator ergonomic
   (`@app.get`) are deferred; the single-flag lookup uses a query parameter to
   avoid a pattern parser. Decide in cleanup whether a small pattern layer earns
   its place or stays out of an adapter-plus-example.
4. **Scope of `without-asgi`.** Confirm the boundary is drawn correctly: the
   whole-`Response` convenience and `encode_response` live in the adapter (the
   "and back" direction); routing/middleware/lifespan policy deliberately do not.
   Streaming response bodies (many `ResponseBody` with `more_body=True`) and
   websockets are representable in the types but unimplemented; request-body
   streaming is collected by the handler rather than streamed.

Carried forward from Checkpoint 6 (still open):

- **The actor-model question** (`ACTOR_MODEL.md`, open question #2). The
  per-request `ask`-shaped funnel did not recur here because `flags` is stateless
  and reads a `Context` instead of messaging a shared fold, so this session adds
  no new pressure but no resolution either. A stateful ASGI example (shared
  application state behind a singular fold, the original ASGI-fork shape) would.
- **Static `Context` ceremony** (open question #3): the dynamic half is now
  demonstrated; the static-config-as-plain-value question is unchanged.
- A **dynamic-merge** connector as the single sanctioned funnel; whether `serve`'s
  consumer parameter deserves a named `Leaf` type; factoring the connection-set +
  bounded-drain orchestration out of `kv`.
- Deferred deliberately: graph/DAG recovery and visualization on `graphlib`;
  known-hard FRP problems (diamond glitches, feedback cycles, teardown order);
  a deterministic "await next update" signal to replace `testing.tick`.

Documentation debt (carried forward): `BIG_IDEA.md` and the early checkpoints
still call the model an "async reducer"; it is more accurately an **async scan**,
and the functional-core / imperative-shell and connection-as-stream framings
should be folded into `BIG_IDEA.md` when it is next revised.
