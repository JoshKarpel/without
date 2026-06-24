# Checkpoint 10

A snapshot of where `without` stands, succeeding `CHECKPOINT_9.md`. For the
original pitch see `BIG_IDEA.md`; for the critical review and open design
questions see `REVIEW_BIG_IDEA.md`; for the actor-model digression see
`ACTOR_MODEL.md`.

## What changed since Checkpoint 9

A single focused refactor of the `without-asgi` connection entrypoint:
`make_asgi_app` moved from one scope-matching handler to a per-protocol **router**
pattern, and the driver now owns the receive/send wiring instead of delegating it.

- **`ScopedApp[T]` is gone; `make_asgi_app` takes optional per-protocol
  routers.** The old signature was `make_asgi_app(lifespan, handler)` where
  `handler: ScopedApp[T] = Callable[[T, ConnectionScope, Receive, Send],
  Awaitable[None]]`: a single callable handed the raw `receive`/`send` that had to
  match on the scope and do all the stream wiring itself. The new signature is
  `make_asgi_app(lifespan, http=None, websocket=None)`, each a `*Router | None`.
- **Routers select handlers; the driver owns the wiring.** A `*Handler` is now
  the bare `Processor` that serves one connection (`HttpHandler =
  Processor[Inbound, Outbound]`, `WebsocketHandler = Processor[WebsocketInbound,
  WebsocketOutbound]`), the same shape as every other `without` node. A `*Router`
  selects that handler from the threaded-in state and the parsed scope
  (`HttpRouter[T] = Callable[[T, HttpScope], HttpHandler]`, and the websocket
  equivalent). `make_asgi_app` calls the router once per connection, then wraps
  `receive` into the inbound stream, runs the returned `Processor`, and drains its
  outbound stream into `send`. The router and its handler only ever see streams,
  never the raw ASGI callables (or the lifespan scope).
- **Each protocol's router is optional, so an app serves only what it provides.**
  A connection whose router is absent is rejected with `NotImplementedError` (an
  HTTP-only app omits `websocket`; a WebSocket-only app omits `http`). HTTP and
  WebSocket keep separate router/handler pairs because their event types differ,
  which makes an HTTP handler emitting a WebSocket event (and vice versa)
  unrepresentable rather than a runtime check.
- **`flags` is wired on the router seam.** `make_app` now passes a `route`
  function (`HttpRouter[Flags]`) that just calls `router.select(method, path)(head,
  flags)`; the old `serve` `ScopedApp` that matched on the scope and drove
  `http_inbound`/`http_outbound` by hand is gone, because the driver does that now.
- **Exports and README tracked the rename.** `__init__.py` drops `ScopedApp` and
  adds `HttpHandler` / `HttpRouter` / `WebsocketHandler` / `WebsocketRouter`; the
  `without-asgi` README's `make_asgi_app` paragraph was rewritten to the router
  pattern (the manual `parse_http_scope` + `http_inbound`/`http_outbound` example
  it keeps is now framed as the drill-under path for a handler that needs the raw
  `receive`/`send`).

## Rationale: a few decisions worth remembering

- **The handler is a `Processor`, not an ASGI callable.** Pushing the receive/send
  wiring into `make_asgi_app` means an app writes only the two shapes `without`
  already speaks: a `Processor` per connection and a `Router` that picks one. The
  raw ASGI callables stay entirely inside the boundary, which is the whole point
  of the adapter. Drilling under it (a handler that genuinely needs raw
  `receive`/`send`) is still `parse_scope` plus the shell functions, now an
  explicit escape hatch rather than the default contract.
- **"Router" is the role even for a constant.** A router that always returns the
  same handler (never dispatching on path) is still a router. The name describes
  the seam's job (state + scope in, handler out), not whether it branches.
- **State is threaded, not captured.** The router is handed the lifespan value per
  connection rather than closing over it, so it stays a value the router receives,
  not a place it reaches into. This is the same `_Cell`-threaded discipline as
  before, now applied one level out at the router boundary.

## Status

Done and verified (mypy strict clean across 41 source files, 140 tests passing,
ruff lint + format clean, pre-commit clean):

- `without-asgi`: connection dispatch is the optional per-protocol router pattern;
  `make_asgi_app` owns all receive/send wiring around a per-connection `Processor`.
  Scope/event/extension coverage of the HTTP, WebSocket, and lifespan sub-specs
  (plus TLS) is unchanged from Checkpoint 9.
- `integration.flags`: unchanged behavior, now wired through an `HttpRouter[Flags]`
  that returns a `Processor` per connection; still HTTP-only (no websocket router).

## Open questions and next steps

Carried from Checkpoint 9, still open:

1. **No real HTTP/WebSocket testing.** Everything is still driven by hand-built
   `scope` dicts, scripted `receive`, and capturing `send`. A few
   `httpx.ASGITransport` tests and a `uvicorn` smoke run over `make_asgi_app` would
   catch conformance gaps the hand-written `send`-capture cannot. The router
   refactor makes this more inviting: a real handler is now just a `Processor`.
2. **Routing fidelity.** The integration `Router` still matches `(method, path)`
   exactly; path parameters and a decorator ergonomic remain deferred.
3. **WebSocket is modeled but unused.** `make_asgi_app` now accepts a
   `websocket` router and the streams exist, but no example handles a websocket
   connection. A worked echo or flags-over-websocket feed would exercise
   `websocket_inbound`/`websocket_outbound` end to end.
4. **Extensions are modeled but never negotiated in anger.** No example reads
   `scope.extensions` and emits an extension event; a small `supports(scope, name)`
   helper plus one worked use would validate the negotiation story.
5. **A non-ASGI shell would make the portability promise concrete.**
   `make_asgi_app` consumes a portable `Lifespan[T]`; nothing yet drives the same
   lifespan from a queue processor or CLI to prove the seam.
6. **Intra-workspace deps are unpinned, a packaging gap for publishing.**
   `without-env`, `without-configmap`, and `without-asgi` each declare a bare
   `"without"` dependency with no version constraint. Before the first real
   release, either have the publish step pin each intra-workspace dep to the shared
   version (`without==X.Y.Z`) or commit a lower bound in each `pyproject.toml` and
   bump it on release.

New this session, deferred:

- **`integration`'s `Handler` name now overlaps `without-asgi`'s.** The flags app
  has its own `type Handler = Callable[[HttpScope, Context[Flags]], Processor[...]]`
  (a per-request *builder* selected by the integration `Router`), which now shares
  the bare name "Handler" with `without-asgi`'s `HttpHandler` (the `Processor`
  itself). They are different layers (`integration.Handler` is closer to a
  `without-asgi` router entry), but the vocabulary could be reconciled so the two
  packages line up end to end.

Carried forward from earlier checkpoints (still open):

- **The actor-model question** (`ACTOR_MODEL.md`, open question #2). Unchanged;
  `flags` is still stateless and reads a `Context` rather than messaging a shared
  fold. A stateful ASGI or websocket example would add pressure.
- **Static `Context` ceremony** (open question #3): the dynamic half remains
  demonstrated; the static-config-as-plain-value question is unchanged.
- A **dynamic-merge** connector as the single sanctioned funnel; whether
  `serve`'s consumer parameter deserves a named `Leaf` type; factoring the
  connection-set + bounded-drain orchestration out of `kv`.
- Deferred deliberately: graph/DAG recovery and visualization on `graphlib`;
  known-hard FRP problems (diamond glitches, feedback cycles, teardown order); a
  deterministic "await next update" signal to replace `testing.tick`.

Documentation debt (carried forward): `BIG_IDEA.md` and the early checkpoints
still call the model an "async reducer"; it is more accurately an **async scan**,
and the functional-core / imperative-shell, connection-as-stream, and
lifecycle-as-a-portable-value framings should be folded into `BIG_IDEA.md` when
it is next revised.
