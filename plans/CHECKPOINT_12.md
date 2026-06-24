# Checkpoint 12

A snapshot of where `without` stands, succeeding `CHECKPOINT_11.md`. For the
original pitch see `BIG_IDEA.md`; for the critical review and open design
questions see `REVIEW_BIG_IDEA.md`; for the actor-model digression see
`ACTOR_MODEL.md`.

## What changed since Checkpoint 11

One sustained pass over the `without-asgi` routing story and the `transform`
example that exercises it. The middleware machinery the example had grown was
generalized and pushed into `without-asgi` as *tools* (not a router), the example
gained a real WebSocket route, config sampling was moved from request-render time
to request-start, and `transform.core` was stripped of every HTTP and byte concern
so it is now a pure text-transform that raises domain errors for the shell to
render.

- **`without-asgi` ships routing *tools*, not a router (`without_asgi.routing`).**
  Writing a router is opinionated work (what a route matches on, how dispatch falls
  back), so the package deliberately ships no dispatcher. The new optional submodule
  provides only the unopinionated pieces: a `Middleware` vocabulary, `stack` to
  compose it, and `buffered` for the request/response handler shape. The
  `make_asgi_app` seam still asks for nothing but an `HttpRouter[T]` function; these
  just make one easy to assemble. Importing the submodule is opt-in; it is not part
  of the core contract.
- **`Layer` collapsed into `Middleware`; `around` is gone.** Checkpoint 11 had two
  concepts bridged by a lift: `Middleware = Handler -> Handler` plumbing and `Layer
  = (Processor, HttpScope) -> Processor` work, with `around` lifting the latter into
  the former. They are now one thing: `Middleware = (HttpHandler, HttpScope) ->
  HttpHandler` (what `Layer` was), and the router applies each middleware directly
  to the built handler. The `Handler -> Handler` dual type and `around` both
  vanished as accidental complexity.
- **`Middleware` is generic over the protocol.** `Middleware[H, S] = Callable[[H,
  S], H]`, with `HttpMiddleware` / `WebsocketMiddleware` aliases. `stack(*middleware)
  -> Middleware` composes a sequence into one (first outermost; a stack of
  middleware is itself a `Middleware`, and `stack()` is identity). The same
  vocabulary serves HTTP and WebSocket.
- **`buffered` adapts a request/response function and reads as a decorator.**
  `buffered[T](make: (T, HttpScope, bytes) -> Response) -> HttpRouter[T]` reads the
  whole body and emits one `Response`, for handlers that don't stream. Because it
  takes a single callable and returns a value, the example uses it bare as
  `@buffered` over each handler, so handlers stay plain functions and the route
  table holds them directly. (`unary` was weighed as a name and rejected;
  `buffered` is the precise antonym of streaming in a stream-oriented package.)
- **The example owns its `Router`, now protocol-generic (`transform.router`).** A
  small `Router[T, S, H]` assembled from the tools above, generic so one
  implementation dispatches both HTTP and WebSocket. Each `Route[T, S, H]` carries a
  match predicate built by `http_route(method, path, ...)` (matches method and path)
  or `ws_route(path, ...)` (matches path alone); `Router.dispatch` is the
  `HttpRouter` / `WebsocketRouter` callable handed to `make_asgi_app`. This
  resolves the carried-over "`integration`'s `Handler` name overlaps
  `without-asgi`'s" item: the example no longer defines a `Handler` type at all, it
  uses without-asgi's `HttpRouter` shape (aliased `Endpoint[T, S, H]`) directly.
- **The example serves a WebSocket route.** `/stream` accepts the connection and
  transforms each text frame with the connect-time default mode (closing a binary
  frame with `1003`); `refuse_socket` is the fallback that closes an unrouted path
  without reading any frames. A `socket_log` middleware demonstrates the same
  generic `Middleware`/`stack` vocabulary composing on the WebSocket side. This
  resolves Checkpoint 11 open item #3 (WebSocket half-used).
- **Config is locked at connection start, not request-render time.** Checkpoint 11
  read `Context.current()` inside the handler after the body was read, so a reload
  mid-request leaked into the response. Now the wiring snapshots the live `Context`
  at the dispatch boundary (`http=lambda config, head: requests.dispatch(
  config.current(), head)`, same for websocket), the moment a connection arrives,
  and threads a plain `Settings` *value* to the handlers. A reload takes effect on
  the next connection rather than changing an open one underfoot. Handlers no longer
  touch the `Context` at all.
- **`transform.core` is pure and HTTP/byte-unaware; the shell owns the rest.** Core
  was rendering JSON, status codes, and headers and parsing query strings. It now
  works in decoded text and raises domain errors: `transform(settings,
  requested_mode, text: str) -> str`, `resolve_mode`, `apply_mode`, and a
  `TransformError` base with one `UnknownMode` subclass. Everything HTTP- or
  byte-shaped moved to `transform.app` (the shell): the `max_bytes` size limit
  (`413`), the UTF-8 decode (`400` on failure), the `mode` query parse, the
  domain-error catch (`400`), and the `json_response` / `text_response` rendering.
  `render_modes` / `route_not_found` / `mode_param` / `_json_response` left core;
  `BodyTooLarge` / `NotUtf8` are gone (those were byte concerns, now inline shell
  responses).

## Rationale: a few decisions worth remembering

- **Tools, not a router.** A router braids dispatch policy (match keys, fallback)
  with mechanism (middleware composition, body buffering). `without-asgi` ships the
  mechanism and leaves the policy to the app, so the package stays unopinionated and
  the seam stays "just give me an `HttpRouter[T]` function." The generic
  `Router[T, S, H]` in the example shows how little the tools demand to build one.
- **A middleware just wraps a handler.** Once a middleware *is* the
  `(handler, scope) -> handler` wrapper, the router applies it directly and the
  `Handler -> Handler` lift (`around`) is unnecessary. Collapsing the two concepts
  into one removed a type and a helper without losing any expressiveness; a
  middleware still sees the whole processor and can wrap the inbound stream, the
  outbound stream, or both.
- **Generality earned by a second caller.** Making `Middleware`/`stack`/`Router`
  generic over `(handler, scope)` was justified the moment the WebSocket route
  reused all three. The only genuinely HTTP-specific tool is `buffered` (a body and
  a single `Response` have no WebSocket analogue); dispatch keys differ per protocol
  and live in the `Route` predicate.
- **Lock config at connection start (values over places).** Snapshotting
  `Context.current()` at the dispatch boundary hands each connection an immutable
  `Settings` value rather than a live holder it reads later. A request becomes
  reproducible against the config it started with; a reload cannot change it
  mid-flight. The next connection picks up the new value. This is the configuration
  rule applied at the request boundary.
- **The core knows neither bytes nor HTTP.** `transform` takes decoded text and
  returns text, raising `UnknownMode` on a bad mode; it never names a status, a wire
  format, or a byte count. The shell owns the body (size limit, decode), the query
  parse, and the rendering, and maps each raised `TransformError` to a response.
  This is functional-core / imperative-shell drawn at the obvious line, and it makes
  the core a plain in/out function to test.

## Status

Done and verified (mypy strict clean across 43 source files, the test suite green
at 154 passing, ruff lint + format clean, pre-commit clean):

- `without-asgi`: new opt-in `without_asgi.routing` submodule with `Middleware`
  (generic, plus `HttpMiddleware` / `WebsocketMiddleware` aliases), `stack`, and
  `buffered`. The core `make_asgi_app` contract is unchanged; the README documents
  the tools-not-a-router stance. Scope/event/extension spec coverage unchanged from
  Checkpoint 9.
- `integration.transform.core`: pure and HTTP-unaware (`Mode`, `Settings`,
  `TransformError`, `UnknownMode`, `apply_mode`, `resolve_mode`, `transform` over
  decoded text). Unit-tested as plain functions and raised exceptions.
- `integration.transform.router`: a protocol-generic `Router[T, S, H]` with
  `Route`, `Endpoint`, `http_route`, `ws_route`, built on the routing tools.
- `integration.transform.app`: the shell. HTTP handlers (`transform_text`, `modes`,
  `not_found`) and a WebSocket handler (`transform_socket`) plus its `refuse_socket`
  fallback; the middleware stack (`access_log`, `with_header`, `socket_log`); the
  HTTP rendering helpers (`json_response`, `text_response`, `mode_param`); and the
  per-connection config snapshot wired into `make_asgi_app`. Now serves both an HTTP
  route table and a WebSocket route.

## Open questions and next steps

Raised this checkpoint:

1. **Generic `Router[T, S, H]` vs two concrete routers.** The example's router is a
   single protocol-generic type with three parameters and a per-route match
   predicate. It is a tidy "look how little the tools demand" demonstration, but a
   real two-protocol app might prefer two small concrete routers (HTTP on
   method+path, WebSocket on path) over the type-parameter noise and predicate
   indirection. Left generic for now; worth revisiting if it reads as too clever.
2. **`Settings.max_bytes` is core config only the shell reads.** `Settings` lives in
   `transform.core` as the single ConfigMap-parsed model, but `max_bytes` is a
   transport limit the shell enforces (core only uses `default_mode`). Kept whole
   for now; splitting the shell-only knob out of the domain config is a reasonable
   next move.

Carried from Checkpoint 11, still open:

3. **No real HTTP/WebSocket testing.** Everything is still driven by hand-built
   `scope` dicts, scripted `receive`, and capturing `send`. The `transform` example
   now exercises request bodies, a middleware stack, *and* a websocket, so a few
   `httpx.ASGITransport` tests plus a `uvicorn` smoke run over `make_asgi_app` would
   catch conformance gaps the hand-written capture cannot. Still the best first
   target.
4. **Routing fidelity.** Dispatch still matches `(method, path)` (HTTP) or `path`
   (WebSocket) exactly. `buffered` gives a decorator ergonomic for handlers, but
   path parameters and a route-registration decorator (`@route.post("/x")`) remain
   deferred.
5. **Extensions are modeled but never negotiated in anger.** No example reads
   `scope.extensions` and emits an extension event; a small `supports(scope, name)`
   helper plus one worked use would validate the negotiation story.
6. **A non-ASGI shell would make the portability promise concrete.**
   `make_asgi_app` consumes a portable `Lifespan[T]`; nothing yet drives the same
   lifespan from a queue processor or CLI to prove the seam.
7. **Intra-workspace deps are unpinned, a packaging gap for publishing.**
   `without-env`, `without-configmap`, and `without-asgi` each declare a bare
   `"without"` dependency with no version constraint. Before the first real release,
   pin each intra-workspace dep at publish time or commit a lower bound and bump it
   on release.
8. **Unconsumed or partially-read input streams.** A handler may send its whole
   response without draining the inbound stream (`refuse_socket` and every body-less
   handler do). ASGI permits it and the inbound generators own no resources, so it
   is harmless, but `make_asgi_app` never explicitly `aclose()`s the inbound stream;
   wrapping the handler call in `contextlib.aclosing(...)` would make cleanup
   deterministic (robustness, not a fix). The websocket refusal also sends `close`
   without first receiving `websocket.connect`, still unverified against a live
   server (folds into item #3).

Carried forward from earlier checkpoints (still open):

- **The actor-model question** (`ACTOR_MODEL.md`, open question #2). Unchanged;
  `transform` is still stateless and reads a snapshot rather than messaging a shared
  fold. A stateful ASGI or websocket example would add pressure.
- **Static `Context` ceremony** (open question #3): the dynamic half remains
  demonstrated; the static-config-as-plain-value question is unchanged.
- A **dynamic-merge** connector as the single sanctioned funnel; whether a consumer
  parameter deserves a named `Leaf` type; factoring the connection-set +
  bounded-drain orchestration out of `kv`.
- Deferred deliberately: graph/DAG recovery and visualization on `graphlib`;
  known-hard FRP problems (diamond glitches, feedback cycles, teardown order); a
  deterministic "await next update" signal to replace `testing.tick`.

Documentation debt (carried forward): `BIG_IDEA.md` and the early checkpoints still
call the model an "async reducer"; it is more accurately an **async scan**, and the
functional-core / imperative-shell, connection-as-stream, and
lifecycle-as-a-portable-value framings should be folded into `BIG_IDEA.md` when it
is next revised.
