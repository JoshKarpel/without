# Checkpoint 11

A snapshot of where `without` stands, succeeding `CHECKPOINT_10.md`. For the
original pitch see `BIG_IDEA.md`; for the critical review and open design
questions see `REVIEW_BIG_IDEA.md`; for the actor-model digression see
`ACTOR_MODEL.md`.

## What changed since Checkpoint 10

Five related threads, all working the `without-asgi` boundary and the example
that exercises it: the integration example was reshaped to actually read request
content, body-reading and connection-refusal became first-class adapter pieces,
the example grew a real middleware layer, and two stream utilities were promoted
out of the testing module.

- **`integration.flags` became `integration.transform`, an example that reads
  request content.** The old feature-flag service only ever served `GET`s, so its
  handlers drained the inbound stream and ignored it: the `Processor[Inbound,
  Outbound]` shape was warped because nothing flowed in. The replacement is a
  text-transform service whose endpoint genuinely maps inbound + scope + config to
  outbound. `POST /transform` reads the request body, upper/lower/title-cases it
  per a `?mode=` query override, and caps the size; `GET /modes` reports the modes
  and the live default. Config is a `Settings` value (`default_mode: Mode`,
  `max_bytes`) read from a `without-configmap` `Context` at request time, so a
  reload still reaches in-flight requests. It splits into `transform.core` (pure:
  `Mode`, `Settings`, `transform`, `apply_mode`, `render_modes`,
  `route_not_found`, `mode_param`) and `transform.app` (the ASGI wiring).
- **`read_body` and `ClientDisconnect` are now `without-asgi` shell pieces.** Body
  accumulation moved out of the example into a public
  `read_body(events: Stream[Inbound]) -> bytes`, the inbound counterpart to
  `encode_response` on the way out. It joins the `RequestBody` chunks and raises
  `ClientDisconnect` if the stream ends on a `Disconnect` before the final chunk,
  so a truncated body fails loudly rather than passing for a complete one (parse,
  don't validate). Exported and unit-tested.
- **The integration `Router` grew a middleware layer.** A `Middleware =
  Callable[[Handler], Handler]` plus a `Router.middleware` tuple applied to every
  route, `not_found` included, first entry outermost. The `around(layer)` helper
  lifts a processor-level `Layer = Callable[[Processor, HttpScope], Processor]`
  into router `Middleware`, factoring out the identical `Handler -> Handler`
  plumbing each middleware otherwise repeats. Two concrete middlewares ship: a
  `with_header` factory now serving `X-Clacks-Overhead: GNU Terry Pratchett` on
  every route, and an `access_log` (a `Middleware` value, no config) that logs the
  inbound request (`--> METHOD PATH`) and the outbound response
  (`<-- METHOD PATH STATUS`), demonstrating one layer wrapping both the input and
  output streams at once.
- **`make_asgi_app` refuses unserved protocols with default routers, not an
  exception.** The per-protocol params now default to public `refuse_http` /
  `refuse_websocket` routers instead of `None` + `raise NotImplementedError`. An
  unserved HTTP scope gets `501 Not Implemented` (the HTTP status for "no handler
  for this", per a quick spec check, as opposed to `500` for an unexpected
  failure); an unserved WebSocket scope is closed before `accept`, which the ASGI
  server is required to turn into a `403` (per the WebSocket sub-spec). Refusal is
  now "just a router" the caller overrides by passing its own, so connection
  dispatch lost its `if router is None` branch entirely. `WebsocketClose` gained a
  docstring (linking MDN's `CloseEvent.code`) noting that `code`/`reason` are
  discarded when sent before `accept`.
- **`stream` and `collect` are public, in `without.wiring`.** They are a genuine
  in-memory source (a `Stream` from a fixed iterable) and its terminal dual (drain
  to a list), not test-only: `make_asgi_app`'s refusal path now emits its fixed
  response with the public `stream` rather than a private `_emit`. They sit next
  to `stream_from_queue` (the push-source counterpart). `tick` stays in
  `without.testing`, since nudging the event loop one step really is a test-only
  crutch (still flagged for replacement by a deterministic signal).

## Rationale: a few decisions worth remembering

- **Refusal is a router, not a special case.** Defaulting each protocol to a
  refusal router means dispatch has no protocol branch; serving a protocol is just
  overriding its default. The refusal becomes a spec-grounded value (`501`, or a
  close that the server turns into `403`) produced inside the boundary, rather than
  an exception the ASGI server has to catch and render as a generic `500`. Making
  the refusal routers public (`refuse_http`/`refuse_websocket`) lets an app
  reference or compose them deliberately.
- **Middleware is a stream transform; `around` is only the lift.** Each middleware
  had two layers: the `Handler -> Handler` plumbing that threads `head`/state, and
  the `Processor -> Processor` work that actually wraps the event stream. Only the
  second carries behaviour, so `around` supplies the first once and a middleware
  author writes just the layer. Because a layer wraps the whole `Processor`, one
  layer can touch the inbound stream, the outbound stream, or both (`access_log`
  does both).
- **`read_body` parses, it does not validate.** Returning the accumulated `bytes`
  but raising `ClientDisconnect` on a mid-body disconnect keeps a partial read from
  masquerading as a complete body downstream.
- **`stream`/`collect` are core; `tick` is not.** Building a stream from values (a
  fixed reply) and draining one to a list (a bounded fold) are production
  operations, so they belong in the public API. Only the loop-advancing `tick`
  is a test artifact.
- **`encode_response` stays the two-event tuple; no body chunking.** Considered
  adding an optional `chunk_size`, then rejected it: a `Response` body is already a
  fully-resident `bytes`, so splitting it into ASGI messages controls neither wire
  framing (the server owns that) nor process memory, and buys time-to-first-byte
  nothing (there is nothing incremental to stream). True streaming with
  backpressure is a different code path, a handler that yields
  `ResponseBody(more_body=True)` itself, not the whole-response-as-a-value helper.

## Status

Done and verified (mypy strict clean, the test suite green, ruff lint + format
clean, pre-commit clean):

- `without`: `stream` and `collect` are public in `without.wiring` and exported;
  `tick` remains in `without.testing`.
- `without-asgi`: connection dispatch defaults to public `refuse_http` /
  `refuse_websocket` routers (`501` and close-to-`403`); `read_body` /
  `ClientDisconnect` added to the shell; `WebsocketClose` documented. Scope, event,
  and extension coverage of the HTTP, WebSocket, and lifespan sub-specs (plus TLS)
  is unchanged from Checkpoint 9.
- `integration.transform` (was `integration.flags`): a text-transform service that
  reads the request body, a `?mode=` query override, and a live config `Context`,
  served through a router with a cross-cutting middleware stack (clacks header plus
  access logging). Still HTTP-only (no websocket handler).

## Open questions and next steps

Carried from Checkpoint 10, still open:

1. **No real HTTP/WebSocket testing.** Everything is still driven by hand-built
   `scope` dicts, scripted `receive`, and capturing `send`. A few
   `httpx.ASGITransport` tests and a `uvicorn` smoke run over `make_asgi_app` would
   catch conformance gaps the hand-written `send`-capture cannot. The `transform`
   example, which now exercises request bodies and a middleware stack, would make a
   good first target.
2. **Routing fidelity.** The integration `Router` still matches `(method, path)`
   exactly; path parameters and a decorator ergonomic remain deferred.
3. **WebSocket is still half-used.** `make_asgi_app` now has a default WebSocket
   refusal router exercised by a test (the close-to-`403` path), but no example
   *serves* a websocket connection. A worked echo or a config feed over websocket
   would exercise `websocket_inbound`/`websocket_outbound` end to end.
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

Raised this checkpoint:

7. **Unconsumed or partially-read input streams.** A handler may send its whole
   response without reading the inbound stream to completion: the refusal routers
   do exactly this, and any body-less handler can. ASGI permits it (the server
   drains or closes the unread request body), and `http_inbound` /
   `websocket_inbound` own no resources, so an abandoned generator is harmless and
   there is no correctness problem. Two loose ends remain. First, `make_asgi_app`
   never explicitly `aclose()`s the inbound stream, so a partially-read generator
   is finalized by GC / the loop's async-gen hook rather than deterministically;
   wrapping the handler call in `contextlib.aclosing(...)` would make cleanup
   deterministic (robustness, not a fix). Second, the websocket refusal sends
   `close` without first receiving `websocket.connect`, which is slightly
   non-canonical and unverified against a live server (folds into open item #1).

Carried from Checkpoint 10, still deferred:

- **`integration`'s `Handler` name still overlaps `without-asgi`'s.** The transform
  app keeps its own `type Handler = Callable[[HttpScope, Context[Settings]],
  Processor[...]]` (a per-request *builder* selected by the integration `Router`,
  now also the thing `Middleware` wraps), which shares the bare name "Handler" with
  `without-asgi`'s `HttpHandler` (the `Processor` itself). They are different
  layers; the vocabulary could be reconciled so the two packages line up.

Carried forward from earlier checkpoints (still open):

- **The actor-model question** (`ACTOR_MODEL.md`, open question #2). Unchanged;
  `transform` is still stateless and reads a `Context` rather than messaging a
  shared fold. A stateful ASGI or websocket example would add pressure.
- **Static `Context` ceremony** (open question #3): the dynamic half remains
  demonstrated; the static-config-as-plain-value question is unchanged.
- A **dynamic-merge** connector as the single sanctioned funnel; whether a
  consumer parameter deserves a named `Leaf` type; factoring the connection-set +
  bounded-drain orchestration out of `kv`.
- Deferred deliberately: graph/DAG recovery and visualization on `graphlib`;
  known-hard FRP problems (diamond glitches, feedback cycles, teardown order); a
  deterministic "await next update" signal to replace `testing.tick` (now the only
  resident of `without.testing`, which sharpens this item).

Documentation debt (carried forward): `BIG_IDEA.md` and the early checkpoints
still call the model an "async reducer"; it is more accurately an **async scan**,
and the functional-core / imperative-shell, connection-as-stream, and
lifecycle-as-a-portable-value framings should be folded into `BIG_IDEA.md` when
it is next revised.
