# CLAUDE.md — without-asgi

## Reference the ASGI spec

This package is an ASGI boundary adapter, so its scope and message types must
track the ASGI specification:

- Index: https://asgi.readthedocs.io/en/latest/index.html
- HTTP & WebSocket scopes: https://asgi.readthedocs.io/en/latest/specs/www.html
- Lifespan scope: https://asgi.readthedocs.io/en/latest/specs/lifespan.html

The published docs are built from rST in the `django/asgiref` repo under
`specs/` (e.g.
https://raw.githubusercontent.com/django/asgiref/main/specs/www.rst), which is a
useful fallback when readthedocs rate-limits. Before changing a scope or event
type, read the relevant spec page and work from the field list there.

## Coverage

The scope dataclasses in `scope.py` (`HttpScope`, `WebsocketScope`,
`LifespanScope`, and the shared `Asgi`) model every field the spec defines, with
per-field docstrings quoting the spec. The `parse_*` functions enforce the
required-versus-optional split and the spec defaults. `type` is consumed by the
`parse_scope` discriminator rather than stored on a dataclass.

The HTTP and WebSocket event types are modeled in both directions: inbound
(`parse_inbound`, `parse_websocket_inbound`) and outbound (`encode_outbound`,
`encode_websocket_outbound`), with lifespan events on their own pair. App-built
outbound events carry the spec's optional-field defaults on the dataclass;
parser-built scopes and inbound events do not (the parser always supplies every
field, so a default there would mask a bug).

Every extension from the reference docs is modeled: HTTP/2 server push, zero-copy
send, path send, early hints, response trailers (with the `trailers` flag on
`ResponseStart`), debug, the WebSocket denial response, and TLS (`parse_tls`
reads the `tls` extension out of a scope's generic `extensions` mapping).

Sourced from the spec on 2026-06-23 (HTTP & WebSocket spec version 2.5, lifespan
spec version 2.0, TLS extension version 0.2). Two easy-to-miss details when
revising: the `asgi.spec_version` default differs by scope (HTTP/WebSocket
default `"2.0"`, lifespan `"1.0"`), and the whole `asgi` mapping is tolerated
missing, defaulting `version` to `"2.0"` as the spec's Applications section
instructs, because real producers omit it (starlette's `TestClient` sends a
lifespan scope with no `asgi` key).

## Where built-in middleware lives

`without-asgi` owns the `Middleware` / `wrap` vocabulary (`routing.py`) and
re-exports the generic `stack` from `without` core (`stack` composes any
`(handler, *context) -> handler` middleware, server or client, so it lives in core),
making it the default home for built-in middleware. The placement rule across the
three HTTP packages: **a middleware lives in the lowest layer whose vocabulary it
needs, and in the package that owns the exchange shape it wraps.**

- **`without-asgi`** holds transport- and router-agnostic HTTP middleware: anything
  that needs only the `HttpScope` / `Inbound` / `Outbound` / `Response` vocabulary,
  so it works under any router and any transport (HTTP/1.1, HTTP/2, HTTP/3, and even
  uvicorn), because it wraps the handler rather than the socket. `limit_concurrent_requests`
  and `limit_request_body` (a `413` body-size cap) live here; natural neighbors are
  default response headers, gzip/decompression, a per-request timeout, request-ID
  injection, trust-gated proxy headers, and CORS preflight. Access logging has the
  same shape and deliberately does not ship: what belongs in the line is deployment
  policy (see the server positions in `docs/without-http/alternatives.md`).
- **`without-web`** holds route-aware middleware: anything keyed on the matched
  route or that maps exceptions to responses, plus route-scoped middleware and
  OpenAPI-aware pieces. Only that layer can see route metadata.
- **`without-http`** holds the **client** middleware, since a `Client`
  (`ClientRequest -> ClientResponse`) is a `Processor` too and lives there
  (`add_headers`, `basic_auth` / `bearer_auth`, `follow_redirects`, `cookies`
  over a caller-owned `CookieJar`, and the content codings `decompress` /
  `gzip_compress` / `zstd_compress` / `brotli_compress`). Its server side keeps only the
  concerns that **cannot** be
  middleware because they run below the app: the wire protocols and TLS. (It used to
  also own a connection-admission cap; that was dropped in favour of the kernel
  listen backlog plus the `limit_concurrent_requests` middleware.)

Server and client share the *same* `Middleware` / `stack` vocabulary, so a concept
like decompression appears as parallel server-side (here) and client-side
(`without-http`) instances that differ only in the request/response shape they wrap.
A middleware that must read the per-connection state is written directly as a
`(handler, state, scope) -> handler` function (handler first, so it matches the
generic `stack`'s `(handler, *context)` shape); one that only transforms streams by
scope uses `wrap`. Cross-request shared state (a rate-limit budget, a cache) is
built once when the middleware is constructed and captured in its closure, then
injected at app assembly, not reached into as a global.

## Note: `scope["state"]` is intentionally not surfaced

The spec's `state` namespace is the standardized mechanism for passing data from
the lifespan cycle into each request (the server passes a shallow copy into every
connection scope). `without` deliberately does not model it: `make_asgi_app`
threads lifespan-derived state to handlers explicitly through its internal
`_Cell`, so surfacing `scope["state"]` would be a redundant, parallel state path.
