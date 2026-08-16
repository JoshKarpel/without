# Alternatives

This page is a feature-by-feature register of where `without-http` stands
against the clients people actually reach for:
[httpx](https://www.python-httpx.org/), [aiohttp](https://docs.aiohttp.org/),
and [niquests](https://github.com/jawah/niquests), and of the server against
the ASGI servers: [uvicorn](https://github.com/encode/uvicorn),
[hypercorn](https://hypercorn.readthedocs.io/), and
[granian](https://github.com/emmett-framework/granian). It is maintained by
hand against each project's own documentation, and it is a roadmap as much as a
comparison. In the `without-http` column, `unwritten` marks a gap whose fix is
a composition against an interface that already ships (a middleware, a
`Content` producer, a `Connect`), with the cell linking to its shape in
[how a gap closes](#how-a-gap-closes-here); a bare `no` there is either genuine
new mechanism or a position taken deliberately
([what stays absent](#what-stays-absent)).

How to read the cells: `yes` and `no` record what each project's
documentation, issue tracker, changelog, or source says, and cells link their
source so the analysis is reproducible; a dash means none of those settled it,
and the page stops at what can be cited. Third-party add-ons are named where
they are the well-known answer.

The structural difference to keep in mind while reading is where each library
puts policy. httpx, aiohttp, and niquests are client *objects*: redirects,
cookies, auth, and timeouts are constructor flags, and the pool, the policy,
and the API arrive as one artifact. Here a client is one function type,
`ClientRequest -> Awaitable[ClientResponse]`; the pool is one implementation of
it, and every policy is a middleware that maps a client to a client. So several
rows below that read as missing *features* are really missing *compositions*:
the mechanism exists and nobody has written the ten-line middleware yet. The
honest flip side is that several others (a WebSocket client, HTTP/3) are real
mechanisms this package does not have.

## The client

### Protocols

| | without-http | httpx | aiohttp | niquests |
|---|---|---|---|---|
| HTTP/1.1 | yes | yes | yes | yes |
| HTTP/2 | yes, [on by default (ALPN)](index.md#connection-pooling) | [opt-in](https://www.python-httpx.org/http2/) (`http2=True`, optional extra) | no | [yes](https://github.com/jawah/niquests) |
| HTTP/2 cleartext, prior knowledge | yes ([`force_http2_cleartext`](index.md#connection-pooling)) | yes ([`http1=False, http2=True`](https://www.encode.io/httpcore/http2/)) | no | [yes](https://github.com/jawah/niquests) |
| HTTP/3 (QUIC) | no; [new mechanism](#how-a-gap-closes-here) | no | no | [yes](https://github.com/jawah/niquests) |
| WebSocket client | no; [new mechanism](#how-a-gap-closes-here) | no | [yes](https://docs.aiohttp.org/en/stable/client_quickstart.html#websockets) | [yes](https://github.com/jawah/niquests), including over HTTP/2 and HTTP/3 |
| Server-sent events | unwritten; [a stream transform](#how-a-gap-closes-here) | third party ([`httpx-sse`](https://github.com/florimondmanca/httpx-sse)) | third party ([`aiohttp-sse-client`](https://github.com/rtfol/aiohttp-sse-client)) | [yes](https://github.com/jawah/niquests) |

### Connections

| | without-http | httpx | aiohttp | niquests |
|---|---|---|---|---|
| Pooling with keep-alive | yes, [keyed by origin](index.md#connection-pooling) | yes | yes | yes |
| Default pool bounds | [unbounded per host](index.md#connection-pooling), by stated position | [100 total, 20 keep-alive](https://www.python-httpx.org/advanced/resource-limits/) | [100 total, no per-host limit](https://docs.aiohttp.org/en/stable/client_reference.html#tcpconnector) | [10 hosts, 10 per host](https://niquests.readthedocs.io/en/latest/api.html) (requests defaults) |
| TCP keepalive probing | [on by default](index.md#tcp-keepalive) | off; `socket_options` on the transport | — | [protocol pings for h2/h3](https://niquests.readthedocs.io/en/latest/api.html) |
| Happy Eyeballs (dual-stack connect) | [on by default](index.md#connection-pooling) (250 ms, via aiohappyeyeballs; `tcp_connect()` tunes it) | no | [yes](https://docs.aiohttp.org/en/stable/client_reference.html#tcpconnector) | [yes](https://github.com/jawah/niquests) |
| DNS caching or custom resolution | resolution injectable ([`tcp_connect(resolve=)`](index.md#connection-pooling)); a cache is unwritten, [a `Resolve` wrapper](#how-a-gap-closes-here) | — | [cache with TTL](https://docs.aiohttp.org/en/stable/client_reference.html#tcpconnector) | [DoH, DoT, DoQ, DNSSEC, custom resolvers](https://github.com/jawah/niquests) |
| Unix domain sockets | unwritten; [a `Connect`](#how-a-gap-closes-here) | yes ([`uds=`](https://www.python-httpx.org/advanced/transports/)) | yes ([`UnixConnector`](https://docs.aiohttp.org/en/stable/client_reference.html#unixconnector)) | — |
| Proxies | unwritten; [a `Connect`](#how-a-gap-closes-here) | [HTTP(S); SOCKS via extra](https://www.python-httpx.org/advanced/proxies/) | [HTTP](https://docs.aiohttp.org/en/stable/client_advanced.html#proxy-support); SOCKS [third party](https://github.com/romis2012/aiohttp-socks) | [HTTP(S) and SOCKS](https://github.com/jawah/niquests) |

### Requests and responses

| | without-http | httpx | aiohttp | niquests |
|---|---|---|---|---|
| Streaming bodies, both directions | [yes](index.md#buffered-and-streaming-both-directions) | [yes](https://www.python-httpx.org/quickstart/) | [yes](https://docs.aiohttp.org/en/stable/client_quickstart.html) | yes |
| Concurrent duplex (early responses, bidi) | yes, [full bidi](index.md#duplex-and-bidirectional-streaming) | [no](https://github.com/encode/httpx/issues/877), request sent fully first | no; WebSockets are the duplex path | [early responses](https://github.com/jawah/niquests) |
| Response trailers | [yes](index.md#trailers) | no | [no public API](https://github.com/aio-libs/aiohttp/issues/8452) | [yes](https://github.com/jawah/niquests) |
| Response decompression | yes, opt-in ([`decompress()` middleware](index.md#client-middleware): gzip, zstd, brotli; injectable coding table) | [yes](https://www.python-httpx.org/) (gzip; brotli/zstd extras) | yes ([`auto_decompress`](https://docs.aiohttp.org/en/stable/client_reference.html)) | yes |
| Request compression | yes, opt-in ([`gzip_compress()`/`zstd_compress()`/`brotli_compress()` middleware](index.md#client-middleware), per client or per call) | — | yes ([`compress=`](https://docs.aiohttp.org/en/stable/client_reference.html), deflate, off by default) | — |
| Multipart and form encoding | unwritten; [a `Content` producer](#how-a-gap-closes-here) | [yes](https://www.python-httpx.org/quickstart/) | [yes](https://docs.aiohttp.org/en/stable/client_quickstart.html) | yes |
| Headers sent unbidden | [none](#what-stays-absent) (only `host` and body framing) | user-agent, accept, accept-encoding | user-agent and friends | requests-compatible set |

### Policy

| | without-http | httpx | aiohttp | niquests |
|---|---|---|---|---|
| Timeouts | [opt-in, per phase](index.md#timeouts), inactivity-based, typed errors | [5 s default](https://www.python-httpx.org/advanced/timeouts/) across four phases | [5 min total default](https://docs.aiohttp.org/en/stable/client_reference.html#clienttimeout) | [on by default, per method](https://niquests.readthedocs.io/en/latest/user/advanced.html) (30 s reads, 120 s writes) |
| Redirects | opt-in middleware ([`follow_redirects()`](index.md#client-middleware)) | built-in, [off by default](https://www.python-httpx.org/compatibility/) | built-in, on by default | built-in, on by default |
| Cookies | [explicit jar](cookies.md), attached by middleware | jar on the client | jar on the session | jar on the session |
| Auth helpers | unwritten; [middleware-shaped](#how-a-gap-closes-here) | [Basic and Digest](https://www.python-httpx.org/quickstart/) | [Basic](https://docs.aiohttp.org/en/stable/client_reference.html#basicauth) | Basic and Digest |
| Retries | unwritten; [middleware-shaped](#how-a-gap-closes-here) | [connect phase only](https://www.python-httpx.org/advanced/transports/) (`HTTPTransport(retries=)`) | third party ([`aiohttp-retry`](https://github.com/inyutin/aiohttp_retry)) | off by default; [first-class `retries=`](https://niquests.readthedocs.io/en/latest/user/advanced.html) (urllib3 `Retry`) |
| Extension mechanism | [function composition over `Client`](index.md#client-middleware) | [event hooks](https://www.python-httpx.org/advanced/event-hooks/), custom transports | [client middlewares](https://docs.aiohttp.org/en/stable/client_reference.html) | requests-style hooks |
| Certificate revocation, OS truststore | no (inject `ssl_context_factory`) | — | — | [OCSP, CRL, OS truststore](https://github.com/jawah/niquests) |
| Synchronous API | [no, by position](#what-stays-absent) | yes | no | [yes](https://github.com/jawah/niquests) |

### Testing

| | without-http | httpx | aiohttp | niquests |
|---|---|---|---|---|
| Canned responses | [`mock_client`](testing.md) | [`MockTransport`](https://www.python-httpx.org/advanced/transports/) | third party ([`aioresponses`](https://github.com/pnuckowski/aioresponses)) | — |
| Drive an app in memory | [`asgi_client`](testing.md) | [`ASGITransport`, `WSGITransport`](https://www.python-httpx.org/advanced/transports/) | [test server on a real socket](https://docs.aiohttp.org/en/stable/testing.html) | — |
| Real wire protocols, no socket | [`loopback_client`, `pipe`](testing.md) | — | — | — |

The last row is the one nothing else offers: the full h11/h2/wsproto stack over
an in-memory pipe, so a test exercises real framing without a port. See
[Testing](testing.md) for what each in-memory client covers and what none can.

### How a gap closes here

Every `unwritten` cell above has a known shape, because the interface it plugs
into already exists. That is the claim this page exists to test, so it is
worth being specific:

| Gap | The shape it takes |
|---|---|
| Multipart and form upload | A `Content` producer beside `json_content`; the encoding travels with its `content-type`. Parsing on the receiving side is a `without-web` extractor, not this package's concern. |
| Auth | Basic and bearer are `add_headers` one-liners today; Digest is a looping middleware with the same shape as `follow_redirects`. |
| Retries | A middleware that re-invokes its inner `Client`, rewriting the request's `Timeout` per attempt. The budget already rides on the request value so an attempt can shorten it. |
| Proxies, Unix sockets, local address | `Connect` implementations. The pool takes `Connect` at construction; none of these are written, but none needs a new interface (`tcp_connect` is the shape, already shipped). |
| DNS caching | A `Resolve` wrapper owning the cache, injected as `tcp_connect(resolve=...)`, so resolution policy stays with the caller instead of inside the pool. The interface ships; the cache does not, because `getaddrinfo` hides record TTLs, making any staleness bound the caller's policy to choose. |
| Server-sent events | A parser from a byte stream to typed events that composes over any response body. It needs nothing from the client, so it may not even live in this package. |
| WebSocket client | A real addition: a new rim API over `wsproto`. The server-side wire mapping (`ws_wire`) exists; the client half and its API shape do not. |
| HTTP/3 | A new wire module over a QUIC implementation, following the sans-IO pattern `h11_wire`/`h2_wire` set. The largest item on this page. |

The pattern in that column is the point. In a client object, each of these is a
feature request against the object; here each is a middleware, a `Content`
producer, a `Connect` implementation, or a stream transform, written against an
interface that already ships. The two that break the pattern (a WebSocket
client, HTTP/3) are honestly new mechanism, and they are the expensive ones.

### What stays absent

Positions, not gaps, each with its cost named:

- **No synchronous API.** httpx and niquests serve scripts and the REPL
  directly; here that audience pays an `asyncio.run` wrapper. The single
  `Client` function type is the thing being protected: a sync twin would be a
  second surface for every middleware.
- **No default timeouts.** A hung peer blocks until cancelled unless a
  `Timeout` is armed. The budget is the caller's policy
  ([Timeouts](index.md#timeouts)); a transport-chosen default is a decision
  taken from them. httpx's 5 s default is the opposite position, and it is the
  friendlier one on day one.
- **No headers sent unbidden.** No user-agent, no `accept-encoding`. Requests
  say exactly what the caller said; the cost is that peers which vary on
  user-agent see an empty one until you add it. Composing the `decompress`
  middleware is how a client opts into an `accept-encoding` offer, which is the
  position holding, not an exception to it.
- **Unbounded per-host connections by default.** Mirrors the server's choice to
  let OS backpressure govern ([Connection pooling](index.md#connection-pooling));
  the cost is that a runaway caller opens sockets until the OS objects, where
  httpx would have queued at 100.

## The server

Read this half with one fact in front: the ASGI boundary already did the work
this page exists to check. A `without` app is a plain ASGI app, so the server
is swappable wholesale, and for raw HTTP/1.1 throughput the obvious choice is
uvicorn or granian, where speed is the whole game and a pure-Python server
should not pretend otherwise. What serving here earns its keep on is protocol
coverage (HTTP/2 end-to-end, including cleartext prior knowledge, which
uvicorn refuses), typed per-connection bounds, and `served_pipe`, which runs
this exact server over an in-memory pipe so wire-level tests need no port.

aiohttp's server is absent from the tables for the same reason it is
instructive: it is a framework with its own application type, not an ASGI
server, so nothing written for it travels. The narrow waist is what makes this
whole column swappable at all.

### Protocols and applications

| | without-http | uvicorn | hypercorn | granian |
|---|---|---|---|---|
| HTTP/1.1 | yes | yes ([`httptools` or `h11`](https://github.com/encode/uvicorn)) | yes | yes |
| HTTP/2 | [yes](index.md#server) | [no](https://github.com/encode/uvicorn) | [yes](https://hypercorn.readthedocs.io/en/latest/discussion/http2.html) | [yes](https://github.com/emmett-framework/granian) |
| HTTP/2 cleartext, prior knowledge | yes ([preface sniffed](index.md#server)) | no | [upgrade only](https://hypercorn.readthedocs.io/en/latest/discussion/http2.html) (no upgrade or ALPN reads as HTTP/1.1) | undocumented (`--http 2` reportedly serves it) |
| HTTP/3 (QUIC) | no | no | [optional](https://pypi.org/project/hypercorn/) (`hypercorn[h3]`, aioquic) | [no](https://github.com/emmett-framework/granian) |
| WebSockets | [over HTTP/1.1](index.md#server) | yes ([`websockets` or `wsproto`](https://uvicorn.dev/settings/)) | [over HTTP/1 and HTTP/2](https://pypi.org/project/hypercorn/) | [yes](https://github.com/emmett-framework/granian) |
| ASGI lifespan | yes, with the no-lifespan fallback | [yes](https://uvicorn.dev/settings/) | yes | yes |
| WSGI apps | no | — | [yes](https://pypi.org/project/hypercorn/) | [yes](https://github.com/emmett-framework/granian) |
| Other app interfaces | any ASGI app | any ASGI app | any ASGI or WSGI app | [RSGI](https://github.com/emmett-framework/granian) (its native interface) |

### ASGI extensions

The [ASGI extensions](https://asgi.readthedocs.io/en/latest/extensions.html)
are the sharpest per-server comparison, because each is a named optional
capability a server either implements or does not.

| Extension | without-http | uvicorn | hypercorn | granian |
|---|---|---|---|---|
| `websocket.http.response` (denial response) | yes ([`ws_wire`](https://github.com/JoshKarpel/without/blob/main/packages/without-http/src/without_http/ws_wire.py)) | [yes](https://github.com/encode/uvicorn/pull/1916) | [yes](https://github.com/pgjones/hypercorn/blob/main/src/hypercorn/protocol/ws_stream.py) | [yes](https://github.com/emmett-framework/granian/issues/793) |
| `http.response.early_hint` (103) | yes, [both protocols](https://github.com/JoshKarpel/without/blob/main/packages/without-http/src/without_http/h11_wire.py) | no | [yes](https://github.com/pgjones/hypercorn/blob/main/src/hypercorn/protocol/http_stream.py) | [pending](https://github.com/emmett-framework/granian/issues/93) |
| `http.response.trailers` | no ([raises](https://github.com/JoshKarpel/without/blob/main/packages/without-http/src/without_http/server.py)) | — | [yes](https://github.com/pgjones/hypercorn/blob/main/src/hypercorn/protocol/http_stream.py) | [pending](https://github.com/emmett-framework/granian/issues/93) |
| `http.response.push` (HTTP/2 server push) | no | no HTTP/2 | [yes](https://github.com/pgjones/hypercorn/blob/main/src/hypercorn/protocol/http_stream.py) | [no, blocked upstream](https://github.com/emmett-framework/granian/issues/93) |
| `http.response.pathsend` | no | — | — | [yes](https://github.com/emmett-framework/granian/issues/82) |
| `http.response.zerocopysend` | no | — | — | [declined](https://github.com/emmett-framework/granian/issues/93) (Rust/Python fd sharing) |
| `http.response.debug` | no | — | — | — |
| `tls` | no; the app side [parses it](https://github.com/JoshKarpel/without/blob/main/packages/without-asgi/src/without_asgi/scope.py) when a server supplies it | — | — | [tracked](https://github.com/emmett-framework/granian/issues/788) |

Two honest notes on the `without-http` column. First, the server implements
denial responses and early hints but advertises no `extensions` mapping on the
scopes it builds (all three scope builders set `extensions=None`), so a
third-party ASGI framework that checks the scope before using an extension, as
the spec tells it to, will never use them; a `without-asgi` app speaks the
typed vocabulary directly and is unaffected. That is a real gap with a
one-line shape. Second, the `no` cells are loud rather than silent:
`without-asgi` types every one of these messages, and the wire layers raise
`NotImplementedError` on the unsupported ones instead of dropping them.

### Operations

| | without-http | uvicorn | hypercorn | granian |
|---|---|---|---|---|
| Multiprocess workers | no | [`--workers`](https://uvicorn.dev/settings/) | [`workers`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [`--workers`](https://github.com/emmett-framework/granian), plus Rust runtime threads |
| Worker lifecycle (max requests, respawn) | no | [`--limit-max-requests`](https://uvicorn.dev/settings/) (+ jitter) | [`max_requests`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [lifetime, max-RSS, respawn on failure](https://github.com/emmett-framework/granian) |
| Auto-reload for development | no | [`--reload`](https://uvicorn.dev/settings/) | [`use_reloader`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [`--reload`](https://github.com/emmett-framework/granian) (extra) |
| Event loop choice | caller's (`asyncio.Runner(loop_factory=...)`, e.g. uvloop) | [`--loop`](https://uvicorn.dev/settings/) (uvloop) | [`worker_class`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) (asyncio, uvloop, trio) | [`--loop`](https://github.com/emmett-framework/granian) (asyncio, rloop, uvloop, winloop) |
| TLS | any `ssl.SSLContext`; [`server_ssl_context` helper with ALPN](index.md#server) | [cert/key flags](https://uvicorn.dev/settings/) | [`certfile`/`keyfile`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [cert/key flags, TLS 1.3 default](https://github.com/emmett-framework/granian) |
| Client certificates (mTLS) | via your `ssl.SSLContext` | [`--ssl-cert-reqs`, `--ssl-ca-certs`](https://uvicorn.dev/settings/) | [`verify_mode`, `ca_certs`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [`--ssl-client-verify`, `--ssl-ca`, CRLs](https://github.com/emmett-framework/granian) |
| Proxy headers (`x-forwarded-*`) | unwritten; ASGI-middleware-shaped | [on by default, trusted-IP gated](https://uvicorn.dev/settings/) | [`ProxyFixMiddleware`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/proxy_fix.html), shipped as ASGI middleware | — |
| Access logging | unwritten; ASGI-middleware-shaped | [yes](https://uvicorn.dev/settings/) | [`accesslog` + format](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [opt-in, custom format](https://github.com/emmett-framework/granian) |
| Static file serving | no | no | no | [yes](https://github.com/emmett-framework/granian) |
| Metrics | live [`in_flight` count on the `Server` value](index.md#server) | no | — | [Prometheus exporter](https://github.com/emmett-framework/granian) |
| Graceful shutdown | [drain on exit; budget composed by the caller](index.md#server) | [`--timeout-graceful-shutdown`](https://uvicorn.dev/settings/) | [`graceful_timeout`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [kill timeout](https://github.com/emmett-framework/granian) |

### Limits and robustness

| | without-http | uvicorn | hypercorn | granian |
|---|---|---|---|---|
| Accept backlog | [`max_pending_connections`](index.md#server) | [`--backlog`](https://uvicorn.dev/settings/) (2048) | [`backlog`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) (100) | [`--backlog`](https://github.com/emmett-framework/granian) (1024) |
| Concurrency limit with shedding | [`limit_concurrent_requests`](index.md#server) app middleware (503) | [`--limit-concurrency`](https://uvicorn.dev/settings/) (503) | — | [`--backpressure`](https://github.com/emmett-framework/granian) (pauses accept loop) |
| Idle / keep-alive timeout | [`idle_timeout`](index.md#server), off by default, also bounds slowloris and idle WebSockets | [`--timeout-keep-alive`](https://uvicorn.dev/settings/) (5 s) | [`keep_alive_timeout`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) (5 s) | [HTTP/2 keep-alive tuning](https://github.com/emmett-framework/granian) |
| Request body cap | [`limit_request_body`](index.md#server) app middleware (413) | — | — | — |
| Request line + header cap | h11's own incomplete-event bound | [`--h11-max-incomplete-event-size`](https://uvicorn.dev/settings/) | [`h11_max_incomplete_size`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [header size options](https://github.com/emmett-framework/granian) |
| HTTP/2 stream and frame tuning | [`max_concurrent_streams`, flow-control-bounded body buffering](index.md#server) | no HTTP/2 | [stream, header-list, and frame size options](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [window, stream, and frame size options](https://github.com/emmett-framework/granian) |
| Rapid Reset mitigation (CVE-2023-44487) | [`max_stream_resets`](index.md#server), reset cancels the app task | no HTTP/2 | — | — |
| WebSocket message cap | [`max_websocket_message_bytes`](index.md#server) | [`--ws-max-size`](https://uvicorn.dev/settings/) (16 MB) | [`websocket_max_message_size`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) (16 MiB) | — |

Two readings of these tables coexist. As a deployment checklist, the
operations table is the honest one: workers, reload, proxy headers, and access
logs are what a production rollout has to bring, and here they belong to the
process manager and the composition root rather than to the server, which is a
position the deployment still has to staff. As a boundary-design comparison,
the limits table is the interesting one: the other servers put shedding and
body caps in server configuration, where they exist once per server
implementation; here `limit_concurrent_requests` and `limit_request_body` are
app-side middleware, written once against the ASGI boundary and carried to any
server underneath, including uvicorn. That is the layering argument of this
whole stack in one row: a limit that is a server flag protects only apps on
that server, while a limit that is a composition travels with the app.
hypercorn takes the same position for proxy headers, shipping
`ProxyFixMiddleware` as ASGI middleware rather than a server flag, which is
the position working from the other direction. The two `unwritten` cells in
the operations table are the server-side mirror of the client's unwritten
compositions: a proxy-header middleware and an access-log middleware over the
ASGI boundary would close both rows generically, for any server underneath,
and are the obvious next generic middleware to ship.
