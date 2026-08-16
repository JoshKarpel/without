# Alternatives

This page is a feature-by-feature register of where `without-http` stands
against the clients people actually reach for:
[httpx](https://www.python-httpx.org/), [aiohttp](https://docs.aiohttp.org/),
and [niquests](https://github.com/jawah/niquests), and of the server against
the ASGI servers: [uvicorn](https://github.com/encode/uvicorn),
[hypercorn](https://hypercorn.readthedocs.io/), and
[granian](https://github.com/emmett-framework/granian). It is maintained by
hand against each project's own documentation, and it is a roadmap as much as a
comparison: a `no` in the `without-http` column is either a gap this package
intends to close in its own style ([how a gap closes](#how-a-gap-closes-here))
or a position taken deliberately ([what stays absent](#what-stays-absent)).

How to read the cells: `yes` and `no` record what each project's documentation
says; a dash means its docs do not advertise the feature and this page did not
dig further. Third-party add-ons are named where they are the well-known
answer.

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
| HTTP/2 | yes, on by default (ALPN) | opt-in (`http2=True`, optional extra) | no | yes |
| HTTP/2 cleartext, prior knowledge | yes (`force_http2_cleartext`) | — | no | yes |
| HTTP/3 (QUIC) | no | no | no | yes |
| WebSocket client | no | no | yes | yes, including over HTTP/2 and HTTP/3 |
| Server-sent events | no | third party (`httpx-sse`) | third party | yes |

### Connections

| | without-http | httpx | aiohttp | niquests |
|---|---|---|---|---|
| Pooling with keep-alive | yes, keyed by origin | yes | yes | yes |
| Default pool bounds | unbounded per host, by stated position | 100 total, 20 keep-alive | 100 total, no per-host limit | — |
| TCP keepalive probing | on by default | — | — | — |
| Happy Eyeballs (dual-stack connect) | no | — | yes | yes |
| DNS caching or custom resolution | no | — | cache with TTL | DoH, DoT, DoQ, DNSSEC, custom resolvers |
| Unix domain sockets | no | yes (`uds=`) | yes (`UnixConnector`) | — |
| Proxies | no | HTTP(S); SOCKS via extra | HTTP; SOCKS third party | HTTP(S) and SOCKS |

### Requests and responses

| | without-http | httpx | aiohttp | niquests |
|---|---|---|---|---|
| Streaming bodies, both directions | yes | yes | yes | yes |
| Concurrent duplex (early responses, bidi) | yes, full bidi | — | — | early responses |
| Response trailers | yes | — | — | yes |
| Automatic decompression | no | yes (gzip; brotli/zstd extras) | yes | yes |
| Multipart and form encoding | no | yes | yes | yes |
| Headers sent unbidden | none (only `host` and body framing) | user-agent, accept, accept-encoding | user-agent and friends | requests-compatible set |

### Policy

| | without-http | httpx | aiohttp | niquests |
|---|---|---|---|---|
| Timeouts | opt-in, per phase, inactivity-based, typed errors | 5 s default across four phases | 5 min total default | — |
| Redirects | opt-in middleware (`follow_redirects()`) | built-in, off by default | built-in, on by default | built-in, on by default |
| Cookies | explicit jar, attached by middleware | jar on the client | jar on the session | jar on the session |
| Auth helpers | no | Basic and Digest | Basic | Basic and Digest |
| Retries | no | connect phase only (`HTTPTransport(retries=)`) | third party | — |
| Extension mechanism | function composition over `Client` | event hooks, custom transports | client middlewares | requests-style hooks |
| Certificate revocation, OS truststore | no (inject `ssl_context_factory`) | — | — | OCSP, CRL, OS truststore |
| Synchronous API | no, by position | yes | no | yes |

### Testing

| | without-http | httpx | aiohttp | niquests |
|---|---|---|---|---|
| Canned responses | `mock_client` | `MockTransport` | — | — |
| Drive an app in memory | `asgi_client` | `ASGITransport`, `WSGITransport` | test server on a real socket | — |
| Real wire protocols, no socket | `loopback_client`, `pipe` | — | — | — |

The last row is the one nothing else offers: the full h11/h2/wsproto stack over
an in-memory pipe, so a test exercises real framing without a port. See
[Testing](testing.md) for what each in-memory client covers and what none can.

### How a gap closes here

Every `no` above that is planned rather than positional has a known shape,
because the interface it plugs into already exists. That is the claim this page
exists to test, so it is worth being specific:

| Gap | The shape it takes |
|---|---|
| Automatic decompression | A `ClientMiddleware`: inject `accept-encoding` outbound, wrap the response body stream in a decoding transform inbound. Opt-in, so the transport never silently rewrites bytes. |
| Multipart and form upload | A `Content` producer beside `json_content`; the encoding travels with its `content-type`. Parsing on the receiving side is a `without-web` extractor, not this package's concern. |
| Auth | Basic and bearer are `add_headers` one-liners today; Digest is a looping middleware with the same shape as `follow_redirects`. |
| Retries | A middleware that re-invokes its inner `Client`, rewriting the request's `Timeout` per attempt. The budget already rides on the request value so an attempt can shorten it. |
| Proxies, Unix sockets, local address | `Connect` implementations. The pool takes `Connect` at construction; none of these are written, but none needs a new interface. |
| Happy Eyeballs | Expose asyncio's own `happy_eyeballs_delay` through the connect path: a knob, not a mechanism. |
| DNS caching | A `Connect` wrapper owning the cache, so resolution policy stays with the caller instead of inside the pool. |
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
  user-agent see an empty one until you add it.
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
| HTTP/1.1 | yes | yes (`httptools` or `h11`) | yes | yes |
| HTTP/2 | yes | no | yes | yes |
| HTTP/2 cleartext, prior knowledge | yes (preface sniffed) | no | — | — |
| HTTP/3 (QUIC) | no | no | optional (`hypercorn[h3]`, aioquic) | no |
| WebSockets | over HTTP/1.1 | yes (`websockets` or `wsproto`) | over HTTP/1 and HTTP/2 | yes |
| Early hints (103) | yes, both protocols | — | — | — |
| ASGI lifespan | yes, with the no-lifespan fallback | yes | yes | yes |
| WSGI apps | no | — | yes | yes |
| Other app interfaces | any ASGI app | any ASGI app | any ASGI or WSGI app | RSGI (its native interface) |

### Operations

| | without-http | uvicorn | hypercorn | granian |
|---|---|---|---|---|
| Multiprocess workers | no | `--workers` | `workers` | `--workers`, plus Rust runtime threads |
| Worker lifecycle (max requests, respawn) | no | `--limit-max-requests` (+ jitter) | `max_requests` | lifetime, max-RSS, respawn on failure |
| Auto-reload for development | no | `--reload` | `use_reloader` | `--reload` (extra) |
| Event loop choice | caller's (`asyncio.Runner(loop_factory=...)`, e.g. uvloop) | `--loop` (uvloop) | `worker_class` (asyncio, uvloop, trio) | `--loop` (asyncio, rloop, uvloop, winloop) |
| TLS | any `ssl.SSLContext`; `server_ssl_context` helper with ALPN | cert/key flags | `certfile`/`keyfile` | cert/key flags, TLS 1.3 default |
| Client certificates (mTLS) | via your `ssl.SSLContext` | `--ssl-cert-reqs`, `--ssl-ca-certs` | `verify_mode`, `ca_certs` | `--ssl-client-verify`, `--ssl-ca`, CRLs |
| Proxy headers (`x-forwarded-*`) | no | on by default, trusted-IP gated | — | — |
| Access logging | no | yes | `accesslog` + format | opt-in, custom format |
| Static file serving | no | no | no | yes |
| Metrics | live `in_flight` count on the `Server` value | no | — | Prometheus exporter |
| Graceful shutdown | drain on exit; budget composed by the caller | `--timeout-graceful-shutdown` | `graceful_timeout` | kill timeout |

### Limits and robustness

| | without-http | uvicorn | hypercorn | granian |
|---|---|---|---|---|
| Accept backlog | `max_pending_connections` | `--backlog` (2048) | `backlog` (100) | `--backlog` (1024) |
| Concurrency limit with shedding | `limit_concurrent_requests` app middleware (503) | `--limit-concurrency` (503) | — | `--backpressure` (pauses accept loop) |
| Idle / keep-alive timeout | `idle_timeout`, off by default, also bounds slowloris and idle WebSockets | `--timeout-keep-alive` (5 s) | `keep_alive_timeout` (5 s) | HTTP/2 keep-alive tuning |
| Request body cap | `limit_request_body` app middleware (413) | — | — | — |
| Request line + header cap | h11's own incomplete-event bound | `--h11-max-incomplete-event-size` | `h11_max_incomplete_size` | header size options |
| HTTP/2 stream and frame tuning | `max_concurrent_streams`, flow-control-bounded body buffering | no HTTP/2 | stream, header-list, and frame size options | window, stream, and frame size options |
| Rapid Reset mitigation (CVE-2023-44487) | `max_stream_resets`, reset cancels the app task | no HTTP/2 | — | — |
| WebSocket message cap | `max_websocket_message_bytes` | `--ws-max-size` (16 MB) | `websocket_max_message_size` (16 MiB) | — |

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
