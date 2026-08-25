# Alternatives

This page is a feature-by-feature register of where `without-http` stands
against the clients people usually reach for:
[httpx](https://www.python-httpx.org/), [aiohttp](https://docs.aiohttp.org/),
and [niquests](https://github.com/jawah/niquests), and of the server against
the ASGI servers: [uvicorn](https://github.com/encode/uvicorn),
[hypercorn](https://hypercorn.readthedocs.io/), and
[granian](https://github.com/emmett-framework/granian). It is maintained by
hand against each project's own documentation, and it is a roadmap as much as a
comparison. In the `without-http` column, `unwritten` marks a gap whose fix is
a composition against an interface that already ships (a middleware, a
`Connect`, a stream transform), with the cell linking to its shape in
[how a gap closes](#how-a-gap-closes-here); a bare `no` there is either genuine
new mechanism or a position taken deliberately
([what stays absent](#what-stays-absent)); and `declined` marks a capability
this project has decided against, with the reasoning on the linked issue.

How to read the cells: `yes` and `no` record what each project's
documentation, issue tracker, changelog, or source says, and cells link their
source so the analysis is reproducible; a dash means none of those settled it,
and the page stops at what can be cited. Third-party add-ons are named where
they are the well-known answer. A bare issue number in the `without-http`
column is this project's own tracker: an open one carries the design
constraints already settled for that gap, and a closed one records a position
taken and why.

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
| HTTP/3 (QUIC) | no; [new mechanism](#how-a-gap-closes-here), [#11](https://github.com/JoshKarpel/without/issues/11) | no | no | [yes](https://github.com/jawah/niquests) |
| WebSocket client | no; [new mechanism](#how-a-gap-closes-here), [#69](https://github.com/JoshKarpel/without/issues/69) | no | [yes](https://docs.aiohttp.org/en/stable/client_quickstart.html#websockets) | [yes](https://github.com/jawah/niquests), including over HTTP/2 and HTTP/3 |
| WebSockets over HTTP/2 (RFC 8441 extended CONNECT) | no, client or server; [#21](https://github.com/JoshKarpel/without/issues/21) | no WebSocket client | no HTTP/2 | [yes](https://github.com/jawah/niquests) |
| Server-sent events | yes; the format is [a pure stream transform](../without-asgi/sse.md) in `without-asgi`, and [`subscribe`](index.md#server-sent-events) here reconnects and resumes | third party ([`httpx-sse`](https://github.com/florimondmanca/httpx-sse)) | third party ([`aiohttp-sse-client`](https://github.com/rtfol/aiohttp-sse-client)) | [yes](https://github.com/jawah/niquests) |

### Connections

| | without-http | httpx | aiohttp | niquests |
|---|---|---|---|---|
| Pooling with keep-alive | yes, [keyed by origin](index.md#connection-pooling) | yes | yes | yes |
| Default pool bounds | [unbounded per host](index.md#connection-pooling), by stated position | [100 total, 20 keep-alive](https://www.python-httpx.org/advanced/resource-limits/) | [100 total, no per-host limit](https://docs.aiohttp.org/en/stable/client_reference.html#tcpconnector) | [10 hosts, 10 per host](https://niquests.readthedocs.io/en/latest/api.html) (requests defaults) |
| TCP keepalive probing | [on by default](index.md#tcp-keepalive) | off; `socket_options` on the transport | — | [protocol pings for h2/h3](https://niquests.readthedocs.io/en/latest/api.html) |
| Happy Eyeballs (dual-stack connect) | [on by default](index.md#connection-pooling) (250 ms, via aiohappyeyeballs; `tcp_connect()` tunes it) | no | [yes](https://docs.aiohttp.org/en/stable/client_reference.html#tcpconnector) | [yes](https://github.com/jawah/niquests) |
| DNS caching or custom resolution | resolution injectable ([`tcp_connect(resolve=)`](index.md#connection-pooling)); a cache is unwritten, [a `Resolve` wrapper](#how-a-gap-closes-here), [#75](https://github.com/JoshKarpel/without/issues/75) | — | [cache with TTL](https://docs.aiohttp.org/en/stable/client_reference.html#tcpconnector) | [DoH, DoT, DoQ, DNSSEC, custom resolvers](https://github.com/jawah/niquests) |
| Unix domain sockets | unwritten; [a `Connect`](#how-a-gap-closes-here), [#72](https://github.com/JoshKarpel/without/issues/72) | yes ([`uds=`](https://www.python-httpx.org/advanced/transports/)) | yes ([`UnixConnector`](https://docs.aiohttp.org/en/stable/client_reference.html#unixconnector)) | — |
| Proxies | unwritten; [a `Connect`](#how-a-gap-closes-here) for the `CONNECT` tunnel, [#73](https://github.com/JoshKarpel/without/issues/73) | [HTTP(S); SOCKS via extra](https://www.python-httpx.org/advanced/proxies/) | [HTTP](https://docs.aiohttp.org/en/stable/client_advanced.html#proxy-support); SOCKS [third party](https://github.com/romis2012/aiohttp-socks) | [HTTP(S) and SOCKS](https://github.com/jawah/niquests) |

### Requests and responses

| | without-http | httpx | aiohttp | niquests |
|---|---|---|---|---|
| Streaming bodies, both directions | [yes](index.md#buffered-and-streaming-both-directions) | [yes](https://www.python-httpx.org/quickstart/) | [yes](https://docs.aiohttp.org/en/stable/client_quickstart.html) | yes |
| Concurrent duplex (early responses, bidi) | yes, [full bidi](index.md#duplex-and-bidirectional-streaming) | [no](https://github.com/encode/httpx/issues/877), request sent fully first | no; WebSockets are the duplex path | [early responses](https://github.com/jawah/niquests) |
| Response trailers | [yes](index.md#trailers) | no | [no public API](https://github.com/aio-libs/aiohttp/issues/8452) | [yes](https://github.com/jawah/niquests) |
| Response decompression | yes, opt-in ([`decompress()` middleware](index.md#client-middleware): gzip, zstd, brotli; injectable coding table) | [yes](https://www.python-httpx.org/) (gzip; brotli/zstd extras) | yes ([`auto_decompress`](https://docs.aiohttp.org/en/stable/client_reference.html)) | yes |
| Request compression | yes, opt-in ([`gzip_compress()`/`zstd_compress()`/`brotli_compress()` middleware](index.md#client-middleware), per client or per call) | — | yes ([`compress=`](https://docs.aiohttp.org/en/stable/client_reference.html), deflate, off by default) | — |
| Multipart and form encoding | yes ([`form_content`, `multipart_content`](../without-asgi/index.md#content-a-body-and-what-it-is); multipart streams file parts) | [yes](https://www.python-httpx.org/quickstart/) | [yes](https://docs.aiohttp.org/en/stable/client_quickstart.html) | yes |
| Headers sent unbidden | [none](#what-stays-absent) (only `host` and body framing) | user-agent, accept, accept-encoding | user-agent and friends | requests-compatible set |

### Policy

| | without-http | httpx | aiohttp | niquests |
|---|---|---|---|---|
| Timeouts | [opt-in, per phase](index.md#timeouts), inactivity-based, typed errors | [5 s default](https://www.python-httpx.org/advanced/timeouts/) across four phases | [5 min total default](https://docs.aiohttp.org/en/stable/client_reference.html#clienttimeout) | [on by default, per method](https://niquests.readthedocs.io/en/latest/user/advanced.html) (30 s reads, 120 s writes) |
| Redirects | opt-in middleware ([`follow_redirects()`](index.md#client-middleware)) | built-in, [off by default](https://www.python-httpx.org/compatibility/) | built-in, on by default | built-in, on by default |
| Cookies | [explicit jar](cookies.md), attached by middleware | jar on the client | jar on the session | jar on the session |
| Auth helpers | Basic and bearer ([`basic_auth`, `bearer_auth`](index.md#client-middleware)); Digest unwritten, [middleware-shaped](#how-a-gap-closes-here), [#74](https://github.com/JoshKarpel/without/issues/74) | [Basic and Digest](https://www.python-httpx.org/quickstart/) | [Basic](https://docs.aiohttp.org/en/stable/client_reference.html#basicauth) | Basic and Digest |
| Retries | [no, by position](#what-stays-absent) (mechanism ships, policy stays with the caller; the one loop that ships is [SSE reconnection](index.md#server-sent-events), whose policy is on the wire) | [connect phase only](https://www.python-httpx.org/advanced/transports/) (`HTTPTransport(retries=)`) | third party ([`aiohttp-retry`](https://github.com/inyutin/aiohttp_retry)) | off by default; [first-class `retries=`](https://niquests.readthedocs.io/en/latest/user/advanced.html) (urllib3 `Retry`) |
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
| Digest auth ([#74](https://github.com/JoshKarpel/without/issues/74)) | A looping middleware with the same shape as `follow_redirects`, since Digest answers a challenge. The challenge-free schemes (`basic_auth`, `bearer_auth`) need no loop and ship. |
| Proxies ([#73](https://github.com/JoshKarpel/without/issues/73)), Unix sockets ([#72](https://github.com/JoshKarpel/without/issues/72)), local address | `Connect` implementations. The pool takes `Connect` at construction; none of these are written, but none needs a new interface (`tcp_connect` is the shape, already shipped). The proxy case has one edge that is *not* a `Connect`: a cleartext forward proxy wants the absolute URI in the request line, and the pool derives that target from the URL, so only the `CONNECT` tunnel fits the seam. |
| DNS caching ([#75](https://github.com/JoshKarpel/without/issues/75)) | A `Resolve` wrapper owning the cache, injected as `tcp_connect(resolve=...)`, so resolution policy stays with the caller instead of inside the pool. The interface ships; the cache does not, because `getaddrinfo` hides record TTLs, making any staleness bound the caller's policy to choose. |
| WebSocket client ([#69](https://github.com/JoshKarpel/without/issues/69)) | A real addition: a new rim API over `wsproto`. The server-side wire mapping (`ws_wire`) exists; the client half and its API shape do not. Carrying WebSockets over HTTP/2 ([#21](https://github.com/JoshKarpel/without/issues/21)) is a second, separable addition: `wsproto`'s high-level connection is welded to the h11 handshake, so both halves would drive `frame_protocol` directly. |
| HTTP/3 ([#11](https://github.com/JoshKarpel/without/issues/11)) | A new wire module over a QUIC implementation, following the sans-IO pattern `h11_wire`/`h2_wire` set. The largest item on this page, and the one whose constraints are most settled: an optional `aioquic` extra, TLS-only, opt-in through the existing `serving` entrypoint alongside the TCP listener, with `Alt-Svc` injection gated on this process actually terminating h3. |

The pattern in that column is the point. In a client object, each of these is a
feature request against the object; here each is a middleware, a `Connect`
implementation, or a stream transform, written against an interface that
already ships. The two that break the pattern (a WebSocket client, HTTP/3) are
honestly new mechanism, and they are the expensive ones.

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
  user-agent see an empty one until you add it, and some (the GitHub API)
  refuse the request outright. Composing the `decompress` middleware is how a
  client opts into an `accept-encoding` offer, and composing `user_agent()` is
  how it opts into an identity (the library's own `without-http/<version>` when
  given no segments), which is the position holding, not an exception to it.
- **No retry middleware.** The mechanism for a safe caller-side retry ships;
  the policy stays with the caller. Errors are typed per phase
  (`ConnectTimeout` vs `ReadTimeout`, so a loop can retry a failed connect and
  not a half-read response), the `Timeout` budget rides on the request value so
  an attempt can shorten it, `Content` bodies are replayable values, and the
  pool checks liveness before reusing an idle connection, preventing the common
  stale keep-alive failure rather than hiding a replay. A shipped `retry()`
  would add only policy (attempts, backoff, which statuses, `Retry-After`),
  which the caller and the layers above (`without-durability` re-runs steps)
  hold more context for, and which grows a predicate or a flag per new user.
  The costs: a caller who wants retries writes the loop, including draining the
  failed response so its connection is released; and the check-then-use race on
  a kept-alive connection can still surface an error other clients hide by
  silently replaying idempotent requests. If that race shows up in practice it
  is a pool concern to fix there, not a middleware.

  [`subscribe`](index.md#server-sent-events) is the one shipped loop, and it is
  this position holding rather than an exception to it. What the position rejects
  is policy the library would have to invent; an event stream carries its own.
  The backoff arrives on the wire as `retry:`, the resumption token as `id:`, and
  the terminal condition (a non-`200`, or a content type that is not
  `text/event-stream`) is written into the protocol, which is exactly what a
  general `retry()` could not say. Its two settings bound how far the peer
  supplying that backoff is trusted.
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
| HTTP/3 (QUIC) | no; [#11](https://github.com/JoshKarpel/without/issues/11) | no | [optional](https://pypi.org/project/hypercorn/) (`hypercorn[h3]`, aioquic) | [no](https://github.com/emmett-framework/granian) |
| WebSockets | [over HTTP/1.1](index.md#server) only; HTTP/2 is [#21](https://github.com/JoshKarpel/without/issues/21) | yes ([`websockets` or `wsproto`](https://uvicorn.dev/settings/)) | [over HTTP/1 and HTTP/2](https://pypi.org/project/hypercorn/) | [yes](https://github.com/emmett-framework/granian) |
| ASGI lifespan | yes, with the no-lifespan fallback | [yes](https://uvicorn.dev/settings/) | yes | yes |
| WSGI apps | [no, by position](#what-the-server-leaves-out) (async only) | — | [yes](https://pypi.org/project/hypercorn/) | [yes](https://github.com/emmett-framework/granian) |
| Other app interfaces | any ASGI app | any ASGI app | any ASGI or WSGI app | [RSGI](https://github.com/emmett-framework/granian) (its native interface) |

### ASGI extensions

The [ASGI extensions](https://asgi.readthedocs.io/en/latest/extensions.html)
are the sharpest per-server comparison, because each is a named optional
capability a server either implements or does not.

| Extension | without-http | uvicorn | hypercorn | granian |
|---|---|---|---|---|
| `websocket.http.response` (denial response) | yes ([`ws_wire`](https://github.com/JoshKarpel/without/blob/main/packages/without_http/src/without_http/ws_wire.py)) | [yes](https://github.com/encode/uvicorn/pull/1916) | [yes](https://github.com/pgjones/hypercorn/blob/main/src/hypercorn/protocol/ws_stream.py) | [yes](https://github.com/emmett-framework/granian/issues/793) |
| `http.response.early_hint` (103) | yes, [both protocols](https://github.com/JoshKarpel/without/blob/main/packages/without_http/src/without_http/h11_wire.py) | no | [yes](https://github.com/pgjones/hypercorn/blob/main/src/hypercorn/protocol/http_stream.py) | [pending](https://github.com/emmett-framework/granian/issues/93) |
| `http.response.trailers` | no ([raises](https://github.com/JoshKarpel/without/blob/main/packages/without_http/src/without_http/server.py)); [#16](https://github.com/JoshKarpel/without/issues/16) | — | [yes](https://github.com/pgjones/hypercorn/blob/main/src/hypercorn/protocol/http_stream.py) | [pending](https://github.com/emmett-framework/granian/issues/93) |
| `http.response.push` (HTTP/2 server push) | no; [#10](https://github.com/JoshKarpel/without/issues/10) | no HTTP/2 | [yes](https://github.com/pgjones/hypercorn/blob/main/src/hypercorn/protocol/http_stream.py) | [no, blocked upstream](https://github.com/emmett-framework/granian/issues/93) |
| `http.response.pathsend` | [declined](https://github.com/JoshKarpel/without/issues/4) (nothing below Python to hand the transfer to) | — | — | [yes](https://github.com/emmett-framework/granian/issues/82) |
| `http.response.zerocopysend` | [declined](https://github.com/JoshKarpel/without/issues/4) (same, plus `loop.sendfile` breaks h11's framing) | — | — | [declined](https://github.com/emmett-framework/granian/issues/93) (Rust/Python fd sharing) |
| `http.response.debug` | [declined](https://github.com/JoshKarpel/without/issues/4); the [spec](https://asgi.readthedocs.io/en/latest/extensions.html) says servers should not implement it | — | — | — |
| `tls` | yes, on every TLS scope ([`tls_extension`](index.md#server)); `server_cert` and `cipher_suite` are `None`, which CPython's `ssl` cannot supply | — | — | [tracked](https://github.com/emmett-framework/granian/issues/788) |

Two notes on the `without-http` column. First, the scopes advertise exactly
what the wire layers implement (`http.response.early_hint` on HTTP scopes,
`websocket.http.response` on WebSocket scopes, `tls` on both when the
connection is over TLS, and the in-memory `asgi_client` adds
`http.response.trailers`, the one extension memory can honor that the
wire cannot), so a third-party ASGI framework that checks the scope before
using an extension, as the spec tells it to, finds them; a `without-asgi` app
can speak the typed vocabulary directly without checking. Advertising follows
what the *request* can use, not just what the wire layer can render: an
HTTP/1.0 request gets no `http.response.early_hint`, since
[RFC 8297](https://datatracker.ietf.org/doc/html/rfc8297#section-2) forbids a
`103` to a client that would read it as the final response. Second, the `no`
cells are loud rather than silent: `without-asgi` types every one of these
messages, and the wire layers raise `NotImplementedError` on the unsupported
ones instead of dropping them.

The three `declined` cells are a position rather than a queue item, and the
reasoning generalizes past this project. The offload extensions exist to get
file bytes out of the Python layer, and a pure-Python asyncio server has no
lower layer to hand them to: `loop.sendfile` is the one true kernel path and it
bypasses h11's content-length accounting, which is the same framing conflict
that ended [uvicorn's zero-copy attempt](https://github.com/Kludex/uvicorn/pull/1210),
while granian's shipped `pathsend` is a chunked read-and-stream rather than the
syscall. A read-and-frame implementation here would be no faster than the app
streaming the body itself, which chunked `http.response.body` already does, so
the extension would offload only the byte-copy loop, exactly the part pure
Python cannot offload. That leaves the app to `stat` and sniff for its own
`content-type` and `content-length` anyway; `without-asgi`'s
[`file_response`](../without-asgi/index.md#streaming-a-file) is the composition
that does it.

### Operations

| | without-http | uvicorn | hypercorn | granian |
|---|---|---|---|---|
| Multiprocess workers | [no, by position](#what-the-server-leaves-out) (a process manager's job) | [`--workers`](https://uvicorn.dev/settings/) | [`workers`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [`--workers`](https://github.com/emmett-framework/granian), plus Rust runtime threads |
| Worker lifecycle (max requests, respawn) | [no, by position](#what-the-server-leaves-out) | [`--limit-max-requests`](https://uvicorn.dev/settings/) (+ jitter) | [`max_requests`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [lifetime, max-RSS, respawn on failure](https://github.com/emmett-framework/granian) |
| Auto-reload for development | [no, by position](#what-the-server-leaves-out) (restart the process) | [`--reload`](https://uvicorn.dev/settings/) | [`use_reloader`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [`--reload`](https://github.com/emmett-framework/granian) (extra) |
| Event loop choice | caller's (`asyncio.Runner(loop_factory=...)`, e.g. uvloop) | [`--loop`](https://uvicorn.dev/settings/) (uvloop) | [`worker_class`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) (asyncio, uvloop, trio) | [`--loop`](https://github.com/emmett-framework/granian) (asyncio, rloop, uvloop, winloop) |
| TLS | any `ssl.SSLContext`; [`server_ssl_context` helper with ALPN](index.md#server) | [cert/key flags](https://uvicorn.dev/settings/) | [`certfile`/`keyfile`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [cert/key flags, TLS 1.3 default](https://github.com/emmett-framework/granian) |
| Client certificates (mTLS) | via your `ssl.SSLContext` | [`--ssl-cert-reqs`, `--ssl-ca-certs`](https://uvicorn.dev/settings/) | [`verify_mode`, `ca_certs`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [`--ssl-client-verify`, `--ssl-ca`, CRLs](https://github.com/emmett-framework/granian) |
| Proxy headers (`x-forwarded-*`) | unwritten; ASGI-middleware-shaped, [#76](https://github.com/JoshKarpel/without/issues/76) | [on by default, trusted-IP gated](https://uvicorn.dev/settings/) | [`ProxyFixMiddleware`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/proxy_fix.html), shipped as ASGI middleware | — |
| Access logging | [no, by position](#what-the-server-leaves-out) (an ASGI middleware the caller writes) | [yes](https://uvicorn.dev/settings/) | [`accesslog` + format](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [opt-in, custom format](https://github.com/emmett-framework/granian) |
| Static file serving | app-side: a [startup inventory](../without-asgi/index.md#serving-a-tree-of-assets) rather than a directory mount, with `Range`, conditional requests, and pre-compressed variants | no | no | [directory mount below Python](https://github.com/emmett-framework/granian) (`--static-path-mount`, `--static-path-route`) |
| Response compression | app-side ([`compress()` middleware](../without-asgi/index.md#negotiated-response-compression): brotli, zstd, and gzip, injectable coding table, RFC 9110 weights, [Heal The Breach padding](../without-asgi/index.md#compression-and-secrets) opt-in) | no; app-side ([Starlette's `GZipMiddleware`](https://www.starlette.io/middleware/#gzipmiddleware), gzip only) | no; app-side | no; app-side ([precompressed *static* files](https://github.com/emmett-framework/granian/pull/791) in flight) |
| Metrics | live [`in_flight` count on the `Server` value](index.md#server); [no exporter, by position](#what-the-server-leaves-out) | no | — | [Prometheus exporter](https://github.com/emmett-framework/granian) |
| Graceful shutdown | [drain on exit; budget composed by the caller](index.md#server) | [`--timeout-graceful-shutdown`](https://uvicorn.dev/settings/) | [`graceful_timeout`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [kill timeout](https://github.com/emmett-framework/granian) |

The compression row is the one where every column agrees, and the agreement is
worth reading rather than skipping. No ASGI server compresses responses, and
none should: the ASGI spec defines no extension for it, the decision needs the
response's media type and the request's `accept-encoding` rather than anything
about the socket, and coverage wants to be scoped per route. It is middleware
everywhere, which is why `compress()` lives in `without-asgi` beside
`limit_request_body` rather than in the server here.

What differs is *how much* of RFC 9110 §12.5.3 the middleware implements, and
the ecosystem answer is "the first line of it". Starlette, Django, BlackSheep,
and aiohttp all decide by substring or word match on the raw header, so
`gzip;q=0` selects gzip in every one of them, `identity;q=0` is ignored, and
weights never rank anything; aiohttp picks in `ContentCoding` declaration order,
so a browser offering `gzip, deflate, br` is answered in deflate.
`Vary: Accept-Encoding` is the other common gap: Django patches it
unconditionally, Starlette omits it on responses below its size threshold, and
BlackSheep never sets it. Django is also the only one of the four to mitigate
BREACH, through the Heal The Breach padding its `max_random_bytes` controls;
`PADDED_COMPRESSORS` is the same mitigation here, extended to zstd's skippable
frames alongside gzip's filename field. [`negotiate_coding`](../without-asgi/reference.md) is the whole section
instead, weights included, as a pure function that can be read and tested apart
from the middleware that calls it.

### Limits and robustness

| | without-http | uvicorn | hypercorn | granian |
|---|---|---|---|---|
| Accept backlog | [`max_pending_connections`](index.md#server) | [`--backlog`](https://uvicorn.dev/settings/) (2048) | [`backlog`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) (100) | [`--backlog`](https://github.com/emmett-framework/granian) (1024) |
| Concurrency limit with shedding | [`limit_concurrent_requests`](index.md#server) app middleware (503); no transport-level shed for third-party apps, [#20](https://github.com/JoshKarpel/without/issues/20) | [`--limit-concurrency`](https://uvicorn.dev/settings/) (503) | — | [`--backpressure`](https://github.com/emmett-framework/granian) (pauses accept loop) |
| Idle / keep-alive timeout | [`idle_timeout`](index.md#server), off by default, also bounds slowloris and idle WebSockets | [`--timeout-keep-alive`](https://uvicorn.dev/settings/) (5 s) | [`keep_alive_timeout`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) (5 s) | [HTTP/2 keep-alive tuning](https://github.com/emmett-framework/granian) |
| Drain bound on connection close | [`close_timeout`](index.md#server) (5 s), per connection and on every close: past it the transport is aborted, so a peer that stops reading cannot hold a descriptor or a shutdown | only shutdown-scoped ([`--timeout-graceful-shutdown`](https://uvicorn.dev/settings/), unset by default) | only shutdown-scoped ([`graceful_timeout`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html)) | only process-scoped ([`--workers-kill-timeout`](https://github.com/emmett-framework/granian), off by default) |
| Request body cap | [`limit_request_body`](index.md#server) app middleware (413) | — | — | — |
| Request line + header cap | [`max_incomplete_event_bytes`](index.md#server) (HTTP/1.1, 16 KiB) and [`max_header_list_bytes`](index.md#server) (HTTP/2, 64 KiB) | [`--h11-max-incomplete-event-size`](https://uvicorn.dev/settings/) | [`h11_max_incomplete_size`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [header size options](https://github.com/emmett-framework/granian) |
| HTTP/2 stream and frame tuning | [`max_concurrent_streams`, `max_header_list_bytes`, flow-control-bounded body buffering](index.md#server) | no HTTP/2 | [stream, header-list, and frame size options](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) | [window, stream, and frame size options](https://github.com/emmett-framework/granian) |
| Rapid Reset mitigation (CVE-2023-44487) | [`max_stream_resets`](index.md#server), reset cancels the app task | no HTTP/2 | — | — |
| WebSocket message cap | [`max_websocket_message_bytes`](index.md#server) | [`--ws-max-size`](https://uvicorn.dev/settings/) (16 MB) | [`websocket_max_message_size`](https://hypercorn.readthedocs.io/en/latest/how_to_guides/configuring.html) (16 MiB) | — |

Two readings of these tables coexist. As a deployment checklist, the
operations table is the honest one: workers, reload, proxy headers, and access
logs are what a production rollout has to bring, and here they belong to the
process manager and the composition root rather than to the server
([what the server leaves out](#what-the-server-leaves-out)), which is a
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
the position working from the other direction.

The argument has one edge, which the shedding row names. A limit that travels
with the app reaches only apps you compose, so hosting a third-party ASGI app
(a FastAPI or Starlette app handed to `serving` whole) leaves nothing to inject
the middleware into, and uvicorn's transport-level `--limit-concurrency` covers
a case this does not
([#20](https://github.com/JoshKarpel/without/issues/20)). That is the price of
the position, payable when protecting a hosted app matters.

Static file serving sits on the same fault line, which is why its cell is split
rather than a `no`. These are handler helpers, so they travel to any server and
can turn a missing file into a clean `404` before a status is on the wire;
granian's mount serves the directory below Python without entering the app at
all, which no composition here can match, and its docs do not settle whether it
handles `Range` or conditional requests.

The interesting divergence is not the layer, though, it is the shape. Every
server in this table mounts a directory and derives a path from the request,
which is the construction that has produced a long line of traversal advisories.
An [`Inventory`](../without-asgi/index.md#there-is-no-directory-traversal) walks
the tree once at startup instead, so the request key selects among precomputed
values and no path is derived at all. That buys a shorter request path as well
as a smaller security surface: a revalidation answers a `304` from memory with
no syscall, and pre-compressed variants held as bytes make a `Range` over a
compressed asset work, which on-the-fly compression cannot do at any layer. The
price is the one in the name: the tree must not change while the process runs,
and a development loop rebuilds the inventory rather than picking up edits.

The remaining `unwritten` cell in the operations table is the server-side mirror
of the client's unwritten compositions: a proxy-header middleware over the ASGI
boundary ([#76](https://github.com/JoshKarpel/without/issues/76)) closes that
row generically, for any server underneath, and is the next generic middleware
to ship. Access logging is the same shape and is *not* shipping, for the reason
below.

### What the server leaves out

Positions, not gaps, each with its cost named. The client half has its own list
([what stays absent](#what-stays-absent)); these are the server's.

- **No multiprocess workers, and no worker lifecycle.** One `serving` call is
  one process. Running N of them, restarting one that dies, recycling one that
  has served enough requests or grown too large: that is a process manager's
  job, and gunicorn already does all of it, decoupled from the server it
  supervises. The cost is that a deployment brings its own supervisor rather
  than passing `--workers`, and that scaling is one more moving piece to
  operate. What is bought is that the server does not reimplement process
  supervision badly, and that the supervisor is replaceable.
- **No auto-reload.** Restart the whole process on a change, with
  [watchfiles](https://watchfiles.helpmanual.io/) or whatever the editor
  already runs. Hot reloading a running interpreter means partial module
  reloads, stale closures, and lifespan state that survives a change it should
  not; the failure mode is a developer debugging a ghost. The cost is startup
  latency on every edit, and the position is that startup is fast enough. If it
  is not, that is a startup-time problem to fix rather than a reason to reload
  in place.
- **No WSGI.** This stack is async to the socket, and a WSGI app is
  synchronous, so hosting one means a thread pool, a second concurrency model,
  and a set of semantics (no streaming request bodies, no WebSockets, no
  lifespan) that the rest of the vocabulary does not have. The cost is that a
  Flask or Django-sync app needs its own server, or an adapter the caller
  chooses.
- **No metrics exporter.** The position is to expose what a metric would be
  computed *from*, as live values on the objects that own them, and let the
  caller emit whatever their telemetry stack wants. `Server.in_flight` is the
  first of those; more (request counts, byte totals) join it as the server
  grows. The cost is that there is no `--metrics` flag to turn on, and a
  deployment writes the few lines that read the value on a timer. What is
  bought is that no telemetry library is baked into the server, and the same
  values feed Prometheus, OpenTelemetry, a log line, or a test assertion.
- **No access logging.** An access-log middleware over the ASGI boundary is a
  composition any caller can write against the scope and the response, and what
  belongs in the line (which headers, which correlation ID, what redaction,
  which format) is deployment policy that a shipped default would have to grow
  a flag per opinion to serve. `without-logging` is the natural place to
  assemble the record. The cost is real, though: a caller who wants the
  ordinary combined-log line writes it themselves, and every project writes it
  slightly differently. The proxy-header middleware is the deliberate contrast,
  and the difference is the failure mode: a slightly wrong log line is a
  slightly wrong log line, while a proxy-header middleware without a trust gate
  is a spoofable client address.
