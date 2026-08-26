# without-http

A sans-IO-backed ASGI **server** and **HTTP client** for `without`. Where
[`without-asgi`](../without-asgi/index.md) is the *app* side of the ASGI boundary (it turns
a server's `receive`/`send` into typed streams), `without-http` is the *server*
side: it owns the socket and the HTTP wire protocol, and drives any ASGI app via
`app(scope, receive, send)`. See the
[`without_http` API reference](../without-http/reference.md) for the full surface.

The wire-protocol state machines are themselves sans-IO libraries:
[`h11`](https://h11.readthedocs.io/) for HTTP/1.1,
[`h2`](https://python-hyper.org/projects/h2/) for HTTP/2, and
[`wsproto`](https://python-hyper.org/projects/wsproto/) for WebSockets.
`without-http` reads and writes socket bytes with `asyncio`, feeds them through
those state machines, and uses `without-asgi`'s server-direction codecs to
translate between typed events and the ASGI dicts an app expects.

For a feature-by-feature register of where the client stands against httpx,
aiohttp, and niquests (and the server against uvicorn, hypercorn, and granian),
including which absences are gaps and which are positions, see
[Alternatives](alternatives.md).

## Server

```python
from without_async import sleep_forever
from without_asgi import make_asgi_app
from without_http import serving

app = make_asgi_app(lifespan, http=router.dispatch, websocket=sockets.dispatch)

async with serving(app, host="127.0.0.1", port=8000):
    await sleep_forever()  # run until cancelled
```

Because `without-http` speaks plain ASGI to the app, *any* ASGI app runs over it,
interchangeably with uvicorn: a [`without-web`](../without-web/index.md) router, a bare
`without-asgi` handler, or a third-party app (Starlette, FastAPI).

`serving(app, ...)` is the entrypoint: an async context manager that drives the
lifespan cycle, binds the socket (pass `port=0` to let the OS pick), yields a
`Server`, and shuts down cleanly on exit. There is no separate run-until-cancelled
wrapper: hold the block open however you like, with `sleep_forever()` for the simple
case or your own loop (signal handling, several servers under `asyncio.gather`). The
yielded `Server` exposes the bound address and live metrics:

```python
async with serving(app, port=0) as server:
    ...  # hit http://{server.host}:{server.port}; server.in_flight is the live count
```

What the server handles:

- **Lifespan.** The app is run once with a `lifespan` scope for the server's
  lifetime: `startup` on entry, `shutdown` on exit. An app that does not support
  lifespan signals so by raising before it acks startup; the server then serves
  without a lifespan cycle (the standard ASGI fallback).
- **TLS.** Pass an `ssl.SSLContext` as `ssl_context` to serve `https`/`wss`
  directly (the scope's `scheme` becomes `https`/`wss`). `server_ssl_context`
  builds one for the common case, advertising the protocols the server speaks via
  ALPN. `ssl_handshake_timeout` and `ssl_shutdown_timeout` bound the TLS handshake
  and close.
- **HTTP/2.** Selected by ALPN (`h2`) over TLS, or by *prior knowledge* over
  cleartext (the h2 connection preface is sniffed off the first bytes, since `h11`
  would mis-parse `PRI` as an HTTP/1 method). Each request stream drives its own
  ASGI app invocation, so many run concurrently over one connection; a single lock
  serializes the shared `h2.Connection` and the writer, and body sends respect
  per-stream `WINDOW_UPDATE` flow control. The same `without-asgi` server-direction
  codecs carry over; only the wire mapping (`h2_wire`) is new.
- **Keep-alive.** Sequential requests on one HTTP/1.1 connection reuse it
  (`h11`'s `start_next_cycle`). Reuse turns on the request being fully
  *received*, not on the app having read it: an app that ignores `receive`
  entirely (as FastAPI does on a body-less `GET`) keeps its connection, because
  the events it left unread are consumed from `h11`'s buffer once it responds.
  A connection whose peer is *still sending* when the response goes out (an
  early response, so the body never fully arrived) is closed *gracefully*
  instead, with a bounded lingering `FIN` rather than a reset that could
  discard the response: see
  [Security](security.md#early-responses-and-connection-close).
- **WebSockets** over the HTTP/1.1 `Upgrade`: the handshake is handed to `wsproto`,
  and the connection runs full-duplex (a reader pump feeds inbound frames to the
  app's `receive` while `send` writes outbound frames). A `websocket.close` sent
  before `websocket.accept` becomes an HTTP `403`, per the ASGI interface.
- **Isolation.** A crashing request handler is contained: it becomes a `500` (when
  no response has started yet) without taking the connection or server down.
- **Connections.** Served via `asyncio.start_server`, which owns the accept loop
  (surviving transient accept errors with its built-in retry delay) and binds every
  address `host` resolves to. `max_pending_connections` is the kernel listen backlog
  (the queue of accepted-by-the-OS-but-not-yet-served connections; when it fills, the
  OS drops or refuses further connection attempts). The server does not cap raw
  connections: the backlog and OS resource limits provide that backpressure, and
  `Server.in_flight` reports the live connection count for metrics. To bound in-flight
  *requests* (the right limit once one HTTP/2 connection multiplexes many requests),
  wrap the app in `limit_concurrent_requests`, which sheds with a `503`.
- **Resource bounds.** `serving` takes per-connection bounds for a hostile network,
  off or generous by default and tuned at the composition root (e.g. from an
  `EnvContext` settings value). `idle_timeout` (a `timedelta`) closes a connection
  whose peer stalls mid-exchange (slowloris) and bounds an idle WebSocket. Over
  HTTP/2, `max_concurrent_streams` is advertised and `max_stream_resets` caps how many
  resets one connection may issue before it is dropped, together defeating the Rapid
  Reset flood (CVE-2023-44487); a client reset also cancels the stream's app task, and
  received body is acked only as the app consumes it, so the flow-control window bounds
  buffered body. `max_websocket_message_bytes` caps a reassembled WebSocket message.
  The request *head* is bounded per protocol, because the two protocols measure it
  differently: `max_incomplete_event_bytes` is how much of an unfinished HTTP/1.1
  event (a request line and its headers, a chunk header) may accumulate before the
  parse is abandoned with a `431`, and `max_header_list_bytes` is advertised over
  HTTP/2 as `MAX_HEADER_LIST_SIZE`, bounding an *uncompressed* header list against an
  hpack bomb. Each defaults to its protocol library's own default, 16 KiB and 64 KiB.
  `close_timeout` (5 seconds) bounds the far end of a connection's life: how long a
  closing connection waits for a response it already queued to reach the peer. Asyncio
  hands the socket back only once that buffer drains, so a peer that stops reading would
  otherwise hold the descriptor, and hold a shutdown, indefinitely; past the bound the
  connection is aborted and the peer loses whatever was still in flight.
  For a body-size cap that works under any transport, wrap the app in
  `without-asgi`'s `limit_request_body`, which answers `413`.
- **TLS facts reach the app.** Over TLS, every scope carries the ASGI
  [`tls` extension](https://asgi.readthedocs.io/en/latest/specs/tls.html), read once
  per connection off the finished handshake, so an app calls `parse_tls` and gets the
  negotiated version and the client certificate chain (PEM) with its subject as an
  RFC 4514 distinguished name. `server_cert` and `cipher_suite` are `None`, which the
  spec permits: an `ssl.SSLContext` never exposes the certificate it loaded, and
  `SSLObject.cipher()` reports a suite by name with no IANA identifier. An mTLS
  deployment configures verification on its own `ssl.SSLContext`, as usual; the
  extension is how the result reaches the handler.

The pure wire cores (`h11_wire`, `h2_wire`, `ws_wire`) are sans-IO and unit-tested:
they map `h11`/`h2`/`wsproto` events to the typed `without-asgi` vocabulary and
back, with no sockets. The `asyncio` shell (`server.py`) is the only part that
touches I/O.

## Client

A client is a function from a request to a response:

```python
type Client = Callable[[ClientRequest], Awaitable[ClientResponse]]
```

That is the whole interface. A `ConnectionPool` is one (calling it answers the request
over the network), middleware maps one to another, and the in-memory clients in
[Testing](testing.md) are more of them. `request` is the surface you drive any of them
through: it builds the `ClientRequest`, runs it, and closes the response body on the way
out.

```python
from without_http import ConnectionPool, request

async with ConnectionPool() as pool:
    async with request(pool, "GET", "http://127.0.0.1:8000/items") as (head, body):
        assert head.status == 200
        data = await body.read()
```

Nothing about the pool is special to `request`, and nothing about `request` is special
to the pool. That split is what makes a test able to swap the network out from under
code it does not otherwise change.

### The response: a `(head, body)` split

`request` yields a `ClientResponse`, which is a `NamedTuple`, so take it whole or
unpack it as you like:

```python
async with request(client, "GET", url) as response:  # response.head, response.body
    ...
async with request(client, "GET", url) as (head, body):  # unpacked, types preserved
    ...
```

`head` is a `ResponseHead` (status + headers), a value you branch on immediately;
`body` is a `ResponseBody`, a live stream you consume separately. This mirrors how the
server *consumes* a request (a `scope` value plus a body stream): the structured head
is pulled out as a value so you can decide what to do before touching the body.

`head` is without-http's own inbound type, deliberately *not* without-asgi's outbound
`ResponseStart` even though the fields match: a type the parser fills from the wire
has no defaults (so a missing field fails loudly), while an outbound type an app
builds carries them for ergonomics. Same split as without-asgi's `RequestBody`
(inbound) versus `ResponseBody` (outbound).

### Buffered and streaming, both directions

Request and response bodies each cover the full buffered/streaming matrix, the
client mirror of `without-web`'s server handlers. The request body is `body=`
on `request`: pass `bytes` to buffer it, or a `Stream[bytes]` (any async
iterable of chunks) to stream it. The response `body` is a live stream: iterate it
chunk by chunk, or `await body.read()` to buffer the whole thing.

When you hold a *value* rather than bytes, pass a
[`Content`](../without-asgi/index.md#content-a-body-and-what-it-is) and the encoding
travels with the `content-type` describing it, which is the same value a handler answers
with:

```python
from without_asgi import json_content

async with request(client, "POST", url, body=json_content(order)) as (head, body):
    ...
```

An explicit `headers=` wins over what the content described, so overriding the
`content-type` does not mean rebuilding the body. `form_content` (URL-encoded
forms, the shape OAuth2 token endpoints take) is the other buffered producer, and
`multipart_content` (RFC 7578 file uploads) produces the streaming sibling
`StreamingContent`, whose chunks ride `body=` with their describing headers the
same way; `await ...buffered()` collapses one into a `Content` when a replayable
body with a `content-length` matters more than streaming.

```python
async def upload() -> AsyncIterator[bytes]:
    for path in paths:
        yield path.read_bytes()


async with request(pool, "POST", url, body=upload()) as (head, body):
    async for chunk in body:  # stream the response as it arrives
        sink.write(chunk)
```

The connection is released when the body is finished: an HTTP/1.1 connection is
returned to the pool only if its body was read to the end (a partial read closes
it, since unread bytes remain on the wire), and an HTTP/2 stream is reset if
abandoned early. `request` closes the body on block exit, so a body you never
read still releases its connection rather than stranding it.

### Trailers

A response can carry trailing headers after its body (gRPC's `grpc-status` is the
common case). The default path drops them: `async for chunk in body` and
`await body.read()` yield only `bytes`. When you know (out of band, by the
endpoint's interface) that trailers matter, opt in:

```python
data, trailers = await body.read_with_trailers()  # trailers: tuple[ResponseTrailers, ...]
# or, while streaming: async for item in body.events():  # bytes | ResponseTrailers
```

`read_with_trailers` returns *all* trailer blocks (an empty tuple if none), so a
consumer that requires them enforces that itself rather than the framework imposing
a failure on every response. Dropping trailers on the default path is a deliberate,
valid choice, not a swallowed error, so a server adding a trailer never breaks a
client that does not ask for it.

### Connection pooling

`ConnectionPool` keys connections by origin. HTTP/2 requests to one origin
multiplex over a single pooled connection; HTTP/1.1 connections are kept alive and
reused serially (an idle one is checked out per request and returned once its
response body is read). h2 is negotiated by ALPN over TLS
(`ConnectionPool(allow_http2=True)`, the default; pass a custom `ssl_context_factory` for a
private CA), or over cleartext by *prior knowledge* with `ConnectionPool(force_http2_cleartext=True)`
(no negotiation, so the caller is asserting the server speaks h2c); otherwise the
origin speaks HTTP/1.1.

```python
async with ConnectionPool(allow_http2=True, ssl_context_factory=make_ctx) as pool:
    # eight concurrent requests, multiplexed over one h2 connection
    bodies = await asyncio.gather(*(fetch(pool, n) for n in range(8)))
```

Open the pool as an async context manager so its connections are closed on exit; a
directly-constructed `ConnectionPool()` works for short-lived use but does not manage
the long-lived connections keep-alive retains.

`max_connections_per_host` bounds the concurrent HTTP/1.1 connections to one origin:
at the bound a checkout *waits* for one to be returned rather than opening another
(the wait a `pool` timeout guards). It is unbounded by default, mirroring the
server's choice to let OS backpressure cap connections rather than an in-process
limit; opt into a bound when you want explicit per-host backpressure. The h2 side has
an intrinsic sibling: stream issuance is gated against the server's advertised
`SETTINGS_MAX_CONCURRENT_STREAMS`, so a burst never over-issues streams on the one
multiplexed connection.

`max_keepalive_per_host` bounds a different axis: how many *idle* HTTP/1.1
connections are retained per origin once a burst subsides. Where the peak cap governs
how high the pool climbs under concurrent load, the idle cap governs how much it holds
onto when quiet: at the cap a returned connection is closed instead of pooled, so the
pool ramps up to `max_connections_per_host` under load but settles back down to
`max_keepalive_per_host` afterward rather than leaving every socket open. It is
unbounded by default (every reusable connection is kept); a value above
`max_connections_per_host` is never reached, since idle connections cannot outnumber
concurrent checkouts. Both knobs, when set, must be `>= 1`.

How the pool reaches an origin is itself injected: `connect` is a `Connect`, the one
step that touches the network. The default resolves with `getaddrinfo` and then
connects with [aiohappyeyeballs](https://github.com/aio-libs/aiohappyeyeballs) (the
CPython-extracted implementation aiohttp uses), racing address families per
[RFC 8305](https://datatracker.ietf.org/doc/html/rfc8305) with 250 ms between
attempts, so a dual-stack host with a black-holed IPv6 route costs one delay rather
than a full connect timeout; the race drives plain `loop.sock_connect`, so it behaves
the same on any event loop. Both steps are knobs on `tcp_connect`, the producer
behind the default: `happy_eyeballs_delay` tunes or disables the race, and `resolve`
injects the resolution step itself, a `(host, port) -> addr_infos` function, so a
DNS cache, DNS over HTTPS, or a test's canned addresses swap in without touching how
the winning address is connected. A cache's staleness bound stays the caller's
policy: `getaddrinfo` hides record TTLs, so no honest default exists. The same
`connect` slot is where a proxy or unix-socket connector would plug in.

### Duplex and bidirectional streaming

The request body and the response are handled **concurrently**: the body is sent by
a background task while the response head and body are read, so a server can answer
before the request body is fully sent. This is what lets a client survive the classic
large-upload deadlock, where a server rejects a big upload early (a `413`, a redirect)
and stops reading: the early response is read even though the request-body write is
still backed up on the wire.

Because the request body is a lazy `Stream[bytes]`, this extends to genuine
bidirectional streaming: hand `request` a queue-backed generator and feed it
*in reaction to* the response you are reading (the gRPC ping-pong shape).

```python
outbound: asyncio.Queue[bytes | None] = asyncio.Queue()


async def request_body() -> AsyncIterator[bytes]:
    while (chunk := await outbound.get()) is not None:
        yield chunk


await outbound.put(first_message)  # client speaks first
async with request(pool, "POST", url, body=request_body()) as (head, body):
    async for message in body:
        await outbound.put(reply_to(message))  # or None to end the request
```

The framework provides the *mechanism* (a concurrent duplex transport); you own the
*policy* (the interleaving protocol, and the knowledge of the server's interface that
keeps it from deadlocking). It deliberately does not buffer or force the body to
finish first, since that would defeat the pattern. A `write`/`read` timeout (below) is
the opt-in safety net that turns a mis-designed interleaving from an eternal hang into
a typed error you chose to arm.

This is genuinely correct over **HTTP/2**, whose independent per-direction flow
control is what bidi is built on. The request head is sent immediately, before the
first body chunk is produced, so both a *client-speaks-first* duplex (send an opening
chunk, then feed more in reaction to the response) and a *server-speaks-first* one (let
the server respond before any body chunk is ready) work over one request. Over
**HTTP/1.1** the same code runs, but real duplex is limited by server and proxy support
in the wild; there the concurrency buys the deadlock fix rather than a promise of robust
bidi.

Answering early and closing safely has a security dimension on both sides (the
client stops sending on the peer's half-close; the server closes with a bounded
lingering `FIN` rather than an `RST` that could discard its own response). See
[Security](security.md#early-responses-and-connection-close).

### Server-Sent Events

The event stream *format* lives in `without-asgi`, because it is two pure
transforms that touch no socket: see
[Server-Sent Events](../without-asgi/sse.md). One connection needs nothing from
this package beyond the byte stream a response body already is:

```python
from without_asgi import parse_events

async with request(client, "GET", url) as (head, body):
    async for event in parse_events(body):
        ...
```

What does need a transport is the loop that keeps the stream up. `subscribe`
opens a connection, parses the body, and when the stream ends waits and opens
another one carrying `Last-Event-ID`, so the producer resumes where the consumer
stopped. That resumption point moves on an event carrying an `id:` *and* on a
`Checkpoint`, an id-only frame a producer sends after skipping work you asked not
to see; acting on only the first replays from before the skip. A caller sees one
uninterrupted stream of events across however many connections it took:

```python
from without_http import subscribe

events = subscribe(lambda headers: pool(ClientRequest("GET", url, headers)))
async for event in events:
    if done(event):
        break
await events.aclose()  # releases the connection there and then
```

What `subscribe` takes is a *function* that opens one connection, not a
`ClientRequest`. A request is not replayable: its body is a `Stream[bytes]`,
which the interface allows to be iterated exactly once, so re-sending one request
value would put a full body on the wire for the first attempt and an empty one
for every attempt after it. Building the request inside the function makes that
unrepresentable, and it is what lets an event stream ride a `POST` (the shape
MCP's Streamable HTTP uses) rather than only the bodyless `GET` a reused request
survives. The headers handed to it are `accept: text/event-stream` and, once the
stream has a resumption point, `last-event-id`; `merge` them with your own to
decide which side wins on a name you also set.

**This is the only retry loop `without-http` ships**, and the
[position against a `retry()` middleware](alternatives.md#the-client) is why it
can be. That position rejects *policy* the library would have to invent: how
many attempts, which statuses, what backoff. Here there is none to invent. The
backoff arrives on the wire as `retry:`, the resumption token arrives as `id:`,
and the terminal condition is written into the protocol. What the settings below
decide is how far to trust the peer that supplies them.

What it does and does not retry:

- A non-`200` status, or a content type other than `text/event-stream`, raises
  `NotAnEventStream` and never reconnects. An endpoint answering `404` or
  `text/html` is not a stream that dropped, it is one that was never there.
- The **first** connection's errors propagate, so a caller that cannot reach the
  endpoint at all learns immediately rather than watching a silent loop.
- Once a stream has been established, a connection error or timeout, on the
  stream or on any later attempt, reconnects. A stream a proxy reaps every 60
  seconds is the ordinary case, which is why the protocol has a resumption token
  at all.

The wait is `reconnect` (three seconds) until the producer names one, after which
it is clamped to between `minimum_reconnect` (100ms) and `maximum_reconnect`
(five minutes). Both ends guard the same thing, a `retry:` that is hostile or
merely wrong: at zero it would spin a consumer into a hot reconnect loop, and a
few orders of magnitude too large it would park one on a subscription that goes
silent forever with nothing raised to notice. Widen either end for a producer you
trust to name its own backoff, narrow them to hold a peer to a window you chose.
A window that runs backwards
raises, and raises at the call rather than at the first `anext`, since by then a
request has already gone out. `sleep` is injected, so a test drives the loop
without waiting and a caller can add jitter.

### Timeouts

By default a request has **no timeouts**: a hung connect or a stalled server blocks
until you cancel it. A timeout is a *policy* keyed to your time budget ("fail rather
than make slow progress, so my caller can react"), which the transport cannot know,
so you opt in per phase with a `Timeout` value on the request:

```python
from datetime import timedelta

from without_http import Timeout, deadline, request

async with request(pool, "GET", url, timeout=Timeout(read=timedelta(seconds=5))) as (head, body):
    ...

# or as a default for everything sent through one client
budgeted = deadline(Timeout(connect=timedelta(seconds=10), read=timedelta(seconds=30)))(pool)
```

The budget rides on the `ClientRequest`, not on the pool, because it belongs to the
caller rather than to the connection: one pool serves callers with different budgets,
and middleware (a retry shortening each attempt) can rewrite it like any other field.
`deadline` fills it in for a request that states none, and leaves a request that states
its own alone.

Each axis is a `timedelta`, so the unit is explicit rather than an ambiguous bare
number, and an *inactivity* bound (it re-arms on progress), not a total deadline:
`read`/`write` bound the gap between chunks, so a slow-but-progressing transfer is
not killed. Every field defaults to `None` (that axis disabled), and there is no
shared-default scalar, since one duration across four unrelated phases carries no
meaning. For an overall wall-clock cap, compose one on the substrate:
`async with asyncio.timeout(t): request(...)`.

**What each axis bounds** (what is actually happening on the wire; the thing most
clients leave you to guess at):

| Axis | Phase it bounds | On the network |
|---|---|---|
| `connect` | DNS + TCP connect, and (over TLS) the handshake | one `open_connection` await; ALPN is negotiated here |
| `write` | making progress sending a request-body chunk | a socket write + `drain`; over h2, waiting for the flow-control window |
| `read` | waiting for the next response chunk (head, body, trailers) | a socket read; over h2, the next `DATA` for this stream |
| `pool` | acquiring a connection slot | *nothing on the wire*: the per-host bound or the h2 stream gate |

Over HTTP/2 the read/write axes measure *per-stream* progress, not socket progress:
a `read` timeout means "no `DATA` for **my** stream in N seconds" even while the
socket is busy with other streams.

**What to do when one fires.** Each axis raises a typed error under `HTTPTimeout`
(itself a `TimeoutError`), so a coarse `except TimeoutError` catches any while the
specific type tells you *how far the request got*, which is what determines the safe
recovery:

| Fired | Request got as far as | Safe to retry? |
|---|---|---|
| `PoolTimeout` | never left the process | **always**; usually the real fix is local backpressure, not retrying the peer |
| `ConnectTimeout` | no connection established | **always**, even a non-idempotent request, or fail over to another origin |
| `WriteTimeout` | mid-sending the request | idempotent: yes; otherwise ambiguous. The connection is discarded, so a retry gets a fresh one |
| `ReadTimeout` | request fully sent, awaiting the response | only if idempotent (the server may already have processed it); if mid-body, decide keep-vs-discard the partial |

### TCP keepalive

Pooled connections outlive the request that opened them, so a kept-alive socket can
sit idle for a long time between uses. Two things can end it while it waits, and they
need different handling:

- A server *cleanly* closing its end of an idle keep-alive connection sends a
  `TCP FIN`, which asyncio surfaces on the event loop. The pool notices it before
  reuse (the checkout skips a connection that is closing or at EOF) and opens a fresh
  one, so this common case needs nothing from you.
- A peer that *silently* vanishes (a crashed server, a network partition, a NAT or
  firewall dropping the flow) sends no `FIN`. Nothing surfaces on the event loop, so
  the dead socket looks reusable until a request stalls on it. With no request
  timeouts armed (the default), that stall has nothing to bound it.

TCP keepalive closes that second gap: the kernel probes an otherwise-idle connection
and tears it down when the peer stops answering, independent of any request. It is
**on by default**, as one entry in the pool's `socket_options`:

```python
from without_async import Seconds
from without_http import ConnectionPool, tcp_keepalive

# The default: probe after 60s idle, every 10s, drop after 6 unanswered probes.
async with ConnectionPool() as pool:
    ...

# Tune the probe timing, or pass () to leave the kernel's own defaults alone.
async with ConnectionPool(socket_options=tcp_keepalive(idle=Seconds(30), interval=Seconds(5), count=4)) as pool:
    ...
```

`idle` and `interval` are counts of
[`Seconds`](../without-async/index.md#durations-that-cross-an-integer-boundary-without_asyncdurations), because the
underlying options carry only integer seconds: a finer duration is not something either
can be built from, so none is silently truncated on the way to the socket. `count` is a
plain probe count. `SO_KEEPALIVE` is
enabled portably; the per-probe tuning maps to the Linux
`TCP_KEEPIDLE`/`TCP_KEEPINTVL`/`TCP_KEEPCNT` socket options, and a platform that lacks
one of those knobs keeps its own default for that axis.

### Socket options

`tcp_keepalive` is not special: it is one of several **pure producers** of
`(level, option, value)` triples, and they compose the way headers do. Each describes a
single concern and knows nothing about the others, so combining them is plain
concatenation rather than a merge that has to understand what any of them mean:

```python
from without_http import ConnectionPool, receive_buffer_size, send_buffer_size, serving, tcp_keepalive

async with ConnectionPool(socket_options=tcp_keepalive() + send_buffer_size(1 << 16)) as pool:
    ...

# On the server, options apply to the *listening* socket.
async with serving(app, socket_options=receive_buffer_size(1 << 16)) as server:
    ...
```

The order is the order they are applied in, and passing `()` sets nothing at all. Note
that keepalive is the pool's *default*, so a `socket_options` that should keep probing
has to say so: include `tcp_keepalive()` in the combined set rather than replacing it.

`send_buffer_size` and `receive_buffer_size` pin `SO_SNDBUF`/`SO_RCVBUF`, which is how
you make a socket's buffer a *known* size: left alone, Linux autotunes each up to the
`max` of `net.ipv4.tcp_wmem`/`tcp_rmem`, and
[its documentation](https://docs.kernel.org/networking/ip-sysctl.html) is the guarantee
being relied on ("Calling `setsockopt()` with `SO_SNDBUF` disables automatic tuning of
that socket's send buffer size"). Both are bounds rather than exact reservations: per
[`socket(7)`](https://man7.org/linux/man-pages/man7/socket.7.html) the value is capped
by `net.core.wmem_max`/`rmem_max`, and the kernel stores (and returns) double what you
set, for bookkeeping.

A listening socket hands its buffer sizes down to every connection accepted on it, so
`receive_buffer_size` on `serving` bounds what the server will buffer from a peer whose
body it has not read yet. Options that are meaningful only per-connection have nothing
to act on at bind time; `TCP_NODELAY` is the notable one, and asyncio already sets it on
every TCP transport it creates, in both directions, so there is nothing to configure.

### Client middleware

A `Client` is the dual of a server handler, and a `ClientMiddleware` wraps one into
another: `Client -> Client`. That is the zero-context case of the **same** `stack` that
composes server middleware (a server middleware is `(handler, state, scope) -> handler`;
a client one needs no context because the request *is* the value it transforms), so the
one `stack` serves both. A decorated client is just another client, so you build the one
you want and pass it to `request`:

```python
from without_http import ConnectionPool, CookieJar, bearer_auth, cookies, follow_redirects, request, stack

jar = CookieJar()
async with ConnectionPool() as pool:
    client = stack(bearer_auth("..."), follow_redirects(), cookies(jar))(pool)
    async with request(client, "GET", url) as (head, body):
        ...
```

The pool holds no middleware of its own, which is what keeps decoration and connection
reuse independent: the order is visible where you compose it, and the same pool can back
several differently-decorated clients (one authorized, one not) without any of them
reaching into it.

Because the whole request is the value a client transforms (not a fixed scope),
middleware can rewrite it on the way out (inject headers, change the URL on redirect,
attach cookies, set a deadline) and wrap the response on the way back.

For the simple independent case, `wrap(request=, response=)` builds a middleware from a
request transform and/or a response transform, the client counterpart to
without-asgi's `wrap` (which wraps a handler's inbound/outbound streams). `add_headers`
is a one-liner over it: `wrap(request=lambda r: replace(r, headers=...))`. Reach for it
when the two sides are independent; a middleware whose sides share state (`cookies`) or
that loops (`follow_redirects`) is written directly as a `Client` wrapper.

```python
from without_http import ClientResponse, wrap

byte_counter = wrap(response=lambda r: ClientResponse(r.head, counting(r.body)))
```

Auth is the canonical fixed-header case, so the two challenge-free schemes ship
as named middleware: `basic_auth(username, password)` sends RFC 7617 `Basic`
credentials (UTF-8, base64), and `bearer_auth(token)` sends
`authorization: Bearer <token>`. The scheme prefix is the part real APIs
disagree on, so it is injectable: `bearer_auth(token, scheme="Token")` for the
peers that spell it differently, or `scheme=""` to send the bare token.
Digest, which answers a challenge, would be a looping middleware like
`follow_redirects` and is not written.

Both set a default rather than a policy: `authorization` is a singleton field
under RFC 9110, so a request carrying its own keeps it and the middleware adds
nothing. That is what makes a composed client usable for the odd call that
authenticates as someone else, where `add_headers` would prepend a second
`authorization` and leave the peer to pick.

That split is the whole difference between the two header middlewares, and it
follows the field rather than the caller's taste. `add_headers` copies its
headers onto every request whatever it already carries, which is what a field
that may repeat (`accept`, `via`, a trace header) wants. `default_headers`
adds each header only to a request that omits it, which is what a field RFC 9110
allows once (`authorization`, `user-agent`, an API key) wants, and it decides
each header separately, so a request stating one default and not another gets
exactly the one it left out. The auth and user-agent middlewares are
`default_headers` underneath. Neither imposes: a caller that must not be
overridden composes its own client instead of handing out one that can be, the
same position `deadline` takes on a time budget.

The other fixed header peers commonly gate on is the user-agent, which this
client never sends unbidden, and which some peers (the GitHub API) refuse to
see absent. `user_agent()` is the opt-in: with no arguments it sends
`USER_AGENT`, the library's own `without-http/<version>` identity read from the
installed distribution, which is the same default every other client sends
without asking; passing segments sends them joined with spaces, the separator
RFC 9110 puts between product tokens, so `user_agent("myapp/1.0", USER_AGENT)`
sends both identities and `user_agent("myapp/1.0")` sends exactly yours.
`USER_AGENT` is public precisely so a caller can join it into their own value
rather than choose between theirs and the library's. `user-agent` is a
singleton field too, so this is a default in the same sense as the auth
middlewares: a request carrying its own keeps it.

Content codings are middleware too, one per direction. `decompress()` offers
`accept-encoding: br, gzip, zstd` (a request carrying its own offer keeps it) and
decodes an encoded response body through an incremental decoder as it streams,
dropping the `content-encoding` and `content-length` that described the encoded bytes
so the response stays self-consistent; an encoding it cannot decode passes through
whole. Both gzip and zstd define a body as a *series* of streams, so a decoder that
reaches the end of one hands its leftover bytes to a fresh one: a concatenated body
(a `cat a.gz b.gz` asset, bgzip output, a proxy that joins two responses) decodes
whole rather than silently stopping at the first member, and the truncation check
then covers the last stream as well as the first. It is composition rather than pool behavior, so the transport never silently
rewrites bytes. `gzip_compress()`, `zstd_compress()`, and `brotli_compress()` encode
request bodies the same streaming way; requests have no `accept-encoding`
negotiation, so all are opt-in for the clients whose upstreams are known to decode
them. The opt-in scope is wherever the composition happens: decorate once at assembly
for a whole client, or inline at one call site
(`request(gzip_compress()(client), ...)`) for a single request, since decorating a
client is a stateless function wrap.

gzip and zstd decode via the stdlib; brotli via
[Google's own bindings](https://github.com/google/brotli), a dependency rather than
an extra because it makes no choice for anyone (the stdlib has no brotli and there
is exactly one library, so see
[the philosophy on dependencies](../philosophy.md#a-dependency-is-a-choice-so-take-only-the-ones-that-arent)),
which is what lets `decompress()` just work against the codings the web actually
serves. The codings are table
entries, not a property of the library: `decompress` takes a mapping from coding to
`Decompressor` factory, defaulting to `DEFAULT_DECOMPRESSORS` and deriving its offer
from the keys so what is advertised and what is decoded cannot disagree; and
`compressing(coding, make_compressor)` is the public mechanism behind the shipped
compressors. A codec this package does not ship plugs into either direction with a
factory whose product satisfies the small `Compressor` / `Decompressor` protocols,
inheriting the framing rewrite, streaming, and truncation check instead of
reimplementing them.

For the encoding direction that protocol has a second rung, because streaming a body
is a demand on the codec rather than only on the loop around it. What a `compress`
call returns is the codec's choice: fed a chunk at a time, zlib and zstd return almost
nothing until the stream ends, so a body wrapped in one of them raw would sit whole
inside the codec while every line of code around it looked like it streamed. A
`StreamingCompressor` is a `Compressor` that can also end a block without ending the
stream, which is what releases each chunk, and `gzip_compressor`, `zstd_compressor`,
and `brotli_compressor` are the shipped factories that produce one (all three live in
`without-asgi` and are re-exported here). Reach for those rather than
`zlib.compressobj` or `zstd.ZstdCompressor` directly, since the raw objects satisfy
`Compressor` and not the second rung. A plain `Compressor` still encodes correctly and
still holds the body to the end: a coding the caller named has no unencoded answer to
fall back on the way a negotiated response does. A body that arrives whole is one
chunk and encodes to identical bytes under either.

The server counterpart is
[`compress()`](../without-asgi/index.md#negotiated-response-compression), an ASGI
middleware in `without-asgi`: it reads the `accept-encoding` that `decompress()`
sends and encodes the response body the client will decode. The two `Compressor`
protocols and the three codec factories behind `gzip_compress`, `zstd_compress`, and
`brotli_compress` all live in `without-asgi`, the lower of the two packages, and are
re-exported here, so one codec serves a coding in both directions and the same three
codings are decoded inbound and produced outbound.
`brotli_compress` keeps the bindings' own quality of 11 while the server table
defaults lower: a client compressing an upload it holds whole is the case that
ratio is worth paying for, and a response encoded per request is not.

State a middleware carries lives in a value you own, not in the transport. A `CookieJar`
is the canonical case: you construct the jar and hand it to `cookies(jar)`, so cookie
scope (application identity) stays independent of connection reuse (transport). Two
requests share cookies exactly when they share a jar. See [Cookies](cookies.md) for the
jar's matching rules, the origin guards it enforces on untrusted `Set-Cookie` responses,
and its expiry model.

### In-memory clients

`without_http.testing` holds three more clients: `mock_client` answers from a function,
`asgi_client` drives an ASGI app with no wire under it, and `loopback_client` runs the
real wire protocols over no socket at all. They are ordinary `Client`s, so a test above
them is the same code that runs against the network, and swapping one in is the only
edit. Underneath them, `pipe()` and `served_pipe(app)` hand over the raw endpoints for a
test that writes frames rather than requests. See [Testing](testing.md) for how much of
the stack each one covers, what none of them can reproduce, and how they interoperate
with `httpx` and starlette's `TestClient`.
