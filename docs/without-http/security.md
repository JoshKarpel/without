# Security

The substrate keeps its security surface small and its policies explicit. This
page collects the deliberate choices that have a security dimension, so they are
documented in one place rather than buried in the code that implements them.

## Early responses and connection close

An HTTP server often needs to answer *before* it has read the whole request
body: a `413` for an oversized upload, a redirect, an auth rejection. Doing that
safely is subtle, because of how TCP closes interact with unread data.

### The reset hazard

When a socket is closed while inbound data the peer already sent is still
unread, the OS sends a TCP `RST` instead of a clean `FIN`. That reset can race
ahead of bytes still sitting in the peer's receive buffer, so the peer gets
`ECONNRESET` on its next read and never sees the response the server had already
written. The failure is a race, and which side of it you land on is
platform-dependent (it shows up mostly on BSD-derived stacks, including macOS),
which is exactly the kind of bug that passes CI on one runner and fails on
another.

Two sides cooperate to avoid it, and neither trusts the other to behave.

### Server: lingering close

When the HTTP/1.1 server closes a connection whose request body it did not
finish reading (an early response, or a malformed request answered with a `400`),
it does not slam the socket shut. It performs a *lingering close*
(`_lingering_close` in `server.py`):

1. Half-close the write side (`write_eof`, a `FIN`), so a well-behaved client
   learns the server is done and reads its response.
2. Read and discard any in-flight request body for a **short, fixed window**.
3. Close, regardless of whether more body was coming.

This mirrors nginx's
[`lingering_close`](https://nginx.org/en/docs/http/ngx_http_core_module.html)
and Go `net/http`'s `closeWriteAndWait`; the window matches Go's
`rstAvoidanceDelay` (500 ms). The load-bearing part is the `FIN`, not the drain:
a correct client stops sending and reads the moment it sees the half-close.

The drain is deliberately **not** run to end-of-input. Draining an arbitrary
request body to please a peer would be a denial-of-service vector: a slow or
endless upload could pin a connection open indefinitely. Because the window is
short and fixed and the server closes when it elapses, a client that keeps
sending past it simply gets the `RST` it would have gotten anyway. It holds no
resources beyond the bounded window, so the mechanism cannot be turned into a
resource-exhaustion lever.

Two limits are accepted, as they are by the servers above:

- TLS cannot half-close, so over TLS the server skips the `FIN` and relies on the
  bounded drain alone.
- The `FIN` signal does not reach a client sitting behind a TLS-terminating
  proxy, which maintains its own separate TCP connection to the client.

HTTP/2 is not subject to this hazard: it frames each request on its own stream
with independent flow control, and signals an early end with `RST_STREAM` /
`GOAWAY` rather than a transport-level close, so an early response is delivered
in-band.

### Client: stop sending when the peer closes

The client's request body is streamed concurrently with reading the response
(see [duplex streaming](index.md#duplex-and-bidirectional-streaming)).
When the peer half-closes, the HTTP/1.1 body sender (`send_body` in `client.py`)
stops streaming rather than writing on into a connection that is going away.

The trigger is the peer's half-close (`reader.at_eof()`), the duplex-safe "the
connection is closing" signal, **not** the arrival of the response head. Over a
duplex exchange a response can legitimately begin while the request body is still
in flight, so a response head implies nothing about whether the request should
stop; a half-close does.

This matters for both correctness and integrity. Continuing to write into a
peer that has closed would trip the socket into a reset, and on the client that
reset can discard the very response the read side is concurrently reading. The
same self-inflicted `RST` that the server's lingering close avoids on its side,
the client avoids by not forcing writes into a closing connection. An unfinished
send simply leaves the connection non-reusable; the response read owns surfacing
the outcome.

## Bounding work the caller controls

Two more knobs turn caller-controlled behavior into bounded, typed failures
rather than unbounded resource use:

- **Timeouts.** Per-phase `connect`/`read`/`write`/`pool` deadlines (see
  [Timeouts](index.md#timeouts)) bound how long any one phase can
  hang, so a stalled peer becomes a typed error you chose to arm rather than an
  eternal wait.
- **In-flight request limits.** The server does not cap raw connections (the
  kernel listen backlog and OS limits provide that backpressure); to bound
  concurrent *requests*, wrap the app in `limit_concurrent_requests`, which sheds
  with a `503` instead of queueing unboundedly.
