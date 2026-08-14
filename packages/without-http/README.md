# without-http

A sans-IO-backed ASGI **server** and **HTTP client** for `without`. Where
[`without-asgi`](../without-asgi) is the *app* side of the ASGI boundary (it turns
a server's `receive`/`send` into typed streams), `without-http` is the *server*
side: it owns the socket and the HTTP wire protocol, and drives any ASGI app via
`app(scope, receive, send)`.

The wire-protocol state machines are themselves sans-IO libraries:
[`h11`](https://h11.readthedocs.io/) for HTTP/1.1,
[`h2`](https://python-hyper.org/projects/h2/) for HTTP/2, and
[`wsproto`](https://python-hyper.org/projects/wsproto/) for WebSockets.
`without-http` reads and writes socket bytes with `asyncio`, feeds them through
those state machines, and uses `without-asgi`'s server-direction codecs to
translate between typed events and the ASGI dicts an app expects.

```python
from without import sleep_forever
from without_asgi import make_asgi_app
from without_http import ConnectionPool, request, serving

app = make_asgi_app(lifespan, http=router.dispatch, websocket=sockets.dispatch)

async with serving(app, host="127.0.0.1", port=8000):
    await sleep_forever()  # run until cancelled

async with ConnectionPool() as pool:
    async with request(pool, "GET", "http://127.0.0.1:8000/items") as (head, body):
        assert head.status == 200
        data = await body.read()
```

Because `without-http` speaks plain ASGI to the app, *any* ASGI app runs over it,
interchangeably with uvicorn. The server handles TLS, HTTP/2 (by ALPN or prior
knowledge), keep-alive, WebSockets over the HTTP/1.1 upgrade, per-handler
isolation, and flow control. A client is a function from a request to a response, and
a `ConnectionPool` is the one that answers over the network: a `(head, body)` response
split, buffered and streaming bodies in both directions, opt-in trailers, HTTP/2
multiplexing, per-host connection bounds, per-phase request timeouts, consumer-driven
duplex (with bidirectional streaming over HTTP/2), and `stack`-composed middleware.

`without_http.testing` carries the same interface into a test: a mock client that answers
from a function, an ASGI client that drives an app in memory, and a loopback client that
runs the real wire protocols over no socket at all.

See the
[`without-http` guide](https://without.help/without-http/)
(with the [API reference](https://without.help/reference/without_http/))
for the full surface, including the deferred work (WebSockets over HTTP/2, HTTP/3).
