from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Buffer
from collections.abc import Callable
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import timedelta
from types import MappingProxyType
from typing import assert_never
from urllib.parse import unquote
from urllib.parse import urlsplit

from without import cancel_futures
from without_asgi import Asgi
from without_asgi import ASGIApp
from without_asgi import Disconnect
from without_asgi import EarlyHint
from without_asgi import HttpScope
from without_asgi import PathSend
from without_asgi import RawHeaders
from without_asgi import RawMessage
from without_asgi import RequestBody
from without_asgi import ResponseDebug
from without_asgi import ResponseStart
from without_asgi import ServerPush
from without_asgi import ZeroCopySend
from without_asgi import encode_http_scope
from without_asgi import encode_inbound
from without_asgi import parse_outbound
from without_asgi.outbound import ResponseBody as OutboundBody
from without_asgi.outbound import ResponseTrailers as OutboundTrailers

from without_http.client import _NO_TIMEOUT
from without_http.client import Client
from without_http.client import ClientMiddleware
from without_http.client import ClientRequest
from without_http.client import ClientResponse
from without_http.client import ConnectionPool
from without_http.client import ResponseBody
from without_http.client import ResponseHead
from without_http.client import ResponseTrailers
from without_http.client import _releasing
from without_http.client import wrap
from without_http.lifespan import _wait_for
from without_http.lifespan import run_lifespan
from without_http.server import _DEFAULT_LIMITS
from without_http.server import _Limits
from without_http.server import _serve_connection
from without_http.socket_options import SocketOptions
from without_http.timeouts import Timeout

# What an in-memory client tells an app about itself. It presents as HTTP/1.1, so an app
# that runs against it faces the same surface it would over a socket.
_ASGI = Asgi(version="3.0", spec_version="2.4")

# The one extension this transport can honestly offer: a `ClientResponse` carries trailer
# blocks through to `read_with_trailers`, so an app's trailers reach the caller here
# rather than being dropped. The server-offload extensions (server push, zero-copy and
# path send) have a kernel or a proxy to offload to and nothing in memory does, so they
# stay unadvertised, as they are over HTTP/1.1.
_EXTENSIONS: Mapping[str, Mapping[str, object]] = MappingProxyType({"http.response.trailers": {}})
_ASCII = "ascii"
_CLIENT_ADDRESS = ("127.0.0.1", 51234)
_BUFFER = 65536

# The address an in-memory server presents as, which nothing resolves: a `pipe` has no
# port to report, so the server reads this back from `sockname` and a client names it in
# the URL (or, over HTTP/2, in `:authority`). Public because a test that writes frames by
# hand has to spell the authority itself, which is what `AUTHORITY` spells for it.
SERVER_ADDRESS = ("testserver", 80)
AUTHORITY = f"{SERVER_ADDRESS[0]}:{SERVER_ADDRESS[1]}".encode()


def mock_client(handler: Callable[[ClientRequest], ClientResponse | Awaitable[ClientResponse]]) -> Client:
    """
    A `Client` that answers every request from `handler`, reaching nothing at all.

    The whole of mocking, because a client is already a function: `handler` takes the
    `ClientRequest` a caller built and returns the `ClientResponse` it should see, so no
    pool, socket, or app exists underneath. Use it to test code that *sends* requests.

    ```python
    def answer(request: ClientRequest) -> ClientResponse:
        if request.url == "https://api.test/items":
            return respond(200, body=b'[]')
        raise AssertionError(f"unexpected request to {request.url}")

    stats = await summarize(mock_client(answer))
    ```

    `handler` may be sync or async, and is called once per request, which is what keeps
    a canned body usable more than once: a `ClientResponse` body is a *stream*, consumed
    exactly once, so build it inside `handler` (as above) rather than holding one
    response value and returning it twice.
    """

    async def client(request: ClientRequest) -> ClientResponse:
        answered = handler(request)
        if isinstance(answered, ClientResponse):
            return answered
        return await answered

    return client


def respond(
    status: int = 200,
    *,
    headers: RawHeaders = (),
    body: bytes = b"",
    trailers: RawHeaders | None = None,
) -> ClientResponse:
    """
    Build a canned `ClientResponse` for a `mock_client` handler to return.

    The body is a one-shot stream over `body`, so call this per request rather than
    reusing one value (see `mock_client`). `trailers`, when given, is a single trailing
    header block a `read_with_trailers` caller will see after the body.
    """

    async def events() -> AsyncGenerator[bytes | ResponseTrailers]:
        if body:
            yield body
        if trailers is not None:
            yield ResponseTrailers(trailers)

    return ClientResponse(ResponseHead(status, headers), ResponseBody(events()))


def base_url(base: str) -> ClientMiddleware:
    """
    Client middleware that resolves each request's URL against `base`.

    A test client needs absolute URLs for the same reason the network one does (the URL
    names the origin), so this is how `"/items"` becomes `"http://testserver/items"`
    without every call site repeating the host. An already-absolute URL is left alone.
    """
    prefix = base.rstrip("/")

    def resolve(request: ClientRequest) -> ClientRequest:
        if urlsplit(request.url).scheme:
            return request
        return replace(request, url=prefix + request.url)

    return wrap(request=resolve)


def scope_from_client_request(request: ClientRequest, *, root_path: str = "") -> HttpScope:
    """
    Build the `HttpScope` an ASGI app expects directly from a `ClientRequest`.

    The in-memory counterpart of `scope_from_request`, which does the same job from an
    `h11.Request`: pure, and reading only the request itself. The URL supplies what the
    wire would have (`scheme`, `server`, the raw path and query string), and a `host`
    header is synthesized when the caller did not set one, matching what the HTTP/1.1
    transport puts on the wire.

    The scope advertises `http.response.trailers`, since trailers do reach the caller in
    memory, and nothing else: an app that negotiates the extension takes its trailer path
    here.
    """
    parts = urlsplit(request.url)
    if parts.hostname is None:
        raise ValueError(f"client request URL must be absolute, got {request.url!r}")
    raw_path = (parts.path or "/").encode(_ASCII)
    headers = request.headers
    if not any(name.lower() == b"host" for name, _ in headers):
        headers = ((b"host", parts.netloc.encode(_ASCII)), *headers)
    return HttpScope(
        asgi=_ASGI,
        http_version="1.1",
        method=request.method,
        scheme=parts.scheme,
        path=unquote(raw_path.decode(_ASCII)),
        raw_path=raw_path,
        query_string=parts.query.encode(_ASCII),
        root_path=root_path,
        headers=headers,
        client=_CLIENT_ADDRESS,
        server=(parts.hostname, parts.port or (443 if parts.scheme == "https" else 80)),
        extensions=_EXTENSIONS,
    )


@asynccontextmanager
async def asgi_client(app: ASGIApp, *, root_path: str = "") -> AsyncIterator[Client]:
    """
    A `Client` that drives `app` in memory, with no wire and no server underneath.

    The app's lifespan runs for the block (through the same `run_lifespan` a real server
    uses), so startup state is in place before the first request and torn down after the
    last, and each request calls `app(scope, receive, send)` directly on a task of its
    own. Nothing is encoded, no socket is opened, and the whole exchange is one process.

    ```python
    async with asgi_client(app) as client:
        async with request(client, "GET", "http://testserver/items") as (head, body):
            assert head.status == 200
    ```

    It speaks only ASGI, so it drives *any* ASGI app, not just a `without` one. The
    response streams: the head is returned the instant the app sends
    `http.response.start`, and each body chunk crosses a one-slot queue, so an app that
    reads the request body while writing its response behaves as it would on the wire. An
    exception from the app surfaces to the caller rather than becoming a `500`, since
    there is no server here to convert it; reach for `loopback_client` to exercise the
    server's own error path.

    URLs are absolute, as they are for a `ConnectionPool`, so the same test body runs
    against a real server by swapping the client. Compose `base_url` for relative ones.
    """
    async with run_lifespan(app):

        async def client(request: ClientRequest) -> ClientResponse:
            return await _drive(app, request, root_path=root_path)

        yield client


async def _drive(app: ASGIApp, request: ClientRequest, *, root_path: str) -> ClientResponse:
    """
    Run one request through `app`, returning as soon as its head is sent.

    The in-memory sibling of the server's `_run_request`: it closes a `receive` and a
    `send` over local state and hands them to the app. Where that one encodes through
    `h11`, this one resolves a future for the head and pushes body chunks onto a
    one-slot queue, which is the in-memory stand-in for a socket buffer: an app that
    runs ahead of a slow reader blocks in `send`, exactly as it would on the wire.
    """
    scope = encode_http_scope(scope_from_client_request(request, root_path=root_path))
    started = asyncio.Event()
    head: list[ResponseHead] = []
    chunks: asyncio.Queue[bytes | ResponseTrailers] = asyncio.Queue(maxsize=1)
    body = aiter(request.body)
    request_done = False  # pragma: no mutate - initial sentinel, read only as a bool
    response_done = False  # pragma: no mutate - initial sentinel, read only as a bool
    sends_trailers = False  # pragma: no mutate - `http.response.start` always assigns it before a body event

    async def receive() -> RawMessage:
        nonlocal request_done
        if request_done:
            return encode_inbound(Disconnect())
        try:
            chunk = await anext(body)
        except StopAsyncIteration:
            request_done = True
            return encode_inbound(RequestBody(body=b"", more_body=False))
        return encode_inbound(RequestBody(body=chunk, more_body=True))

    def end_response() -> None:
        nonlocal response_done
        response_done = True
        chunks.shutdown()  # drains what is queued, then ends the body stream

    async def send(message: RawMessage) -> None:
        nonlocal sends_trailers
        match parse_outbound(message):
            case ResponseStart(status, headers, trailers):
                sends_trailers = trailers
                head.append(ResponseHead(status, headers))
                started.set()
            case OutboundBody(chunk, more_body):
                if chunk and request.method != "HEAD":
                    await chunks.put(chunk)
                if not more_body and not sends_trailers:
                    end_response()
            case OutboundTrailers(headers, more_trailers):
                # The extension puts the trailing blocks *after* the final body message,
                # so for an app that declared them at `http.response.start` it is the
                # last block, not the last body chunk, that ends the response.
                await chunks.put(ResponseTrailers(headers))
                if not more_trailers:
                    end_response()
            case EarlyHint():
                pass  # a client discards informational responses, as the h11 one does
            case ServerPush() | ZeroCopySend() | PathSend() | ResponseDebug() as unsupported:
                # Trailers are the only extension this transport advertises, so an app
                # sending one of these events is misusing the scope it was handed.
                raise NotImplementedError(f"{type(unsupported).__name__} is not supported in memory")
            case _ as unreachable:
                assert_never(unreachable)

    async def drive() -> None:
        try:
            await app(scope, receive, send)
        finally:
            # However the app ends, the body stream ends with it: shutting the queue down
            # is synchronous and non-blocking, so this is safe even under cancellation,
            # and it drains what is already queued before ending the stream.
            chunks.shutdown()

    task = asyncio.create_task(drive())
    if not await _wait_for(started, task):
        await task  # re-raises whatever the app failed with, if anything
        raise RuntimeError("the application returned without starting a response")

    async def events() -> AsyncGenerator[bytes | ResponseTrailers]:
        while True:
            try:
                item = await chunks.get()
            except asyncio.QueueShutDown:
                break
            yield item
        if response_done:
            return
        # The app ended mid-response: short of the final body chunk, or short of the
        # trailing block it declared. Surface why, rather than handing back a truncated
        # response as though it were the whole thing.
        await task  # re-raises the app's failure, when it had one
        raise RuntimeError("the application ended before finishing its response")

    async def release(fully_read: bool) -> None:
        # Shutting the queue down is what unblocks a `send` parked on it, immediate or not;
        # `immediate` additionally drops what is still queued, which nothing will read now
        # that the body generator is closed.
        chunks.shutdown(immediate=True)  # pragma: no mutate - nothing reads the queue after this
        await cancel_futures([task])

    return ClientResponse(head[0], ResponseBody(await _releasing(events(), release)))


class _PipeTransport(asyncio.Transport):
    """
    One direction of an in-memory connection: writes land in the peer's `StreamReader`.

    The stand-in for the socket transport `asyncio.open_connection` would hand back, so
    the reader/writer pair above it is the ordinary `asyncio` one and every consumer
    (`h11`, `h2`, the server's connection loop) is unchanged. Three behaviours make it a
    connection rather than a buffer:

    - **EOF and close.** `write_eof` and `close` feed the peer EOF, which is the half-
      close the wire protocols read as "the peer is done sending"; `close` also completes
      this side's `wait_closed` by delivering `connection_lost` to its own protocol. Once
      either end has closed, a `write` is dropped rather than delivered, since a socket
      also accepts it and reports the failure on a later read.
    - **Backpressure.** When a reader's buffer fills, `asyncio` pauses *its* transport;
      here that is translated into pausing the peer's writing, so the peer's `drain()`
      blocks until the reader catches up. Without that wiring an in-memory writer would
      run arbitrarily far ahead of its reader, which no socket does.
    - **Connection facts.** `get_extra_info` answers `sockname`/`peername` from the
      addresses it was built with and `None` for `socket`/`ssl_object`, so the server
      reads a cleartext connection with a peer address, as it would off a real socket.
    """

    def __init__(self, extra: dict[str, object]) -> None:
        super().__init__(extra)
        # Three placeholders until `link` runs, which it always does before any use.
        self._peer_reader: asyncio.StreamReader | None = None  # pragma: no mutate
        self._peer: _PipeTransport | None = None  # pragma: no mutate
        self._protocol: asyncio.StreamReaderProtocol | None = None  # pragma: no mutate
        self._closing = False
        self._paused = False
        self._eof_sent = False

    def link(
        self, peer_reader: asyncio.StreamReader, peer: _PipeTransport, protocol: asyncio.StreamReaderProtocol
    ) -> None:
        self._peer_reader = peer_reader
        self._peer = peer
        self._protocol = protocol

    def write(self, data: Buffer) -> None:
        # A write lands in the peer's `StreamReader`, which refuses data once it has been
        # fed EOF: by this side's own `write_eof`, or by the peer's `connection_lost` when
        # the peer closed. A socket takes such a write and surfaces the failure on a later
        # read, so the bytes are dropped here rather than raising out of the reader.
        if self._eof_sent or (self._peer is not None and self._peer.is_closing()):
            return
        if self._peer_reader is not None:  # pragma: no branch - always linked before use
            self._peer_reader.feed_data(bytes(data))

    def can_write_eof(self) -> bool:
        return True

    def write_eof(self) -> None:
        self._eof_sent = True
        if self._peer_reader is not None:  # pragma: no branch - always linked before use
            self._peer_reader.feed_eof()

    def is_closing(self) -> bool:
        return self._closing

    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self.write_eof()
        if self._protocol is not None:  # pragma: no branch - always linked before use
            asyncio.get_running_loop().call_soon(self._protocol.connection_lost, None)

    def abort(self) -> None:
        self.close()

    def pause_reading(self) -> None:
        # asyncio pauses the transport a full reader belongs to; here that means telling
        # the *peer* to stop writing, which is what a socket's receive window would do.
        if self._peer is not None:  # pragma: no branch - always linked before use
            self._peer.stall_writes()

    def resume_reading(self) -> None:
        if self._peer is not None:  # pragma: no branch - always linked before use
            self._peer.resume_writes()

    def stall_writes(self) -> None:
        """Park this side's `drain()` until the peer's reader has caught up."""
        if not self._paused and self._protocol is not None:  # pragma: no branch
            self._paused = True
            self._protocol.pause_writing()

    def resume_writes(self) -> None:
        """Let this side's `drain()` proceed again."""
        if self._paused and self._protocol is not None:  # pragma: no branch
            self._paused = False
            self._protocol.resume_writing()


type Endpoint = tuple[asyncio.StreamReader, asyncio.StreamWriter]


def pipe(
    *,
    server: tuple[str, int] = SERVER_ADDRESS,
    client: tuple[str, int] = _CLIENT_ADDRESS,
    limit: int = _BUFFER,
) -> tuple[Endpoint, Endpoint]:
    """
    Two connected `(reader, writer)` endpoints, wired to each other and to nothing else.

    The in-memory equivalent of a connected socket pair, `(client_side, server_side)`,
    with no file descriptor, no port, and no kernel involved. `limit` is the reader
    buffer at which backpressure kicks in, the analogue of a socket receive buffer.

    What it cannot reproduce is what only a kernel provides: TLS, and the difference
    between an orderly `FIN` and an abortive `RST`. Tests that turn on those stay on a
    real socket.
    """
    loop = asyncio.get_running_loop()

    def endpoint(
        sockname: tuple[str, int], peername: tuple[str, int]
    ) -> tuple[Endpoint, _PipeTransport, asyncio.StreamReaderProtocol]:
        reader = asyncio.StreamReader(limit=limit, loop=loop)
        protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
        transport = _PipeTransport({"sockname": sockname, "peername": peername})
        writer = asyncio.StreamWriter(transport, protocol, reader, loop)
        protocol.connection_made(transport)  # sets the reader's transport, for flow control
        return (reader, writer), transport, protocol

    near, near_transport, near_protocol = endpoint(client, server)
    far, far_transport, far_protocol = endpoint(server, client)
    near_transport.link(far[0], far_transport, near_protocol)
    far_transport.link(near[0], near_transport, far_protocol)
    return near, far


@asynccontextmanager
async def served_pipe(
    app: ASGIApp,
    *,
    max_concurrent_streams: int = _DEFAULT_LIMITS.max_concurrent_streams,
    max_stream_resets: int = _DEFAULT_LIMITS.max_stream_resets,
    idle_timeout: timedelta | None = _DEFAULT_LIMITS.idle_timeout,
    max_websocket_message_bytes: int | None = _DEFAULT_LIMITS.max_websocket_message_bytes,
) -> AsyncIterator[Endpoint]:
    """
    The client end of a `pipe` with `app` served on the other, for a test that writes bytes.

    `serving` minus `asyncio.start_server`, and minus a client: where `loopback_client`
    puts a `ConnectionPool` on this end, this hands the raw `(reader, writer)` over, so a
    test can drive the exact frames and (half-)close timing a protocol conformance test
    needs. The server reads `SERVER_ADDRESS` back as its `sockname`, which is the
    authority such a test names.

    ```python
    async with served_pipe(app, max_stream_resets=2) as (reader, writer):
        writer.write(connection.data_to_send())
        await writer.drain()
    ```

    The app's lifespan runs for the block and the connection is cancelled on exit, both
    as `serving` does, so a test can assert what leaving the block does to work still in
    flight. Both ends are closed on the way out, and a test may `write_eof()` its own
    end early to send the half-close a protocol reads as "done sending" while still
    reading the response. `close()` is full teardown, not a half-close: it also ends
    this end's own reader and drops the server's subsequent writes, though closing
    early is safe since closing twice is a no-op. The keyword arguments are `serving`'s
    per-connection bounds, with the same defaults.
    """
    limits = _Limits(
        max_concurrent_streams=max_concurrent_streams,
        max_stream_resets=max_stream_resets,
        idle_timeout=idle_timeout,
        max_websocket_message_bytes=max_websocket_message_bytes,
    )
    async with run_lifespan(app):
        near, far = pipe()
        connection = asyncio.create_task(_serve_connection(app, *far, limits))
        try:
            yield near
        finally:
            # Cancelling first is what makes the shutdown, rather than the client's EOF,
            # the thing that ends work still in flight. The server's end is closed here
            # rather than left to `_serve_connection`, since a block that exits without
            # ever awaiting never lets the connection task run at all. `close()` alone,
            # not `wait_closed()`: a pipe has nothing to flush, and the close waiter is
            # shared with whoever closed the endpoint first, so it may already have been
            # cancelled with them. The closes run even when the connection task crashed
            # (`cancel_futures` re-raises such a failure), so the endpoints never leak.
            try:
                await cancel_futures([connection])
            finally:
                for _reader, writer in (near, far):
                    writer.close()


@asynccontextmanager
async def loopback_client(
    app: ASGIApp,
    *,
    http2: bool = False,
    max_concurrent_streams: int = _DEFAULT_LIMITS.max_concurrent_streams,
    max_stream_resets: int = _DEFAULT_LIMITS.max_stream_resets,
    idle_timeout: timedelta | None = _DEFAULT_LIMITS.idle_timeout,
    max_websocket_message_bytes: int | None = _DEFAULT_LIMITS.max_websocket_message_bytes,
) -> AsyncIterator[Client]:
    """
    A `Client` that reaches `app` through the real wire protocols, over no socket at all.

    This is `serving` minus `asyncio.start_server`: the same `ConnectionPool` encodes the
    request, the same server code decodes and drives the app, and the bytes cross a
    `pipe` instead of the kernel. So it exercises what `asgi_client` skips (framing,
    chunking, keep-alive and connection reuse, the server turning a crashing handler into
    a `500`) while still opening no port and holding no file descriptor.

    ```python
    async with loopback_client(app) as client:
        async with request(client, "GET", "http://testserver/items") as (head, body):
            assert head.status == 200
    ```

    `http2` sends the h2 connection preface instead, which the server recognizes by prior
    knowledge, so one flag runs the same test over HTTP/2. The remaining arguments are
    `serving`'s per-connection bounds, with the same defaults.

    URLs must be `http`, since a pipe has no TLS to negotiate: an `https` URL is a loud
    failure rather than a silent downgrade. Nor can it reproduce an abortive close, so
    tests that turn on `RST` versus `FIN` semantics belong on `serving` and a real socket.
    """
    limits = _Limits(
        max_concurrent_streams=max_concurrent_streams,
        max_stream_resets=max_stream_resets,
        idle_timeout=idle_timeout,
        max_websocket_message_bytes=max_websocket_message_bytes,
    )
    connections: set[asyncio.Task[None]] = set()

    async def connect(
        host: str,
        port: int,
        *,
        ssl_context: ssl.SSLContext | None,
        timeout: Timeout = _NO_TIMEOUT,
        socket_options: SocketOptions = (),
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, str]:
        if ssl_context is not None:
            raise ValueError("loopback_client has no TLS; request an http:// URL")
        near, far = pipe(server=(host, port))
        task = asyncio.create_task(_serve_connection(app, *far, limits))
        connections.add(task)
        task.add_done_callback(connections.discard)
        return (*near, "http/1.1")

    async with run_lifespan(app), ConnectionPool(connect=connect, force_http2_cleartext=http2) as pool:
        try:
            yield pool
        finally:
            # Close the pooled connections first, so each server task sees the EOF and
            # ends on its own; whatever is still in flight after that is cancelled, which
            # is what `serving` does at shutdown too.
            await pool.aclose()
            await cancel_futures(connections)
