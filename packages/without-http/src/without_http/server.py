from __future__ import annotations

import asyncio
import socket
import ssl
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import asynccontextmanager
from contextlib import suppress

import h11
from without import background_task
from without import limit_concurrency
from without import sleep_forever
from without_asgi import ASGIApp
from without_asgi import Disconnect
from without_asgi import RawMessage
from without_asgi import RequestBody
from without_asgi import WebsocketAccept
from without_asgi import WebsocketBinary
from without_asgi import WebsocketClose
from without_asgi import WebsocketConnect
from without_asgi import WebsocketDisconnect
from without_asgi import WebsocketReceive
from without_asgi import WebsocketText
from without_asgi import encode_http_scope
from without_asgi import encode_inbound
from without_asgi import encode_websocket_inbound
from without_asgi import encode_websocket_scope
from without_asgi import parse_outbound
from without_asgi import parse_websocket_outbound
from wsproto import ConnectionType
from wsproto import WSConnection
from wsproto.events import BytesMessage
from wsproto.events import CloseConnection
from wsproto.events import Ping
from wsproto.events import Pong
from wsproto.events import TextMessage

from without_http.h11_wire import h11_events_from_outbound
from without_http.h11_wire import inbound_from_event
from without_http.h11_wire import scope_from_request
from without_http.lifespan import run_lifespan
from without_http.ws_wire import is_websocket_upgrade
from without_http.ws_wire import websocket_scope_from_request
from without_http.ws_wire import ws_events_from_outbound

_BUFFER = 65536


def _address(info: object) -> tuple[str, int] | None:
    if isinstance(info, tuple) and len(info) >= 2 and isinstance(info[0], str) and isinstance(info[1], int):
        return info[0], info[1]
    return None


async def _send_simple(conn: h11.Connection, writer: asyncio.StreamWriter, status: int, body: bytes) -> None:
    response = h11.Response(
        status_code=status,
        headers=[(b"content-type", b"text/plain; charset=utf-8"), (b"content-length", str(len(body)).encode())],
    )
    with suppress(h11.ProtocolError, OSError):
        for event in (response, h11.Data(data=body), h11.EndOfMessage()):
            chunk = conn.send(event)
            if chunk is not None:
                writer.write(chunk)
        await writer.drain()


async def _read_request(conn: h11.Connection, reader: asyncio.StreamReader) -> h11.Request | None:
    """Pull h11 events until the next request line+headers, reading the socket as needed."""
    while True:
        event = conn.next_event()
        if event is h11.NEED_DATA:
            conn.receive_data(await reader.read(_BUFFER))
            continue
        if isinstance(event, h11.Request):
            return event
        return None


async def _run_request(
    app: ASGIApp,
    conn: h11.Connection,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    scheme: str,
    server: tuple[str, int] | None,
    client: tuple[str, int] | None,
    request: h11.Request,
) -> bool:
    """Drive one request/response exchange. Returns whether the connection may keep alive."""
    scope = scope_from_request(request, scheme=scheme, server=server, client=client)
    method = scope.method
    request_done = False
    response_done = False

    async def receive() -> RawMessage:
        nonlocal request_done
        if request_done:
            return encode_inbound(Disconnect())
        while True:
            event = conn.next_event()
            if event is h11.NEED_DATA:
                conn.receive_data(await reader.read(_BUFFER))
                continue
            if not isinstance(event, h11.Event):
                request_done = True
                return encode_inbound(Disconnect())
            inbound = inbound_from_event(event)
            if inbound is None:
                request_done = True
                return encode_inbound(Disconnect())
            if isinstance(inbound, Disconnect) or (isinstance(inbound, RequestBody) and not inbound.more_body):
                request_done = True
            return encode_inbound(inbound)

    async def send(message: RawMessage) -> None:
        nonlocal response_done
        for event in h11_events_from_outbound(parse_outbound(message)):
            if method == "HEAD" and isinstance(event, h11.Data):
                continue
            chunk = conn.send(event)
            if chunk is not None:
                writer.write(chunk)
            if isinstance(event, h11.EndOfMessage):
                response_done = True
        await writer.drain()

    # An ASGI server isolates a single request's failure from the connection loop:
    # a crashing app must not take the server down, so its exception is contained
    # here and turned into a 500 when no response has gone out yet.
    crashed = False
    try:
        await app(encode_http_scope(scope), receive, send)
    except Exception:
        crashed = True
    if not response_done and conn.our_state is h11.SEND_RESPONSE:
        await _send_simple(conn, writer, 500, b"internal server error\n")
    if crashed:
        return False
    return conn.our_state is h11.DONE and conn.their_state is h11.DONE


async def _serve_websocket(
    app: ASGIApp,
    conn: h11.Connection,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    scheme: str,
    server: tuple[str, int] | None,
    client: tuple[str, int] | None,
    request: h11.Request,
) -> None:
    """Drive one WebSocket connection over the HTTP/1.1 upgrade, via wsproto.

    WebSockets are full-duplex, so a reader pump feeds inbound frames into a queue
    the app's `receive` drains, while `send` writes the app's frames out. The h11
    connection already consumed the handshake request; `initiate_upgrade_connection`
    hands wsproto the parsed request so it can produce the `101` on accept.
    """
    ws = WSConnection(ConnectionType.SERVER)
    handshake = [(bytes(name), bytes(value)) for name, value in request.headers]
    ws.initiate_upgrade_connection(handshake, request.target.decode("latin-1"))
    for _ in ws.events():
        pass  # discard wsproto's Request; the scope is built from the h11 request
    scope = encode_websocket_scope(websocket_scope_from_request(request, scheme=scheme, server=server, client=client))

    inbound: asyncio.Queue[RawMessage] = asyncio.Queue()
    inbound.put_nowait(encode_websocket_inbound(WebsocketConnect()))
    finished = asyncio.Event()
    accepted = False
    text_parts: list[str] = []
    binary_parts = bytearray()

    async def drain_events() -> bool:
        for event in ws.events():
            match event:
                case TextMessage(data=data, message_finished=done):
                    text_parts.append(data)
                    if done:
                        message = WebsocketReceive(WebsocketText("".join(text_parts)))
                        text_parts.clear()
                        await inbound.put(encode_websocket_inbound(message))
                case BytesMessage(data=data, message_finished=done):
                    binary_parts.extend(data)
                    if done:
                        message = WebsocketReceive(WebsocketBinary(bytes(binary_parts)))
                        binary_parts.clear()
                        await inbound.put(encode_websocket_inbound(message))
                case Ping():
                    writer.write(ws.send(event.response()))
                    await writer.drain()
                case Pong():
                    pass
                case CloseConnection(code=code, reason=reason):
                    writer.write(ws.send(event.response()))
                    await writer.drain()
                    await inbound.put(encode_websocket_inbound(WebsocketDisconnect(code=code, reason=reason or "")))
                    return False
        return True

    async def pump() -> None:
        try:
            if not (trailing := conn.trailing_data[0]) or await _feed(ws, trailing, drain_events):
                while True:
                    data = await reader.read(_BUFFER)
                    if data == b"" or not await _feed(ws, data, drain_events):
                        break
        except Exception:
            pass
        await inbound.put(encode_websocket_inbound(WebsocketDisconnect(code=1006, reason="")))
        finished.set()

    async def receive() -> RawMessage:
        if finished.is_set() and inbound.empty():
            return encode_websocket_inbound(WebsocketDisconnect(code=1006, reason=""))
        return await inbound.get()

    async def send(message: RawMessage) -> None:
        nonlocal accepted
        outbound = parse_websocket_outbound(message)
        for event in ws_events_from_outbound(outbound, accepted=accepted):
            writer.write(ws.send(event))
        await writer.drain()
        if isinstance(outbound, WebsocketAccept):
            accepted = True
        elif isinstance(outbound, WebsocketClose):
            finished.set()

    async with background_task(pump()):
        with suppress(Exception):
            await app(scope, receive, send)


async def _feed(ws: WSConnection, data: bytes, drain_events: Callable[[], Awaitable[bool]]) -> bool:
    """Feed bytes to wsproto and drain the resulting events; False if the peer closed."""
    ws.receive_data(data)
    return await drain_events()


async def _serve_connection(
    app: ASGIApp,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    conn = h11.Connection(our_role=h11.SERVER)
    server = _address(writer.get_extra_info("sockname"))
    client = _address(writer.get_extra_info("peername"))
    secure = writer.get_extra_info("ssl_object") is not None
    try:
        while True:
            try:
                request = await _read_request(conn, reader)
            except h11.RemoteProtocolError as exc:
                await _send_simple(conn, writer, exc.error_status_hint, b"bad request\n")
                return
            if request is None:
                return
            if is_websocket_upgrade(request):
                scheme = "wss" if secure else "ws"
                await _serve_websocket(
                    app, conn, reader, writer, scheme=scheme, server=server, client=client, request=request
                )
                return
            scheme = "https" if secure else "http"
            keep_alive = await _run_request(
                app, conn, reader, writer, scheme=scheme, server=server, client=client, request=request
            )
            if not keep_alive:
                return
            try:
                conn.start_next_cycle()
            except h11.LocalProtocolError:
                return
    finally:
        with suppress(OSError):
            writer.close()
            await writer.wait_closed()


def _bound_address(server: asyncio.Server) -> tuple[str, int]:
    host, port = server.sockets[0].getsockname()[:2]
    return host, port


async def _serve_accepted(
    app: ASGIApp,
    conn: socket.socket,
    *,
    ssl_context: ssl.SSLContext | None,
    ssl_handshake_timeout: float | None,
    ssl_shutdown_timeout: float | None,
) -> None:
    """Wrap a freshly accepted socket in streams, then serve one connection.

    The dual of what `asyncio.start_server` does internally per accepted socket,
    spelled out here because the bounded path drives its own accept loop: build a
    `StreamReader`/`StreamWriter` over the socket (performing the TLS handshake when
    `ssl_context` is set), then hand them to `_serve_connection`. A handshake that
    fails closes the socket without disturbing the accept loop.
    """
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(loop=loop)
    protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
    try:
        transport, _ = await loop.connect_accepted_socket(
            lambda: protocol,
            conn,
            ssl=ssl_context,
            ssl_handshake_timeout=ssl_handshake_timeout,
            ssl_shutdown_timeout=ssl_shutdown_timeout,
        )
    except ssl.SSLError, OSError:
        conn.close()
        return
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    await _serve_connection(app, reader, writer)


@asynccontextmanager
async def serving(
    app: ASGIApp,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    max_pending_connections: int = 100,
    max_concurrent_connections: int | None = None,
    ssl_context: ssl.SSLContext | None = None,
    ssl_handshake_timeout: float | None = None,
    ssl_shutdown_timeout: float | None = None,
) -> AsyncIterator[tuple[str, int]]:
    """Serve `app` over HTTP for the duration of the `with` block.

    Drives the lifespan cycle, binds a socket (`port=0` picks a free one), and
    yields the bound `(host, port)`. On exit it stops accepting, cancels any
    in-flight connections, and runs lifespan shutdown. This is the testable seam
    `serve` runs forever around.

    `max_pending_connections` is the kernel's listen backlog: the depth of the
    queue of connections that have completed the TCP handshake but have not yet
    been accepted. It absorbs accept bursts; once a connection is accepted it no
    longer counts against it. When the queue is *full*, the OS handles further
    connection attempts: on Linux the new `SYN` is dropped, so the client's
    `connect()` retransmits and either succeeds once room frees or eventually
    times out (a client may also see "connection refused" on platforms that
    reset instead). Nothing is queued in the server process.

    `max_concurrent_connections` caps how many connections are accepted and served
    at once (default: unlimited). At the cap the server stops calling `accept()`,
    so additional connections wait in that kernel pending queue rather than as
    parked tasks: no per-connection task is created and no TLS handshake or request
    parsing begins until a slot frees. Enforcing this requires a single listening
    socket, so it cannot be combined with a `host` that resolves to several
    addresses.

    Pass `ssl_context` to serve `https`/`wss` directly; `server_ssl_context` builds
    one for the common case. `ssl_handshake_timeout` bounds a single TLS handshake
    (asyncio's default is 60s) and `ssl_shutdown_timeout` the closing `close_notify`
    exchange (default 30s); both are meaningful only alongside `ssl_context`.
    """
    if max_concurrent_connections is None:
        connections: set[asyncio.Task[None]] = set()

        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            task = asyncio.current_task()
            assert task is not None
            connections.add(task)
            try:
                await _serve_connection(app, reader, writer)
            finally:
                connections.discard(task)

        async with run_lifespan(app):
            server = await asyncio.start_server(
                handle,
                host,
                port,
                backlog=max_pending_connections,
                ssl=ssl_context,
                ssl_handshake_timeout=ssl_handshake_timeout,
                ssl_shutdown_timeout=ssl_shutdown_timeout,
            )
            try:
                yield _bound_address(server)
            finally:
                server.close()
                await server.wait_closed()
                pending = list(connections)
                for task in pending:
                    task.cancel()
                for task in pending:
                    with suppress(asyncio.CancelledError):
                        await task
        return

    # The bounded path drives its own accept loop, so it owns the listening
    # socket directly (a single address, which is what gating the accept needs)
    # rather than handing it to `asyncio.start_server`'s built-in accept loop.
    family, socktype, proto, _canon, sockaddr = socket.getaddrinfo(
        host, port, type=socket.SOCK_STREAM, flags=socket.AI_PASSIVE
    )[0]
    listener = socket.socket(family, socktype, proto)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(sockaddr)
    listener.listen(max_pending_connections)
    listener.setblocking(False)
    bound_host, bound_port = listener.getsockname()[:2]
    loop = asyncio.get_running_loop()

    async def incoming() -> AsyncIterator[Awaitable[None]]:
        # `limit_concurrency` advances this generator (and so awaits the next
        # `accept`) only while below the limit, so the accept itself is gated.
        while True:
            conn, _ = await loop.sock_accept(listener)
            yield _serve_accepted(
                app,
                conn,
                ssl_context=ssl_context,
                ssl_handshake_timeout=ssl_handshake_timeout,
                ssl_shutdown_timeout=ssl_shutdown_timeout,
            )

    async def accept() -> None:
        async for _done in limit_concurrency(incoming(), max_concurrent_connections):
            pass

    async with run_lifespan(app):
        try:
            async with background_task(accept()):
                yield bound_host, bound_port
        finally:
            listener.close()


async def serve(
    app: ASGIApp,
    *,
    host: str = "127.0.0.1",
    port: int = 8000,
    max_pending_connections: int = 100,
    max_concurrent_connections: int | None = None,
    ssl_context: ssl.SSLContext | None = None,
    ssl_handshake_timeout: float | None = None,
    ssl_shutdown_timeout: float | None = None,
) -> None:
    """Serve `app` over HTTP until cancelled. The simple entrypoint over `serving`."""
    async with serving(
        app,
        host=host,
        port=port,
        max_pending_connections=max_pending_connections,
        max_concurrent_connections=max_concurrent_connections,
        ssl_context=ssl_context,
        ssl_handshake_timeout=ssl_handshake_timeout,
        ssl_shutdown_timeout=ssl_shutdown_timeout,
    ):
        await sleep_forever()
