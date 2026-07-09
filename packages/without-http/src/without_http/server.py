from __future__ import annotations

import asyncio
import logging
import ssl
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import asynccontextmanager
from contextlib import suppress
from dataclasses import dataclass

import h2.config
import h2.connection
import h2.events
import h2.exceptions
import h11
from without import background_task
from without import cancel_futures
from without_asgi import ASGIApp
from without_asgi import Disconnect
from without_asgi import EarlyHint
from without_asgi import Inbound
from without_asgi import PathSend
from without_asgi import RawMessage
from without_asgi import RequestBody
from without_asgi import ResponseBody
from without_asgi import ResponseDebug
from without_asgi import ResponseStart
from without_asgi import ResponseTrailers
from without_asgi import ServerPush
from without_asgi import WebsocketAccept
from without_asgi import WebsocketBinary
from without_asgi import WebsocketClose
from without_asgi import WebsocketConnect
from without_asgi import WebsocketDisconnect
from without_asgi import WebsocketReceive
from without_asgi import WebsocketText
from without_asgi import ZeroCopySend
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

from without_http.h2_wire import H2_PREFACE
from without_http.h2_wire import early_hint_headers
from without_http.h2_wire import response_headers
from without_http.h2_wire import scope_from_h2_headers
from without_http.h11_wire import h11_events_from_outbound
from without_http.h11_wire import inbound_from_event
from without_http.h11_wire import scope_from_request
from without_http.lifespan import run_lifespan
from without_http.ws_wire import is_websocket_upgrade
from without_http.ws_wire import websocket_scope_from_request
from without_http.ws_wire import ws_events_from_outbound

_BUFFER = 65536

logger = logging.getLogger(__name__)


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
            if (
                chunk is not None
            ):  # pragma: no branch - h11.send returns None only for ConnectionClosed, never sent here
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
            # Unreachable defensive guards: after NEED_DATA is handled, h11 yields only
            # body-phase Events (Data, EndOfMessage, ConnectionClosed), each of which
            # `inbound_from_event` maps to a non-None Inbound; it never returns PAUSED
            # here (that needs a completed request, which sets `request_done` above first).
            if not isinstance(event, h11.Event):  # pragma: no cover
                logger.warning(f"Treating unexpected h11 event as a client disconnect: {event!r}")
                request_done = True
                return encode_inbound(Disconnect())
            inbound = inbound_from_event(event)
            if inbound is None:  # pragma: no cover
                logger.warning(f"Treating unmappable h11 event as a client disconnect: {event!r}")
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
            if (
                chunk is not None
            ):  # pragma: no branch - h11.send returns None only for ConnectionClosed, never sent here
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
    except Exception:  # noqa: BLE001 - isolate a crashing app from the connection loop; turned into a 500 below
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
    """
    Drive one WebSocket connection over the HTTP/1.1 upgrade, via wsproto.

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
                case _:  # pragma: no cover - post-handshake wsproto emits only the events handled above
                    logger.warning(f"Discarding unexpected WebSocket event from wsproto: {event!r}")
        return True

    async def pump() -> None:
        try:
            if not (trailing := conn.trailing_data[0]) or await _feed(ws, trailing, drain_events):
                while True:
                    data = await reader.read(_BUFFER)
                    if data == b"" or not await _feed(ws, data, drain_events):
                        break
        # wsproto folds malformed frames into a CloseConnection event rather than
        # raising, so this fires only on a socket failure mid-write (a racy disconnect);
        # either way the connection becomes a WebSocket disconnect below.
        except Exception as exc:  # noqa: BLE001  # pragma: no cover
            logger.warning(f"WebSocket read pump ended on a connection error: {exc!r}")
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


# Matches Go net/http's `rstAvoidanceDelay`: long enough for our FIN, and the
# response we already sent, to reach the client before we fully close; short
# enough that a client which keeps sending cannot hold the connection open.
_LINGER_TIMEOUT = 0.5


async def _lingering_close(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """
    Close a connection whose peer may still be sending, without RST-ing it.

    Closing a socket that still has unread inbound data makes the OS send a TCP
    `RST` instead of a clean `FIN`, and that reset can race ahead of a response we
    already wrote, so the peer reads `ECONNRESET` and never sees it. This is the
    early-response case: we answered (say a `413`) before reading the request body.

    Mirror nginx `lingering_close` and Go `closeWriteAndWait`: half-close our write
    side so a well-behaved client learns to stop sending and read the response,
    then read and discard any in-flight body for a short bounded window before
    closing. We never drain to end-of-input, so a client that keeps sending only
    gets the `RST` it would have gotten anyway; it cannot hold the connection open.

    Half-closing needs a duplex transport; TLS cannot half-close, so over TLS we
    skip the `FIN` and rely on the bounded drain alone (as nginx notes, the signal
    cannot reach a client behind a TLS-terminating proxy in any case).
    """
    if writer.can_write_eof():
        with suppress(OSError):
            writer.write_eof()
    with suppress(OSError, TimeoutError):
        async with asyncio.timeout(_LINGER_TIMEOUT):
            while await reader.read(_BUFFER):
                pass
    with suppress(OSError):
        writer.close()
        await writer.wait_closed()


async def _serve_connection(
    app: ASGIApp,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    """
    Pick the wire protocol for one connection, serve it, then close the socket.

    HTTP/2 is selected two ways: by ALPN (`h2`) when serving over TLS, and by
    *prior knowledge* over cleartext, recognizing the h2 connection preface in the
    first bytes (which must be peeked before feeding `h11`, since `h11` would
    mis-parse `PRI` as an HTTP/1 method). Everything else is HTTP/1.1.
    """
    server = _address(writer.get_extra_info("sockname"))
    client = _address(writer.get_extra_info("peername"))
    ssl_object = writer.get_extra_info("ssl_object")
    secure = ssl_object is not None
    alpn = ssl_object.selected_alpn_protocol() if ssl_object is not None else None
    try:
        if alpn == "h2":
            await _serve_h2_connection(app, reader, writer, initial=b"", secure=secure, server=server, client=client)
            return
        initial = b""
        if alpn is None:
            initial = await reader.read(_BUFFER)
            if initial.startswith(H2_PREFACE):
                await _serve_h2_connection(
                    app, reader, writer, initial=initial, secure=secure, server=server, client=client
                )
                return
        await _serve_h11_connection(app, reader, writer, initial=initial, secure=secure, server=server, client=client)
    finally:
        with suppress(OSError):
            writer.close()
            await writer.wait_closed()


async def _serve_h11_connection(
    app: ASGIApp,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    initial: bytes,
    secure: bool,
    server: tuple[str, int] | None,
    client: tuple[str, int] | None,
) -> None:
    """Serve sequential HTTP/1.1 requests (and WebSocket upgrades) on one connection."""
    conn = h11.Connection(our_role=h11.SERVER)
    if initial:
        conn.receive_data(initial)
    while True:
        try:
            request = await _read_request(conn, reader)
        except h11.RemoteProtocolError as exc:
            # An early error response before the (malformed) body was read: linger so
            # the client reads the response instead of an `RST`. See `_lingering_close`.
            await _send_simple(conn, writer, exc.error_status_hint, b"bad request\n")
            await _lingering_close(reader, writer)
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
            # An unread request body means the peer is still sending; close with a
            # lingering FIN so an early response is not lost to an `RST`. A cleanly
            # finished exchange (body fully read) needs none of this. See
            # `_lingering_close`.
            if conn.their_state is not h11.DONE:
                await _lingering_close(reader, writer)
            return
        try:
            conn.start_next_cycle()
        # Unreachable: keep_alive is only true when both roles reached DONE, which is
        # exactly the state start_next_cycle requires; a non-reusable cycle returns above.
        except h11.LocalProtocolError as exc:  # pragma: no cover
            logger.warning(f"Closing HTTP/1.1 connection after start_next_cycle failed: {exc!r}")
            return


@dataclass(slots=True)
class _H2Stream:
    """The per-request mutable state the read loop and the stream's app task share."""

    inbound: asyncio.Queue[Inbound]
    window: asyncio.Event
    head: bool
    response_started: bool = False
    response_done: bool = False


async def _serve_h2_connection(
    app: ASGIApp,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    initial: bytes,
    secure: bool,
    server: tuple[str, int] | None,
    client: tuple[str, int] | None,
) -> None:
    """
    Serve concurrent multiplexed HTTP/2 requests on one connection.

    Each request stream drives its own ASGI app invocation (the per-request
    processor model), so many run at once over the single connection. The read loop
    feeds wire bytes to one shared `h2.Connection` and dispatches its events; every
    stream's `send` writes back through that same connection. A single `lock`
    serializes all access to the connection object and the writer.

    The flow-control invariant: a body sender that finds the stream's window empty
    clears its wake event and waits *while holding the lock*, and the read loop only
    ever sets that event after applying a `WINDOW_UPDATE` under the same lock. So the
    window can never grow between a sender's check and its wait, which would
    otherwise be a lost wakeup that strands the response.
    """
    scheme = "https" if secure else "http"
    conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False, header_encoding=None))
    lock = asyncio.Lock()
    streams: dict[int, _H2Stream] = {}
    tasks: set[asyncio.Task[None]] = set()
    closed = asyncio.Event()

    async def send_body(stream_id: int, stream: _H2Stream, body: bytes, *, end: bool) -> None:
        remaining = memoryview(b"" if stream.head else body)
        while True:
            async with lock:
                try:
                    window = conn.local_flow_control_window(stream_id)
                # Defensive: h2 returns a stale window for a reset stream rather than
                # raising here, so `send_data` below is what rejects a closed stream.
                except h2.exceptions.StreamClosedError as exc:  # pragma: no cover
                    logger.warning(f"Aborting body send for closed HTTP/2 stream {stream_id}: {exc!r}")
                    return
                sendable = min(len(remaining), window, conn.max_outbound_frame_size)
                if sendable > 0 or len(remaining) == 0:
                    chunk, remaining = remaining[:sendable].tobytes(), remaining[sendable:]
                    last = end and len(remaining) == 0
                    try:
                        conn.send_data(stream_id, chunk, end_stream=last)
                    except h2.exceptions.ProtocolError, h2.exceptions.StreamClosedError:
                        return
                    writer.write(conn.data_to_send())
                    blocked = False
                else:
                    stream.window.clear()
                    blocked = True
            if blocked:
                await stream.window.wait()
                continue
            await writer.drain()
            if len(remaining) == 0:
                return

    async def send_outbound(stream_id: int, stream: _H2Stream, message: RawMessage) -> None:
        outbound = parse_outbound(message)
        match outbound:
            case ResponseStart(status, headers, _trailers):
                stream.response_started = True
                async with lock:
                    conn.send_headers(stream_id, response_headers(status, headers))
                    writer.write(conn.data_to_send())
                await writer.drain()
            case ResponseBody(body, more_body):
                await send_body(stream_id, stream, body, end=not more_body)
                if not more_body:
                    stream.response_done = True
            case EarlyHint(links):
                async with lock:
                    conn.send_headers(stream_id, early_hint_headers(links))
                    writer.write(conn.data_to_send())
                await writer.drain()
            case ServerPush() | ZeroCopySend() | PathSend() | ResponseTrailers() | ResponseDebug():
                raise NotImplementedError(f"{type(outbound).__name__} is not supported over HTTP/2")

    async def finalize_incomplete(stream_id: int, stream: _H2Stream) -> None:
        async with lock:
            with suppress(h2.exceptions.ProtocolError, h2.exceptions.StreamClosedError):
                if stream.response_started:
                    conn.reset_stream(stream_id)
                else:
                    conn.send_headers(
                        stream_id, [(b":status", b"500"), (b"content-type", b"text/plain; charset=utf-8")]
                    )
                    conn.send_data(stream_id, b"internal server error\n", end_stream=True)
                writer.write(conn.data_to_send())
        await writer.drain()

    async def run_stream(stream_id: int, headers: list[tuple[bytes, bytes]], stream: _H2Stream) -> None:
        scope = scope_from_h2_headers(headers, scheme=scheme, server=server, client=client)
        request_done = False

        async def receive() -> RawMessage:
            nonlocal request_done
            if request_done:
                return encode_inbound(Disconnect())
            inbound = await stream.inbound.get()
            if isinstance(inbound, Disconnect) or (isinstance(inbound, RequestBody) and not inbound.more_body):
                request_done = True
            return encode_inbound(inbound)

        async def send(message: RawMessage) -> None:
            await send_outbound(stream_id, stream, message)

        try:
            try:
                await app(encode_http_scope(scope), receive, send)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - isolate one stream's app failure; finalized as incomplete below
                pass
            if not stream.response_done:
                await finalize_incomplete(stream_id, stream)
        finally:
            streams.pop(stream_id, None)

    async def handle_event(event: h2.events.Event) -> None:
        match event:
            case h2.events.RequestReceived(stream_id=stream_id, headers=headers):
                method = next((v for n, v in headers if n == b":method"), b"")
                new = _H2Stream(inbound=asyncio.Queue(), window=asyncio.Event(), head=method == b"HEAD")
                streams[stream_id] = new
                task = asyncio.create_task(run_stream(stream_id, list(headers), new))
                tasks.add(task)
                task.add_done_callback(tasks.discard)
            case h2.events.DataReceived(stream_id=stream_id, data=data, flow_controlled_length=length):
                # `is not None` guards are defensive: h2 raises a protocol error rather
                # than emitting a body/end/reset/window event for a stream it is not
                # tracking, so the miss branch cannot be reached.
                if (target := streams.get(stream_id)) is not None:
                    target.inbound.put_nowait(RequestBody(body=bytes(data), more_body=True))
                else:  # pragma: no cover - h2 rejects DATA for an untracked stream before this point
                    logger.warning(f"Discarding HTTP/2 DATA for untracked stream {stream_id}")
                async with lock:
                    conn.acknowledge_received_data(length, stream_id)
                    writer.write(conn.data_to_send())
                await writer.drain()
            case h2.events.StreamEnded(stream_id=stream_id):
                if (target := streams.get(stream_id)) is not None:
                    target.inbound.put_nowait(RequestBody(body=b"", more_body=False))
                else:  # pragma: no cover - h2 rejects END_STREAM for an untracked stream before this point
                    logger.warning(f"Discarding HTTP/2 END_STREAM for untracked stream {stream_id}")
            case h2.events.StreamReset(stream_id=stream_id):
                if (target := streams.get(stream_id)) is not None:
                    target.inbound.put_nowait(Disconnect())
                    # Wake a sender blocked on the window so it observes the closed
                    # stream and unwinds, rather than lingering until connection close.
                    target.window.set()
                else:  # pragma: no cover - h2 rejects RST_STREAM for an untracked stream before this point
                    logger.warning(f"Discarding HTTP/2 RST_STREAM for untracked stream {stream_id}")
            case h2.events.WindowUpdated(stream_id=stream_id):
                if stream_id == 0:
                    for target in streams.values():
                        target.window.set()
                elif (target := streams.get(stream_id)) is not None:
                    target.window.set()
                else:  # pragma: no cover - h2 rejects WINDOW_UPDATE for an untracked stream before this point
                    logger.warning(f"Discarding HTTP/2 WINDOW_UPDATE for untracked stream {stream_id}")
            case h2.events.RemoteSettingsChanged():
                # A changed initial window resizes every stream's window, so wake all
                # blocked senders to re-check rather than tracking which grew.
                for target in streams.values():
                    target.window.set()
            case h2.events.ConnectionTerminated():
                closed.set()
            case _:
                pass

    async def feed(data: bytes) -> bool:
        async with lock:
            try:
                events = conn.receive_data(data)
            except h2.exceptions.ProtocolError:
                writer.write(conn.data_to_send())
                events = []
                closed.set()
            else:
                writer.write(conn.data_to_send())
        await writer.drain()
        for event in events:
            await handle_event(event)
        return not closed.is_set()

    async with lock:
        conn.initiate_connection()
        writer.write(conn.data_to_send())
    await writer.drain()
    try:
        if initial and not await feed(initial):
            return
        while True:
            data = await reader.read(_BUFFER)
            if not data or not await feed(data):
                return
    finally:
        await cancel_futures(tasks)


@dataclass(slots=True)
class _LiveConnections:
    """
    A live count of connections currently being served, for observability.

    `tracked` brackets one connection's lifetime, moving the count up while it runs
    and back down when it ends; `in_flight` is always readable, so the server can
    report how many connections it is serving.
    """

    in_flight: int = 0

    @asynccontextmanager
    async def tracked(self) -> AsyncIterator[None]:
        self.in_flight += 1
        try:
            yield
        finally:
            self.in_flight -= 1


@dataclass(frozen=True, slots=True)
class Server:
    """
    A handle to a running server, yielded by `serving` for the block's duration.

    `host`/`port` are the bound address (`port` is the OS-assigned one when `port=0`
    was requested). `in_flight` reports how many connections are being served right
    now, for metrics and observability. More fields (request counts, byte totals) can
    join as the server grows.
    """

    host: str
    port: int
    _connections: _LiveConnections

    @property
    def in_flight(self) -> int:
        return self._connections.in_flight


@asynccontextmanager
async def serving(
    app: ASGIApp,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    max_pending_connections: int = 100,
    ssl_context: ssl.SSLContext | None = None,
    ssl_handshake_timeout: float | None = None,
    ssl_shutdown_timeout: float | None = None,
) -> AsyncIterator[Server]:
    """
    Serve `app` over HTTP for the duration of the `with` block.

    Drives the lifespan cycle, binds a socket (`port=0` picks a free one) with
    `asyncio.start_server`, and yields a `Server` (its bound `host`/`port`, plus live
    metrics like `in_flight`). On exit it stops accepting, cancels any in-flight
    connections, and runs lifespan shutdown. To run a server until cancelled, hold the
    block open with `without.sleep_forever()` (or your own run loop, e.g. one that
    handles signals).

    `max_pending_connections` is the kernel's listen backlog: the depth of the
    queue of connections that have completed the TCP handshake but have not yet
    been accepted. It absorbs accept bursts; once a connection is accepted it no
    longer counts against it. When the queue is *full*, the OS handles further
    connection attempts: on Linux the new `SYN` is dropped, so the client's
    `connect()` retransmits and either succeeds once room frees or eventually
    times out (a client may also see "connection refused" on platforms that
    reset instead). Nothing is queued in the server process.

    To bound *in-flight requests* (the right limit once HTTP/2 multiplexes many
    requests over one connection), wrap the app in `limit_concurrent_requests`, which
    sheds with a `503` rather than capping connections. This server does not cap raw
    connections: the kernel listen backlog above and OS resource limits provide that
    backpressure, and `Server.in_flight` reports the live connection count for metrics.

    `asyncio.start_server` owns the accept loop, so it survives transient accept
    failures (pausing for `ACCEPT_RETRY_DELAY` on resource exhaustion) and binds every
    address `host` resolves to.

    Pass `ssl_context` to serve `https`/`wss` directly; `server_ssl_context` builds
    one for the common case. `ssl_handshake_timeout` bounds a single TLS handshake
    (asyncio's default is 60s) and `ssl_shutdown_timeout` the closing `close_notify`
    exchange (default 30s); both are meaningful only alongside `ssl_context`.
    """
    live = _LiveConnections()
    connections: set[asyncio.Task[None]] = set()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        connections.add(task)
        try:
            async with live.tracked():
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
        bound_host, bound_port = server.sockets[0].getsockname()[:2]
        try:
            yield Server(host=bound_host, port=bound_port, _connections=live)
        finally:
            server.close()
            await cancel_futures(connections)
            await server.wait_closed()
