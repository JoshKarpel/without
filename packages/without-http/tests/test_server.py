from __future__ import annotations

import asyncio
import logging
import ssl
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import asynccontextmanager
from contextlib import suppress
from datetime import timedelta
from typing import cast

import h2.config
import h2.connection
import h2.events
import h11
import httpx
import pytest
from pytest_mock import MockerFixture
from without_asgi import ASGIApp
from without_asgi import HttpScope
from without_asgi import RawMessage
from without_asgi import Receive
from without_asgi import Response
from without_asgi import Send
from without_asgi import make_asgi_app
from without_asgi.routing import buffered
from without_http import serving
from without_http.server import _address
from without_http.server import _consume_buffered_request
from without_http.server import _Limits
from without_http.server import _lingering_close
from without_http.server import _LiveConnections
from without_http.server import _send_simple
from without_http.server import _serve_connection
from without_http.server import _serve_h11_connection
from wsproto import ConnectionType
from wsproto import WSConnection
from wsproto.events import Request as WsRequest
from wsproto.events import TextMessage

type _Endpoint = tuple[asyncio.StreamReader, asyncio.StreamWriter]
type _StreamPairFactory = Callable[[], Awaitable[tuple[_Endpoint, _Endpoint]]]

_DEFAULT_LIMITS = _Limits(
    max_concurrent_streams=100,
    max_stream_resets=200,
    idle_timeout=None,
    max_websocket_message_bytes=None,
)


async def echo_app(scope: RawMessage, receive: Receive, send: Send) -> None:
    """A raw ASGI app that echoes the request line and body. Has no lifespan support."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    body = b""
    more = True
    while more:
        message = await receive()
        if message["type"] == "http.disconnect":  # pragma: no cover - clients here never disconnect mid-body
            return
        chunk = message.get("body", b"")
        assert isinstance(chunk, bytes)
        body += chunk
        more = bool(message.get("more_body", False))
    method = str(scope["method"])
    path = str(scope["path"])
    payload = f"{method} {path} {body.decode()}".encode()
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": payload})


_UNREAD_BODY = b"answered without reading"


async def unread_app(scope: RawMessage, receive: Receive, send: Send) -> None:
    """A raw ASGI app that answers without ever calling `receive`, as FastAPI does on a GET."""
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"text/plain"), (b"content-length", str(len(_UNREAD_BODY)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": _UNREAD_BODY})


def configured_app() -> ASGIApp:
    @asynccontextmanager
    async def lifespan() -> AsyncIterator[str]:
        yield "configured-state"

    def handle(state: str, head: HttpScope, body: bytes) -> Response:
        return Response(status=200, headers=((b"content-type", b"text/plain"),), body=f"{state}:{head.path}".encode())

    return make_asgi_app(lifespan, http=buffered(handle))


def crash_app() -> ASGIApp:
    @asynccontextmanager
    async def lifespan() -> AsyncIterator[None]:
        yield None

    def handle(state: None, head: HttpScope, body: bytes) -> Response:
        raise RuntimeError("handler exploded")

    return make_asgi_app(lifespan, http=buffered(handle))


@asynccontextmanager
async def _client(app: ASGIApp) -> AsyncIterator[httpx.AsyncClient]:
    async with serving(app) as server, httpx.AsyncClient(base_url=f"http://{server.host}:{server.port}") as client:
        yield client


async def test_serves_a_get_response() -> None:
    async with _client(echo_app) as client:
        response = await client.get("/items")

    assert response.status_code == 200
    assert response.text == "GET /items "


@pytest.mark.security("an idle connection is closed after the idle timeout (slowloris defense)")
async def test_an_idle_connection_is_closed_after_the_idle_timeout() -> None:
    async with serving(echo_app, idle_timeout=timedelta(seconds=0.1)) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        # Send nothing: the server's read outlives the idle timeout and it closes the socket.
        assert await reader.read(65536) == b""
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()


@pytest.mark.security("the idle timeout is scoped: a request completing within it is served")
async def test_a_request_within_the_idle_timeout_is_served() -> None:
    async with serving(echo_app, idle_timeout=timedelta(seconds=30)) as server:
        async with httpx.AsyncClient(base_url=f"http://{server.host}:{server.port}") as client:
            response = await client.get("/items")

    assert response.status_code == 200
    assert response.text == "GET /items "


async def test_serves_a_post_body() -> None:
    async with _client(echo_app) as client:
        response = await client.post("/submit", content=b"payload")

    assert response.text == "POST /submit payload"


async def test_a_head_request_omits_the_body() -> None:
    async with _client(echo_app) as client:
        response = await client.head("/items")

    assert response.status_code == 200
    assert response.content == b""


async def test_keep_alive_serves_sequential_requests_on_one_connection() -> None:
    async with _client(echo_app) as client:
        first = await client.get("/one")
        second = await client.get("/two")

    assert first.text == "GET /one "
    assert second.text == "GET /two "


async def test_keep_alive_survives_an_app_that_never_reads_the_request() -> None:
    # h11 advances the client's state only as events are pulled, so an app that
    # ignores `receive` (FastAPI, on any request with no body parameter) leaves the
    # request's EndOfMessage unread and the connection looks unfinished. Driven over
    # a raw socket because httpx would transparently reconnect and hide the drop.
    async with serving(unread_app) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        statuses = []
        for path in (b"/one", b"/two"):
            writer.write(b"GET " + path + b" HTTP/1.1\r\nhost: test\r\n\r\n")
            await writer.drain()
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
            statuses.append(head.split(b"\r\n")[0])
            await asyncio.wait_for(reader.readexactly(len(_UNREAD_BODY)), timeout=5)
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()

    assert statuses == [b"HTTP/1.1 200 ", b"HTTP/1.1 200 "]


async def test_an_unread_partial_body_is_not_kept_alive() -> None:
    # The other side of consuming what the app never read: only h11's buffered bytes
    # count. A peer that promised 100 bytes and sent 10 still owes us a body, so the
    # connection must close rather than be reused for whatever it sends next.
    async with serving(unread_app) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.write(b"POST /x HTTP/1.1\r\nhost: test\r\ncontent-length: 100\r\n\r\n" + b"a" * 10)
        await writer.drain()
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
        await asyncio.wait_for(reader.readexactly(len(_UNREAD_BODY)), timeout=5)
        assert head.split(b"\r\n")[0] == b"HTTP/1.1 200 "
        assert await asyncio.wait_for(reader.read(), timeout=5) == b""
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()


async def test_an_unread_malformed_body_closes_the_connection_without_crashing() -> None:
    # Draining what the app never read can meet a malformed chunk header, which puts
    # h11 in ERROR. The response we already sent must still stand and the connection
    # must close quietly, rather than the parse error escaping the connection task.
    async with serving(unread_app) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.write(b"POST /x HTTP/1.1\r\nhost: test\r\ntransfer-encoding: chunked\r\n\r\nZZZZ\r\n")
        await writer.drain()
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
        body = await asyncio.wait_for(reader.readexactly(len(_UNREAD_BODY)), timeout=5)
        assert head.split(b"\r\n")[0] == b"HTTP/1.1 200 "
        assert body == _UNREAD_BODY
        assert await asyncio.wait_for(reader.read(), timeout=5) == b""
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()


async def test_threads_lifespan_state_into_the_handler() -> None:
    async with _client(configured_app()) as client:
        response = await client.get("/where")

    assert response.text == "configured-state:/where"


async def test_a_crashing_handler_returns_500() -> None:
    async with _client(crash_app()) as client:
        response = await client.get("/boom")

    assert response.status_code == 500


async def test_live_connections_counts_in_flight_connections() -> None:
    live = _LiveConnections()

    assert live.in_flight == 0
    async with live.tracked():
        assert live.in_flight == 1
        async with live.tracked():
            assert live.in_flight == 2
        assert live.in_flight == 1
    assert live.in_flight == 0


@pytest.mark.parametrize(
    ("info", "expected"),
    [
        (("198.51.100.7", 8000), ("198.51.100.7", 8000)),
        ("a-unix-socket-path", None),
        (("only-one-element",), None),
        (("host", "not-an-int"), None),
    ],
)
def test_address_parses_only_a_host_port_tuple(info: object, expected: tuple[str, int] | None) -> None:
    assert _address(info) == expected


async def test_serves_a_large_post_body_spanning_multiple_socket_reads() -> None:
    payload = b"q" * 200_000
    async with _client(echo_app) as client:
        response = await client.post("/big", content=payload)

    assert response.text == "POST /big " + payload.decode()


async def receive_after_done_app(scope: RawMessage, receive: Receive, send: Send) -> None:
    """Read the body to completion, then call `receive` once more to observe the disconnect."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    more = True
    while more:
        message = await receive()
        more = bool(message.get("more_body", False))
    trailing = await receive()
    trailing_type = trailing["type"]
    assert isinstance(trailing_type, str)
    body = trailing_type.encode()
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": body})


async def test_receiving_after_the_request_body_is_done_yields_a_disconnect() -> None:
    async with _client(receive_after_done_app) as client:
        response = await client.post("/x", content=b"payload")

    assert response.text == "http.disconnect"


async def test_a_malformed_request_gets_a_400() -> None:
    async with serving(echo_app) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.write(b"!!! not a valid request line !!!\r\n\r\n")
        await writer.drain()
        status_line = await reader.readline()
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()

    assert status_line.startswith(b"HTTP/1.1 400")


class _ResettingReader:
    """A reader that raises a peer reset, as a socket does when the client aborts."""

    async def read(self, _size: int) -> bytes:
        raise ConnectionResetError(104, "connection reset by peer")


class _RecordingWriter:
    """A writer that records only what `_serve_connection` needs: extra info and close."""

    def __init__(self) -> None:
        self.closed = False

    def get_extra_info(self, _name: str) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


async def test_a_peer_reset_while_awaiting_a_request_ends_the_connection_without_raising() -> None:
    async def unused_app(scope: RawMessage, receive: Receive, send: Send) -> None:  # pragma: no cover
        raise AssertionError("the app is never reached when the peer resets first")

    writer = _RecordingWriter()

    # A peer reset is a normal end-of-connection, so serving one must complete
    # quietly rather than letting the error escape the connection task.
    await _serve_connection(
        unused_app,
        cast(asyncio.StreamReader, _ResettingReader()),
        cast(asyncio.StreamWriter, writer),
        _DEFAULT_LIMITS,
    )

    assert writer.closed


async def test_a_malformed_request_lingers_so_the_client_reads_the_error(
    stream_pair: _StreamPairFactory,
) -> None:
    # Drive the connection loop directly over a socketpair (not through `serving`, whose
    # shutdown would cancel the connection mid-linger) so the lingering close runs to
    # completion: the peer must read the whole `400` even though it half-closed while the
    # server was still parsing, instead of losing it to an `RST`.
    (server_reader, server_writer), (client_reader, client_writer) = await stream_pair()
    client_writer.write(b"!!! not a valid request line !!!\r\n\r\n")
    await client_writer.drain()
    client_writer.write_eof()  # the peer stops sending; the server's linger drains to EOF

    await _serve_h11_connection(
        echo_app,
        server_reader,
        server_writer,
        initial=b"",
        secure=False,
        server=None,
        client=None,
        limits=_DEFAULT_LIMITS,
    )

    response = await client_reader.read()
    assert response.startswith(b"HTTP/1.1 400")
    assert response.endswith(b"bad request\n")


class _HalfCloseRefusingWriter:
    """
    A writer that cannot half-close its write side, as a TLS transport cannot.

    It deliberately has no `write_eof`, so a linger that asks for the `FIN` anyway fails
    loudly instead of quietly doing the wrong thing to a transport that cannot do it.
    """

    def __init__(self) -> None:
        self.closed = False

    def can_write_eof(self) -> bool:
        return False

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


async def test_lingering_over_a_transport_that_cannot_half_close_still_drains_and_closes() -> None:
    # TLS cannot half-close, so the linger has no `FIN` to send and rests on the bounded
    # drain alone: it must still read the peer's in-flight bytes off the socket before
    # closing, or the unread data turns the close into the `RST` the linger exists to avoid.
    reader = asyncio.StreamReader()
    reader.feed_data(b"in-flight body the client is still sending")
    reader.feed_eof()
    writer = _HalfCloseRefusingWriter()

    await _lingering_close(reader, cast(asyncio.StreamWriter, writer))

    assert reader.at_eof()
    assert writer.closed


async def test_reports_in_flight_connections_while_a_request_is_served() -> None:
    release = asyncio.Event()

    async def slow_app(scope: RawMessage, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            raise RuntimeError("this app serves only http")
        await release.wait()
        await send({"type": "http.response.start", "status": 200, "headers": [(b"connection", b"close")]})
        await send({"type": "http.response.body", "body": b"ok"})

    async with serving(slow_app) as server:
        assert server.in_flight == 0
        async with httpx.AsyncClient(base_url=f"http://{server.host}:{server.port}") as client:
            request = asyncio.create_task(client.get("/"))
            async with asyncio.timeout(5):
                while server.in_flight == 0:
                    await asyncio.sleep(0.001)
            assert server.in_flight == 1
            release.set()
            response = await request
            assert response.status_code == 200


async def scope_echo_app(scope: RawMessage, receive: Receive, send: Send) -> None:
    """Echo the request's scheme and the resolved server/client addresses in the body."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    scheme = scope["scheme"]
    server = scope["server"]
    client = scope["client"]
    assert isinstance(server, list)
    assert isinstance(client, list)
    payload = f"{scheme} {server[0]}:{server[1]} {client[0]}:{client[1]}".encode()
    headers = [(b"content-type", b"text/plain"), (b"content-length", str(len(payload)).encode())]
    await send({"type": "http.response.start", "status": 200, "headers": headers})
    await send({"type": "http.response.body", "body": payload})


async def sized_echo_app(scope: RawMessage, receive: Receive, send: Send) -> None:
    """Echo the request path in a `content-length`-framed body, so a raw reader can bound it."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    payload = f"body for {scope['path']}".encode()
    headers = [(b"content-type", b"text/plain"), (b"content-length", str(len(payload)).encode())]
    await send({"type": "http.response.start", "status": 200, "headers": headers})
    await send({"type": "http.response.body", "body": payload})


async def _read_one_http_response(reader: asyncio.StreamReader, *, read_body: bool = True) -> tuple[bytes, bytes]:
    """Read one `content-length`-framed HTTP/1.1 response, returning its status line and body."""
    head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
    status_line = head.split(b"\r\n", 1)[0]
    length = 0
    for line in head.split(b"\r\n"):
        name, sep, value = line.partition(b":")
        if sep and name.strip().lower() == b"content-length":
            length = int(value.strip())
    body = await asyncio.wait_for(reader.readexactly(length), timeout=5) if read_body and length else b""
    return status_line, body


async def test_http11_scope_carries_scheme_server_and_client() -> None:
    # Threads the resolved scheme and the distinct server/client addresses all the way to
    # the app: a dropped or Noned address (or a wrong scheme) shows up as a crash-to-500 or
    # a mismatched body, so every hop that carries them is pinned by this one round-trip.
    async with serving(scope_echo_app) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        client_host, client_port = writer.get_extra_info("sockname")
        writer.write(b"GET /s HTTP/1.1\r\nhost: test\r\n\r\n")
        await writer.drain()
        status_line, body = await _read_one_http_response(reader)
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()

    assert status_line == b"HTTP/1.1 200 "
    assert body == f"http {server.host}:{server.port} {client_host}:{client_port}".encode()


async def test_head_keeps_the_connection_alive_after_suppressing_the_body() -> None:
    # A HEAD response must skip its body Data (h11 rejects a body on a HEAD response), then
    # finish cleanly so the keep-alive connection is reusable. If the method check or the
    # skip is broken, sending the body raises and the connection dies before the next request.
    async with serving(sized_echo_app) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.write(b"HEAD /one HTTP/1.1\r\nhost: test\r\n\r\n")
        await writer.drain()
        head_status, head_body = await _read_one_http_response(reader, read_body=False)
        writer.write(b"GET /two HTTP/1.1\r\nhost: test\r\n\r\n")
        await writer.drain()
        get_status, get_body = await _read_one_http_response(reader)
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()

    assert head_status == b"HTTP/1.1 200 "
    assert head_body == b""
    assert get_status == b"HTTP/1.1 200 "
    assert get_body == b"body for /two"


async def test_a_crash_sends_the_500_body_and_does_not_keep_the_connection_alive() -> None:
    # A crashing handler yields exactly one 500 whose body is the internal-error text, then
    # the connection closes: a second pipelined request must be dropped, never served.
    async with serving(crash_app()) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.write(b"GET /a HTTP/1.1\r\nhost: test\r\n\r\nGET /b HTTP/1.1\r\nhost: test\r\n\r\n")
        await writer.drain()
        status_line, body = await _read_one_http_response(reader)
        leftover = await asyncio.wait_for(reader.read(65536), timeout=5)
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()

    assert status_line == b"HTTP/1.1 500 "
    assert body == b"internal server error\n"
    assert leftover == b""


@pytest.mark.security("partial request headers followed by a stall hit the idle timeout")
async def test_partial_request_headers_hit_the_idle_timeout() -> None:
    async with serving(echo_app, idle_timeout=timedelta(seconds=0.1)) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.write(b"GET /x HTTP/1.1\r\nhost: test\r\n")  # headers never terminated; then stall
        await writer.drain()
        assert await asyncio.wait_for(reader.read(), timeout=5) == b""
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()


@pytest.mark.security("a stalled partial request body hits the idle timeout (slowloris on the body)")
async def test_a_stalled_partial_body_hits_the_idle_timeout() -> None:
    async with serving(echo_app, idle_timeout=timedelta(seconds=0.1)) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port)
        writer.write(b"POST /x HTTP/1.1\r\nhost: test\r\ncontent-length: 100\r\n\r\n" + b"a" * 10)
        await writer.drain()
        response = await asyncio.wait_for(reader.read(), timeout=5)
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()

    assert response.startswith(b"HTTP/1.1 500")


@pytest.mark.security("closing a connection idle beyond the timeout is logged at INFO")
async def test_idle_timeout_logs_the_close_reason(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="without_http.server"):
        async with serving(echo_app, idle_timeout=timedelta(seconds=0.1)) as server:
            reader, writer = await asyncio.open_connection(server.host, server.port)
            assert await asyncio.wait_for(reader.read(), timeout=5) == b""
            writer.close()
            with suppress(OSError):  # pragma: no branch - wait_closed's suppressed-exception arc is unobservable here
                await writer.wait_closed()

    assert "Closing a connection idle beyond the idle timeout" in caplog.messages


def _server_conn_awaiting_response() -> h11.Connection:
    """An h11 server connection primed with a finished request, ready to send a response."""
    conn = h11.Connection(our_role=h11.SERVER)
    conn.receive_data(b"GET / HTTP/1.1\r\nhost: test\r\n\r\n")
    while not isinstance(conn.next_event(), h11.EndOfMessage):
        pass
    return conn


class _RecordingResponseWriter:
    """A writer that records the bytes written, for inspecting an exact response on the wire."""

    def __init__(self) -> None:
        self.buffer = bytearray()

    def write(self, data: bytes) -> None:
        self.buffer.extend(data)

    async def drain(self) -> None:
        return None


class _WriteRaisingWriter:
    """A writer whose `write` raises `OSError`, as a socket does when the peer has gone."""

    def write(self, _data: bytes) -> None:
        raise OSError(107, "transport endpoint is not connected")

    async def drain(self) -> None: ...  # never awaited: write raises before drain is reached


async def test_send_simple_writes_the_exact_response_on_the_wire() -> None:
    conn = _server_conn_awaiting_response()
    writer = _RecordingResponseWriter()

    await _send_simple(conn, cast(asyncio.StreamWriter, writer), 500, b"internal server error\n")

    assert bytes(writer.buffer) == (
        b"HTTP/1.1 500 \r\ncontent-type: text/plain; charset=utf-8\r\ncontent-length: 22\r\n\r\ninternal server error\n"
    )


async def test_send_simple_swallows_a_writer_oserror() -> None:
    conn = _server_conn_awaiting_response()
    writer = _WriteRaisingWriter()

    # The peer vanished mid-write; the error must be swallowed, not propagated to the loop.
    await _send_simple(conn, cast(asyncio.StreamWriter, writer), 500, b"internal server error\n")


async def test_send_simple_swallows_an_h11_protocol_error() -> None:
    conn = _server_conn_awaiting_response()
    for event in (h11.Response(status_code=200, headers=[(b"content-length", b"0")]), h11.EndOfMessage()):
        conn.send(event)  # drive the connection to DONE, where sending another Response is illegal
    writer = _RecordingResponseWriter()

    await _send_simple(conn, cast(asyncio.StreamWriter, writer), 500, b"internal server error\n")

    assert bytes(writer.buffer) == b""


async def test_consume_buffered_request_swallows_a_malformed_body() -> None:
    conn = h11.Connection(our_role=h11.SERVER)
    conn.receive_data(b"POST /x HTTP/1.1\r\nhost: test\r\ntransfer-encoding: chunked\r\n\r\nZZZZ\r\n")
    assert isinstance(conn.next_event(), h11.Request)  # advance to SEND_BODY with the bad body buffered

    _consume_buffered_request(conn)  # the malformed chunk header must not escape

    assert conn.their_state is h11.ERROR


class _LingerWriter:
    """A writer for `_lingering_close`, with opt-in failures on `write_eof` and `close`."""

    def __init__(self, *, eof_error: bool = False, close_error: bool = False) -> None:
        self.eof_error = eof_error
        self.close_error = close_error
        self.eof_written = False
        self.closed = False

    def can_write_eof(self) -> bool:
        return True

    def write_eof(self) -> None:
        if self.eof_error:
            raise OSError(9, "bad file descriptor")
        self.eof_written = True

    def close(self) -> None:
        if self.close_error:
            raise OSError(107, "transport endpoint is not connected")
        self.closed = True

    async def wait_closed(self) -> None:
        return None


class _ReadRaisingReader:
    """A reader whose `read` raises `OSError`, as a socket does on an aborted connection."""

    async def read(self, _size: int) -> bytes:
        raise OSError(104, "connection reset by peer")


class _EndlessReader:
    """A reader that always yields more bytes, so a bounded drain must time out to finish."""

    async def read(self, _size: int) -> bytes:
        await asyncio.sleep(0)
        return b"still sending"


async def test_lingering_close_swallows_a_write_eof_oserror() -> None:
    reader = asyncio.StreamReader()
    reader.feed_eof()
    writer = _LingerWriter(eof_error=True)

    await _lingering_close(reader, cast(asyncio.StreamWriter, writer))

    assert writer.closed


async def test_lingering_close_swallows_a_read_oserror() -> None:
    writer = _LingerWriter()

    await _lingering_close(cast(asyncio.StreamReader, _ReadRaisingReader()), cast(asyncio.StreamWriter, writer))

    assert writer.closed


async def test_lingering_close_swallows_a_close_oserror() -> None:
    reader = asyncio.StreamReader()
    reader.feed_eof()
    writer = _LingerWriter(close_error=True)

    await _lingering_close(reader, cast(asyncio.StreamWriter, writer))


async def test_lingering_close_is_bounded_by_the_linger_timeout(mocker: MockerFixture) -> None:
    # A peer that keeps sending cannot hold the drain open: the bounded window expires, the
    # timeout is swallowed, and the socket still closes rather than draining forever.
    mocker.patch("without_http.server._LINGER_TIMEOUT", 0.05)
    writer = _LingerWriter()

    await asyncio.wait_for(
        _lingering_close(cast(asyncio.StreamReader, _EndlessReader()), cast(asyncio.StreamWriter, writer)),
        timeout=5,
    )

    assert writer.closed


async def _drive_h11_over_socketpair(
    app: ASGIApp, request: bytes, stream_pair: _StreamPairFactory
) -> tuple[asyncio.StreamReader, _Endpoint]:
    """Feed one request into a server-side `_serve_h11_connection`, returning the client end."""
    (server_reader, server_writer), (client_reader, client_writer) = await stream_pair()
    client_writer.write(request)
    await client_writer.drain()
    await _serve_h11_connection(
        app,
        server_reader,
        server_writer,
        initial=b"",
        secure=False,
        server=None,
        client=None,
        limits=_DEFAULT_LIMITS,
    )
    return client_reader, (server_reader, server_writer)


async def test_a_malformed_request_lingers_with_both_stream_ends(
    stream_pair: _StreamPairFactory, mocker: MockerFixture
) -> None:
    # The early-400 path must hand the lingering close both the reader and the writer, in
    # that order: a dropped or Noned end would strand the in-flight body drain.
    linger = mocker.patch("without_http.server._lingering_close")
    (server_reader, server_writer), (_client_reader, client_writer) = await stream_pair()
    client_writer.write(b"!!! not a valid request line !!!\r\n\r\n")
    await client_writer.drain()

    await _serve_h11_connection(
        echo_app,
        server_reader,
        server_writer,
        initial=b"",
        secure=False,
        server=None,
        client=None,
        limits=_DEFAULT_LIMITS,
    )

    linger.assert_awaited_once_with(server_reader, server_writer)


async def test_an_unread_body_lingers_instead_of_being_kept_alive(
    stream_pair: _StreamPairFactory, mocker: MockerFixture
) -> None:
    # A fully-sent response over a half-read request (their_state still SEND_BODY) is not
    # reusable: the loop must linger to flush the early response rather than start a new cycle,
    # handing the lingering close both the reader and the writer, in that order.
    linger = mocker.patch("without_http.server._lingering_close")
    request = b"POST /x HTTP/1.1\r\nhost: test\r\ncontent-length: 100\r\n\r\n" + b"a" * 10

    _client_reader, (server_reader, server_writer) = await _drive_h11_over_socketpair(unread_app, request, stream_pair)

    linger.assert_awaited_once_with(server_reader, server_writer)


async def _ws_first_text(host: str, port: int, path: str) -> tuple[str | None, tuple[str, int]]:
    """Open a WebSocket, return the first text frame the server sends and the client address."""
    reader, writer = await asyncio.open_connection(host, port)
    conn = WSConnection(ConnectionType.CLIENT)
    writer.write(conn.send(WsRequest(host=host, target=path)))
    await writer.drain()
    client_host, client_port = writer.get_extra_info("sockname")
    text: str | None = None
    while text is None:
        data = await asyncio.wait_for(reader.read(65536), timeout=5)
        if not data:  # pragma: no cover - the awaited response arrives before the socket reaches EOF
            break
        conn.receive_data(data)
        for event in conn.events():
            if isinstance(event, TextMessage):
                text = event.data
    writer.close()
    with suppress(OSError):
        await writer.wait_closed()
    return text, (client_host, client_port)


async def ws_scope_echo_app(scope: RawMessage, receive: Receive, send: Send) -> None:
    """Accept a WebSocket, then send its scheme and resolved server/client addresses as text."""
    if scope["type"] != "websocket":
        raise RuntimeError("this app serves only websocket")
    connect = await receive()
    assert connect["type"] == "websocket.connect"
    await send({"type": "websocket.accept"})
    scheme = scope["scheme"]
    server = scope["server"]
    client = scope["client"]
    assert isinstance(server, list)
    assert isinstance(client, list)
    await send({"type": "websocket.send", "text": f"{scheme} {server[0]}:{server[1]} {client[0]}:{client[1]}"})
    await send({"type": "websocket.close"})


async def test_websocket_scope_carries_scheme_server_and_client() -> None:
    async with serving(ws_scope_echo_app) as server:
        text, (client_host, client_port) = await _ws_first_text(server.host, server.port, "/ws")

    assert text == f"ws {server.host}:{server.port} {client_host}:{client_port}"


async def _h2c_scope_roundtrip(host: str, port: int, path: str) -> tuple[int, bytes, tuple[str, int]]:
    """Drive one HTTP/2-over-cleartext request, returning status, body, and the client address."""
    reader, writer = await asyncio.open_connection(host, port)
    conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=True, header_encoding=None))
    conn.initiate_connection()
    stream_id = conn.get_next_available_stream_id()
    conn.send_headers(
        stream_id,
        [
            (b":method", b"GET"),
            (b":path", path.encode()),
            (b":scheme", b"http"),
            (b":authority", f"{host}:{port}".encode()),
        ],
        end_stream=True,
    )
    writer.write(conn.data_to_send())
    await writer.drain()
    client_host, client_port = writer.get_extra_info("sockname")
    status = 0
    chunks: list[bytes] = []
    done = False
    while not done:
        data = await asyncio.wait_for(reader.read(65536), timeout=5)
        if not data:  # pragma: no cover - the awaited response arrives before the socket reaches EOF
            break
        for event in conn.receive_data(data):
            if isinstance(event, h2.events.ResponseReceived) and event.stream_id == stream_id:
                status = int(dict(event.headers)[b":status"])
            elif isinstance(event, h2.events.DataReceived) and event.stream_id == stream_id:
                chunks.append(event.data)
                conn.acknowledge_received_data(event.flow_controlled_length, stream_id)
            elif isinstance(event, h2.events.StreamEnded) and event.stream_id == stream_id:
                done = True
        writer.write(conn.data_to_send())
        await writer.drain()
    writer.close()
    with suppress(OSError):
        await writer.wait_closed()
    return status, b"".join(chunks), (client_host, client_port)


async def test_h2_cleartext_scope_carries_server_and_client() -> None:
    async with serving(scope_echo_app) as server:
        status, body, (client_host, client_port) = await _h2c_scope_roundtrip(server.host, server.port, "/s")

    assert status == 200
    assert body == f"http {server.host}:{server.port} {client_host}:{client_port}".encode()


async def test_h2_over_tls_carries_server_and_client(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with serving(scope_echo_app, ssl_context=server_context) as server:
        async with httpx.AsyncClient(
            base_url=f"https://{server.host}:{server.port}", verify=trusting_client_context, http2=True
        ) as client:
            response = await client.get("/s")

    assert response.http_version == "HTTP/2"
    assert response.status_code == 200
    assert response.text.startswith(f"https {server.host}:{server.port} {server.host}:")


async def test_h2_alpn_over_tls_sends_the_server_preface_before_any_client_bytes(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    # With ALPN "h2" negotiated the server takes the h2 branch and writes its SETTINGS preface
    # immediately, before reading anything. Dropping the ALPN check instead routes the connection
    # into the cleartext-preface path, which blocks on a first client read that never arrives, so
    # no server bytes would ever reach a client that is only listening.
    trusting_client_context.set_alpn_protocols(["h2"])
    async with serving(echo_app, ssl_context=server_context) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port, ssl=trusting_client_context)
        ssl_object = writer.get_extra_info("ssl_object")
        assert ssl_object.selected_alpn_protocol() == "h2"
        preface = await asyncio.wait_for(reader.read(65536), timeout=5)
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()

    assert preface != b""


async def _h2_request_over(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, host: str, port: int, path: str
) -> tuple[int, bytes]:
    """Drive one HTTP/2 GET over an already-open (reader, writer), returning status and body."""
    conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=True, header_encoding=None))
    conn.initiate_connection()
    stream_id = conn.get_next_available_stream_id()
    conn.send_headers(
        stream_id,
        [
            (b":method", b"GET"),
            (b":path", path.encode()),
            (b":scheme", b"https"),
            (b":authority", f"{host}:{port}".encode()),
        ],
        end_stream=True,
    )
    writer.write(conn.data_to_send())
    await writer.drain()
    status = 0
    chunks: list[bytes] = []
    done = False
    while not done:
        data = await asyncio.wait_for(reader.read(65536), timeout=5)
        if not data:  # pragma: no cover - the awaited response arrives before the socket reaches EOF
            break
        for event in conn.receive_data(data):
            if isinstance(event, h2.events.ResponseReceived) and event.stream_id == stream_id:
                status = int(dict(event.headers)[b":status"])
            elif isinstance(event, h2.events.DataReceived) and event.stream_id == stream_id:
                chunks.append(event.data)
                conn.acknowledge_received_data(event.flow_controlled_length, stream_id)
            elif isinstance(event, h2.events.StreamEnded) and event.stream_id == stream_id:
                done = True
        writer.write(conn.data_to_send())
        await writer.drain()
    return status, b"".join(chunks)


async def test_h2_prior_knowledge_over_tls_carries_the_https_scheme(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    # Prior-knowledge h2 over TLS (no ALPN offered, so the server detects the preface itself)
    # still reaches the secure connection through the preface branch: the scope must carry the
    # "https" scheme. Dropping `secure` there would mislabel a TLS request's scheme as "http".
    async with serving(scope_echo_app, ssl_context=server_context) as server:
        reader, writer = await asyncio.open_connection(server.host, server.port, ssl=trusting_client_context)
        ssl_object = writer.get_extra_info("ssl_object")
        assert ssl_object.selected_alpn_protocol() is None
        status, body = await _h2_request_over(reader, writer, server.host, server.port, "/s")
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()

    assert status == 200
    assert body.decode().startswith(f"https {server.host}:{server.port} ")
