from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import asynccontextmanager
from contextlib import suppress
from datetime import timedelta
from typing import cast

import httpx
import pytest
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
from without_http.server import _Limits
from without_http.server import _lingering_close
from without_http.server import _LiveConnections
from without_http.server import _serve_connection
from without_http.server import _serve_h11_connection

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

    assert (await client_reader.read()).startswith(b"HTTP/1.1 400")


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
