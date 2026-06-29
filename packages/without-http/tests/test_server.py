from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextlib import suppress

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
from without_http.server import _LiveConnections


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
