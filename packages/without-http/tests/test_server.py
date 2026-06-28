from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from without_asgi import ASGIApp
from without_asgi import HttpScope
from without_asgi import RawMessage
from without_asgi import Receive
from without_asgi import Response
from without_asgi import Send
from without_asgi import make_asgi_app
from without_asgi.routing import buffered
from without_http import serving


async def echo_app(scope: RawMessage, receive: Receive, send: Send) -> None:
    """A raw ASGI app that echoes the request line and body. Has no lifespan support."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    body = b""
    more = True
    while more:
        message = await receive()
        if message["type"] == "http.disconnect":
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
    async with serving(app) as (host, port):
        async with httpx.AsyncClient(base_url=f"http://{host}:{port}") as client:
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


async def test_caps_the_number_of_connections_served_at_once() -> None:
    started = 0
    active = 0
    peak = 0
    release = asyncio.Event()

    async def gated_app(scope: RawMessage, receive: Receive, send: Send) -> None:
        nonlocal started, active, peak
        if scope["type"] != "http":
            raise RuntimeError("this app serves only http")
        started += 1
        active += 1
        peak = max(peak, active)
        try:
            await release.wait()
        finally:
            active -= 1
        # `connection: close` frees the connection's slot as soon as it responds,
        # so a waiting connection can then be accepted.
        await send({"type": "http.response.start", "status": 200, "headers": [(b"connection", b"close")]})
        await send({"type": "http.response.body", "body": b"ok"})

    async with serving(gated_app, max_concurrent_connections=2) as (host, port):
        async with httpx.AsyncClient(base_url=f"http://{host}:{port}") as client:
            requests = [asyncio.create_task(client.get("/")) for _ in range(3)]
            async with asyncio.timeout(5):
                while started < 2:
                    await asyncio.sleep(0.001)
            await asyncio.sleep(0.1)  # a third connection, if accepted, would start here

            assert started == 2  # the third was not even accepted, so its app never ran
            assert active == 2

            release.set()
            responses = await asyncio.gather(*requests)

    assert peak == 2
    assert [response.status_code for response in responses] == [200, 200, 200]
