from __future__ import annotations

from without_asgi import RawScope
from without_asgi import Receive
from without_asgi import Send
from without_asgi import parse_http_scope
from without_http import Session
from without_http import default_headers
from without_http import follow_redirects
from without_http import open_session
from without_http import serving


async def _read_body(receive: Receive) -> bytes:
    body = b""
    more = True
    while more:
        message = await receive()
        if message["type"] == "http.disconnect":
            break
        chunk = message.get("body", b"")
        assert isinstance(chunk, bytes)
        body += chunk
        more = bool(message.get("more_body", False))
    return body


async def echo_app(scope: RawScope, receive: Receive, send: Send) -> None:
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    head = parse_http_scope(scope)
    body = await _read_body(receive)
    marker = next((value for name, value in head.headers if name == b"x-test"), b"")
    payload = f"{head.method} {head.path} test={marker.decode()} body={body.decode()}".encode()
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": payload})


async def redirect_app(scope: RawScope, receive: Receive, send: Send) -> None:
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    head = parse_http_scope(scope)
    await _read_body(receive)
    if head.path == "/start":
        await send(
            {
                "type": "http.response.start",
                "status": 302,
                "headers": [(b"location", b"/end"), (b"content-length", b"0")],
            }
        )
        await send({"type": "http.response.body", "body": b""})
    else:
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
        await send({"type": "http.response.body", "body": b"arrived"})


async def test_session_gets_a_response() -> None:
    async with serving(echo_app) as server, open_session() as session:
        async with session.request("GET", f"http://{server.host}:{server.port}/items") as response:
            assert response.status == 200
            assert response.body == b"GET /items test= body="


async def test_session_posts_a_body() -> None:
    async with serving(echo_app) as server, open_session() as session:
        async with session.request("POST", f"http://{server.host}:{server.port}/submit", body=b"payload") as response:
            assert response.body == b"POST /submit test= body=payload"


async def test_default_headers_middleware_injects_a_header_seen_server_side() -> None:
    session = Session(middleware=default_headers((b"x-test", b"injected")))
    async with serving(echo_app) as server:
        async with session.request("GET", f"http://{server.host}:{server.port}/items") as response:
            assert response.body == b"GET /items test=injected body="


async def test_follow_redirects_middleware_follows_a_302() -> None:
    session = Session(middleware=follow_redirects())
    async with serving(redirect_app) as server:
        async with session.request("GET", f"http://{server.host}:{server.port}/start") as response:
            assert response.status == 200
            assert response.body == b"arrived"
