from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from without_asgi import RawScope
from without_asgi import Receive
from without_asgi import Send
from without_asgi import parse_http_scope
from without_http import Session
from without_http import add_headers
from without_http import follow_redirects
from without_http import open_session
from without_http import serving
from without_http.client import _REDIRECT_STATUSES
from without_http.client import _Http11Connection


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
    """Redirect `/start?status=<code>` to `/end`, which answers `200 arrived`."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    head = parse_http_scope(scope)
    await _read_body(receive)
    if head.path == "/start":
        status = int(head.query_string.removeprefix(b"status="))
        await send(
            {
                "type": "http.response.start",
                "status": status,
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
            assert await response.read() == b"GET /items test= body="


async def test_session_posts_a_body() -> None:
    async with serving(echo_app) as server, open_session() as session:
        async with session.request(
            "POST", f"http://{server.host}:{server.port}/submit", content=b"payload"
        ) as response:
            assert await response.read() == b"POST /submit test= body=payload"


async def test_add_headers_middleware_injects_a_header_seen_server_side() -> None:
    async with (
        serving(echo_app) as server,
        open_session(middleware=add_headers((b"x-test", b"injected"))) as session,
    ):
        async with session.request("GET", f"http://{server.host}:{server.port}/items") as response:
            assert await response.read() == b"GET /items test=injected body="


@pytest.mark.parametrize("status", sorted(_REDIRECT_STATUSES))
async def test_follow_redirects_middleware_follows_each_redirect_status(status: int) -> None:
    async with serving(redirect_app) as server, open_session(middleware=follow_redirects()) as session:
        async with session.request("GET", f"http://{server.host}:{server.port}/start?status={status}") as response:
            assert response.status == 200
            assert await response.read() == b"arrived"


async def chain_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """`/hop/N` redirects to `/hop/N-1`; `/hop/0` answers `200 done`."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    head = parse_http_scope(scope)
    await _read_body(receive)
    remaining = int(head.path.removeprefix("/hop/"))
    if remaining == 0:
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
        await send({"type": "http.response.body", "body": b"done"})
        return
    location = f"/hop/{remaining - 1}".encode()
    await send(
        {"type": "http.response.start", "status": 302, "headers": [(b"location", location), (b"content-length", b"0")]}
    )
    await send({"type": "http.response.body", "body": b""})


async def test_follow_redirects_middleware_follows_a_chain_of_hops() -> None:
    async with serving(chain_app) as server, open_session(middleware=follow_redirects(max_hops=5)) as session:
        async with session.request("GET", f"http://{server.host}:{server.port}/hop/3") as response:
            assert response.status == 200
            assert await response.read() == b"done"


async def test_follow_redirects_middleware_stops_at_max_hops() -> None:
    async with serving(chain_app) as server, open_session(middleware=follow_redirects(max_hops=2)) as session:
        async with session.request("GET", f"http://{server.host}:{server.port}/hop/5") as response:
            # The chain is longer than max_hops, so it stops still on a redirect.
            assert response.status == 302
            assert await response.read() == b""


async def sized_echo_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Like `echo_app` but sends a `content-length`, so the response is keep-alive framed."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    head = parse_http_scope(scope)
    body = await _read_body(receive)
    payload = f"{head.method} {head.path} body={body.decode()}".encode()
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"text/plain"), (b"content-length", str(len(payload)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": payload})


def _only_idle(session: Session) -> list[_Http11Connection]:
    return list(next(iter(session._pool._idle_h11.values())))


async def test_keep_alive_reuses_one_connection_for_sequential_requests() -> None:
    async with serving(sized_echo_app) as server, open_session() as session:
        url = f"http://{server.host}:{server.port}/items"
        async with session.request("GET", url) as first:
            assert await first.read() == b"GET /items body="
        kept = _only_idle(session)
        assert len(kept) == 1
        async with session.request("GET", url) as second:
            assert await second.read() == b"GET /items body="
        assert _only_idle(session) == kept


async def test_a_stale_pooled_connection_is_replaced_with_a_fresh_one() -> None:
    async with serving(sized_echo_app) as server, open_session() as session:
        url = f"http://{server.host}:{server.port}/items"
        async with session.request("GET", url) as first:
            assert await first.read() == b"GET /items body="
        (stale,) = _only_idle(session)
        await stale.aclose()  # the server-closed-an-idle-keep-alive case, simulated
        async with session.request("GET", url) as second:
            assert await second.read() == b"GET /items body="
        (fresh,) = _only_idle(session)
        assert fresh is not stale


async def test_cleartext_h2c_uses_http_2_by_prior_knowledge() -> None:
    async with serving(echo_app) as server, open_session(http2_cleartext=True) as session:
        async with session.request("GET", f"http://{server.host}:{server.port}/items") as response:
            assert response.status == 200
            assert await response.read() == b"GET /items test= body="
        assert len(session._pool._h2) == 1
        assert session._pool._idle_h11 == {}


async def _chunks(*parts: bytes) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


async def chunked_response_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """A raw ASGI app that streams its response body across several `more_body` chunks."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await _read_body(receive)
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    for part in (b"one", b"two", b"three"):
        await send({"type": "http.response.body", "body": part, "more_body": True})
    await send({"type": "http.response.body", "body": b"", "more_body": False})


async def test_streams_a_request_body_from_an_async_iterator() -> None:
    async with serving(echo_app) as server, open_session() as session:
        body = _chunks(b"ab", b"cd", b"ef")
        async with session.request("POST", f"http://{server.host}:{server.port}/up", content=body) as response:
            assert await response.read() == b"POST /up test= body=abcdef"


async def test_streams_a_response_body_chunk_by_chunk() -> None:
    async with serving(chunked_response_app) as server, open_session() as session:
        async with session.request("GET", f"http://{server.host}:{server.port}/down") as response:
            received = [chunk async for chunk in response]
    assert b"".join(received) == b"onetwothree"
