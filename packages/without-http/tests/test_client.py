from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import replace
from urllib.parse import urlsplit

import h11
import pytest
from without_asgi import RawScope
from without_asgi import Receive
from without_asgi import Send
from without_asgi import parse_http_scope
from without_http import ClientRequest
from without_http import ClientResponse
from without_http import ConnectionPool
from without_http import CookieJar
from without_http import ResponseBody
from without_http import ResponseHead
from without_http import ResponseTrailers
from without_http import add_headers
from without_http import cookies
from without_http import follow_redirects
from without_http import serving
from without_http import wrap
from without_http.client import _REDIRECT_STATUSES
from without_http.client import _build_request
from without_http.client import _Http11Connection
from without_http.client import _origin

type _Endpoint = tuple[asyncio.StreamReader, asyncio.StreamWriter]
type _StreamPairFactory = Callable[[], Awaitable[tuple[_Endpoint, _Endpoint]]]


async def _read_body(receive: Receive) -> bytes:
    body = b""
    more = True
    while more:
        message = await receive()
        if message["type"] == "http.disconnect":  # pragma: no cover - clients here never disconnect mid-body
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


async def test_pool_gets_a_response() -> None:
    async with serving(echo_app) as server, ConnectionPool() as pool:
        async with pool.request("GET", f"http://{server.host}:{server.port}/items") as (head, body):
            assert head.status == 200
            assert await body.read() == b"GET /items test= body="


async def test_pool_posts_a_body() -> None:
    async with serving(echo_app) as server, ConnectionPool() as pool:
        url = f"http://{server.host}:{server.port}/submit"
        async with pool.request("POST", url, body=b"payload") as (_head, body):
            assert await body.read() == b"POST /submit test= body=payload"


async def test_wrap_request_side_rewrites_the_outgoing_request() -> None:
    inject = wrap(request=lambda request: replace(request, headers=(*request.headers, (b"x-test", b"viawrap"))))
    async with serving(echo_app) as server, ConnectionPool(middleware=inject) as pool:
        async with pool.request("GET", f"http://{server.host}:{server.port}/items") as (_head, body):
            assert await body.read() == b"GET /items test=viawrap body="


async def test_wrap_response_side_transforms_the_returned_body() -> None:
    def shout(response: ClientResponse) -> ClientResponse:
        async def upper(
            events: AsyncIterator[bytes | ResponseTrailers],
        ) -> AsyncGenerator[bytes | ResponseTrailers]:
            async for item in events:
                yield item.upper() if isinstance(item, bytes) else item

        return ClientResponse(response.head, ResponseBody(upper(response.body.events())))

    async with serving(echo_app) as server, ConnectionPool(middleware=wrap(response=shout)) as pool:
        async with pool.request("GET", f"http://{server.host}:{server.port}/items") as (_head, body):
            assert await body.read() == b"GET /ITEMS TEST= BODY="


async def test_add_headers_middleware_injects_a_header_seen_server_side() -> None:
    async with (
        serving(echo_app) as server,
        ConnectionPool(middleware=add_headers((b"x-test", b"injected"))) as pool,
    ):
        async with pool.request("GET", f"http://{server.host}:{server.port}/items") as (_head, body):
            assert await body.read() == b"GET /items test=injected body="


@pytest.mark.parametrize("status", sorted(_REDIRECT_STATUSES))
async def test_follow_redirects_middleware_follows_each_redirect_status(status: int) -> None:
    async with serving(redirect_app) as server, ConnectionPool(middleware=follow_redirects()) as pool:
        async with pool.request("GET", f"http://{server.host}:{server.port}/start?status={status}") as (head, body):
            assert head.status == 200
            assert await body.read() == b"arrived"


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
    async with serving(chain_app) as server, ConnectionPool(middleware=follow_redirects(max_hops=5)) as pool:
        async with pool.request("GET", f"http://{server.host}:{server.port}/hop/3") as (head, body):
            assert head.status == 200
            assert await body.read() == b"done"


async def test_follow_redirects_middleware_stops_at_max_hops() -> None:
    async with serving(chain_app) as server, ConnectionPool(middleware=follow_redirects(max_hops=2)) as pool:
        async with pool.request("GET", f"http://{server.host}:{server.port}/hop/5") as (head, body):
            # The chain is longer than max_hops, so it stops still on a redirect.
            assert head.status == 302
            assert await body.read() == b""


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


def _only_idle(pool: ConnectionPool) -> list[_Http11Connection]:
    return list(next(iter(pool._h11.values())).idle)


async def test_keep_alive_reuses_one_connection_for_sequential_requests() -> None:
    async with serving(sized_echo_app) as server, ConnectionPool() as pool:
        url = f"http://{server.host}:{server.port}/items"
        async with pool.request("GET", url) as (_head, body):
            assert await body.read() == b"GET /items body="
        kept = _only_idle(pool)
        assert len(kept) == 1
        async with pool.request("GET", url) as (_head, body):
            assert await body.read() == b"GET /items body="
        assert _only_idle(pool) == kept


def _idle_count(pool: ConnectionPool) -> int:
    return sum(len(host_pool.idle) for host_pool in pool._h11.values())


async def early_reject_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Answer `413` immediately, without ever draining the request body."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await send({"type": "http.response.start", "status": 413, "headers": [(b"content-length", b"0")]})
    await send({"type": "http.response.body", "body": b""})


async def _large_upload() -> AsyncIterator[bytes]:
    # To block, the upload must outrun the sender's send buffer *plus* the receiver's
    # receive buffer, since a peer that never reads still absorbs both. Linux autotunes
    # each up to `net.ipv4.tcp_wmem`/`tcp_rmem` (~10 MB combined on default kernels, and
    # more on a tuned host), so a cap sized against one buffer is a race the kernel wins:
    # the whole body lands in the buffers, nothing blocks, and the test hangs.
    #
    # The cap is generous rather than tuned because it costs nothing to raise: the
    # generator is lazy, so a consumer that blocks (or is cancelled by an early response)
    # only ever pays for the chunks it actually wrote.
    for _ in range(512):  # pragma: no branch - the early response cancels this before the loop finishes
        yield b"x" * 100_000


async def test_early_response_to_a_large_upload_does_not_deadlock() -> None:
    async def post_status(pool: ConnectionPool, url: str) -> int:
        async with pool.request("POST", url, body=_large_upload()) as (head, body):
            await body.read()
            return head.status

    async with serving(early_reject_app) as server, ConnectionPool(max_connections_per_host=1) as pool:
        url = f"http://{server.host}:{server.port}/upload"
        # Without concurrent send/read this hangs: the body write blocks on backpressure
        # forever and the early 413 is never read. wait_for bounds the regression.
        assert await asyncio.wait_for(post_status(pool, url), timeout=10) == 413
        assert _idle_count(pool) == 0  # the unfinished upload is never a reusable connection
        # A second request proves the single permit was released, not stranded.
        assert await asyncio.wait_for(post_status(pool, url), timeout=10) == 413


async def test_send_body_stops_at_a_chunk_boundary_when_the_peer_half_closes(
    stream_pair: _StreamPairFactory,
) -> None:
    (client_reader, client_writer), (peer_reader, peer_writer) = await stream_pair()
    connection = _Http11Connection.new(client_reader, client_writer)
    url = "http://upstream/upload"

    async def body() -> AsyncIterator[bytes]:
        yield b"first-chunk"
        peer_writer.write_eof()  # the peer half-closes after the first chunk
        assert await client_reader.read() == b""  # ...and the client observes the FIN
        yield b"second-chunk"  # pulled, but the send stops before writing it

    request = _build_request("POST", url, (), body())
    await connection.send_head(request, urlsplit(url))
    await connection.send_body(request)
    client_writer.write_eof()
    sent = await peer_reader.read()

    assert b"first-chunk" in sent  # the pre-close chunk went out
    assert b"second-chunk" not in sent  # the post-close chunk was pulled but never written
    assert connection._conn.our_state is h11.SEND_BODY  # the body was never framed to its end


async def test_send_body_skips_the_end_of_message_when_the_peer_half_closes(
    stream_pair: _StreamPairFactory,
) -> None:
    (client_reader, client_writer), (peer_reader, peer_writer) = await stream_pair()
    connection = _Http11Connection.new(client_reader, client_writer)
    url = "http://upstream/upload"

    async def body() -> AsyncIterator[bytes]:
        yield b"only-chunk"
        peer_writer.write_eof()  # the peer half-closes just as the body ends
        assert await client_reader.read() == b""

    request = _build_request("POST", url, (), body())
    await connection.send_head(request, urlsplit(url))
    await connection.send_body(request)
    client_writer.write_eof()
    sent = await peer_reader.read()

    # The body drains fully, but the trailing EndOfMessage (a chunked `0\r\n\r\n`
    # terminator) is suppressed by the half-close, leaving the request unfinished.
    assert b"only-chunk" in sent
    assert b"0\r\n\r\n" not in sent
    assert connection._conn.our_state is h11.SEND_BODY


async def _raising_body() -> AsyncIterator[bytes]:
    yield b"first"
    raise ValueError("body generator blew up")


async def test_a_request_body_generator_error_surfaces_to_the_caller() -> None:
    async with serving(echo_app) as server, ConnectionPool(max_connections_per_host=1) as pool:
        url = f"http://{server.host}:{server.port}/up"
        with pytest.raises(ValueError, match="body generator blew up"):
            # The echo server waits for the whole body, so the head never arrives: the error
            # surfaces as the request is made, before the response body is ever read.
            async with pool.request("POST", url, body=_raising_body()) as (_head, body):  # pragma: no branch
                await body.read()  # pragma: no cover
        assert _idle_count(pool) == 0  # the broken exchange leaves nothing pooled
        # The permit was freed despite the error, so the origin is not starved.
        async with pool.request("GET", url) as (head, _body):
            assert head.status == 200


async def test_max_connections_per_host_serializes_concurrent_requests() -> None:
    async with serving(sized_echo_app) as server, ConnectionPool(max_connections_per_host=1) as pool:
        url = f"http://{server.host}:{server.port}/items"
        async with pool.request("GET", url) as (head, first):
            assert head.status == 200
            # `first` holds the origin's only permit until its body is read, so a second
            # request to the same origin must wait rather than open a second connection.
            second = asyncio.create_task(_read_one(pool, url))
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(second), timeout=0.2)
            assert await first.read() == b"GET /items body="  # releases the permit
        assert await second == b"GET /items body="


async def _read_one(pool: ConnectionPool, url: str) -> bytes:
    async with pool.request("GET", url) as (_head, body):
        return await body.read()


async def test_max_keepalive_per_host_caps_retained_idle_connections() -> None:
    async with serving(sized_echo_app) as server, ConnectionPool(max_keepalive_per_host=1) as pool:
        url = f"http://{server.host}:{server.port}/items"
        # Two overlapping requests force two live connections (no peak bound is set), so the
        # pool ramps up under load past the idle cap.
        async with (
            pool.request("GET", url) as (_first_head, first),
            pool.request("GET", url) as (_second_head, second),
        ):
            assert await first.read() == b"GET /items body="
            assert await second.read() == b"GET /items body="
        # Both are reusable on return, but the idle list settles back to the cap: the first
        # returned is retained and the second is closed rather than pooled.
        assert _idle_count(pool) == 1


@pytest.mark.parametrize(
    ("knob", "make_pool"),
    [
        ("max_connections_per_host", lambda value: ConnectionPool(max_connections_per_host=value)),
        ("max_keepalive_per_host", lambda value: ConnectionPool(max_keepalive_per_host=value)),
    ],
)
@pytest.mark.parametrize("value", [0, -1])
def test_non_positive_per_host_bounds_are_rejected(
    knob: str, make_pool: Callable[[int], ConnectionPool], value: int
) -> None:
    with pytest.raises(ValueError, match=f"{knob} must be >= 1 when set, got {value}"):
        make_pool(value)


async def test_a_stale_pooled_connection_is_replaced_with_a_fresh_one() -> None:
    async with serving(sized_echo_app) as server, ConnectionPool() as pool:
        url = f"http://{server.host}:{server.port}/items"
        async with pool.request("GET", url) as (_head, body):
            assert await body.read() == b"GET /items body="
        (stale,) = _only_idle(pool)
        await stale.aclose()  # the server-closed-an-idle-keep-alive case, simulated
        async with pool.request("GET", url) as (_head, body):
            assert await body.read() == b"GET /items body="
        (fresh,) = _only_idle(pool)
        assert fresh is not stale


async def test_cleartext_h2c_uses_http_2_by_prior_knowledge() -> None:
    async with serving(echo_app) as server, ConnectionPool(force_http2_cleartext=True) as pool:
        async with pool.request("GET", f"http://{server.host}:{server.port}/items") as (head, body):
            assert head.status == 200
            assert await body.read() == b"GET /items test= body="
        assert len(pool._h2) == 1
        assert pool._h11 == {}


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
    async with serving(echo_app) as server, ConnectionPool() as pool:
        upload = _chunks(b"ab", b"cd", b"ef")
        async with pool.request("POST", f"http://{server.host}:{server.port}/up", body=upload) as (_head, body):
            assert await body.read() == b"POST /up test= body=abcdef"


async def test_streams_a_response_body_chunk_by_chunk() -> None:
    async with serving(chunked_response_app) as server, ConnectionPool() as pool:
        async with pool.request("GET", f"http://{server.host}:{server.port}/down") as (_head, body):
            received = [chunk async for chunk in body]
    assert b"".join(received) == b"onetwothree"


async def cookie_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """
    `/set` issues a cookie, `/clear` deletes it (`Max-Age=0`), `/echo` returns the
    `Cookie` header the request carried.
    """
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    head = parse_http_scope(scope)
    await _read_body(receive)
    set_cookie = {
        "/set": (b"set-cookie", b"sid=xyz789; Path=/"),
        "/clear": (b"set-cookie", b"sid=; Path=/; Max-Age=0"),
    }.get(head.path)
    if set_cookie is not None:
        await send({"type": "http.response.start", "status": 200, "headers": [set_cookie, (b"content-length", b"0")]})
        await send({"type": "http.response.body", "body": b""})
        return
    received = next((value for name, value in head.headers if name == b"cookie"), b"")
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": b"cookie=" + received})


async def test_cookie_jar_carries_a_set_cookie_to_the_next_request() -> None:
    jar = CookieJar()
    async with serving(cookie_app) as server, ConnectionPool() as pool:
        base = f"http://{server.host}:{server.port}"
        async with pool.request("GET", f"{base}/set", middleware=cookies(jar)) as (head, _body):
            assert head.status == 200
        async with pool.request("GET", f"{base}/echo", middleware=cookies(jar)) as (_head, body):
            assert await body.read() == b"cookie=sid=xyz789"


async def test_cookie_jar_drops_a_cookie_deleted_with_max_age_zero() -> None:
    jar = CookieJar()
    async with serving(cookie_app) as server, ConnectionPool(middleware=cookies(jar)) as pool:
        base = f"http://{server.host}:{server.port}"
        async with pool.request("GET", f"{base}/set") as (head, _body):
            assert head.status == 200
        async with pool.request("GET", f"{base}/clear") as (head, _body):
            assert head.status == 200
        async with pool.request("GET", f"{base}/echo") as (_head, body):
            assert await body.read() == b"cookie="


def test_origin_rejects_a_url_without_a_host() -> None:
    with pytest.raises(ValueError, match="must be absolute"):
        _origin(urlsplit("/relative/path"))


def test_build_request_keeps_an_explicit_content_length_for_a_buffered_body() -> None:
    request = _build_request("POST", "http://h/x", ((b"content-length", b"7"),), b"payload")

    assert request.headers == ((b"content-length", b"7"),)


async def test_build_request_keeps_explicit_framing_for_a_streaming_body() -> None:
    stream = _chunks(b"ab", b"cd")
    request = _build_request("POST", "http://h/x", ((b"transfer-encoding", b"chunked"),), stream)

    assert request.headers == ((b"transfer-encoding", b"chunked"),)
    assert [chunk async for chunk in request.body] == [b"ab", b"cd"]


async def _chunks_with_gaps(*parts: bytes) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


async def test_streams_an_h11_body_with_an_explicit_host_and_empty_chunks() -> None:
    async with serving(echo_app) as server, ConnectionPool() as pool:
        upload = _chunks_with_gaps(b"", b"ab", b"", b"cd")
        url = f"http://{server.host}:{server.port}/up"
        async with pool.request("POST", url, headers=((b"host", b"override.test"),), body=upload) as (_head, body):
            assert await body.read() == b"POST /up test= body=abcd"


async def test_reads_a_large_h11_response_body_across_multiple_socket_reads() -> None:
    payload = b"x" * 200_000
    async with serving(echo_app) as server, ConnectionPool() as pool:
        url = f"http://{server.host}:{server.port}/big"
        async with pool.request("POST", url, body=payload) as (_head, body):
            assert await body.read() == b"POST /big test= body=" + payload


async def no_location_redirect_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Answer every request with a `302` that carries no `Location` header."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await _read_body(receive)
    await send({"type": "http.response.start", "status": 302, "headers": [(b"content-length", b"0")]})
    await send({"type": "http.response.body", "body": b""})


async def test_follow_redirects_stops_when_a_redirect_lacks_a_location() -> None:
    async with serving(no_location_redirect_app) as server, ConnectionPool(middleware=follow_redirects()) as pool:
        async with pool.request("GET", f"http://{server.host}:{server.port}/start") as (head, _body):
            assert head.status == 302


async def _closed_body() -> AsyncGenerator[bytes | ResponseTrailers]:
    return
    yield  # pragma: no cover - an empty response body for a scripted redirect hop


def _redirect_to(location: str, status: int = 302) -> ClientResponse:
    head = ResponseHead(status=status, headers=((b"location", location.encode()),))
    return ClientResponse(head=head, body=ResponseBody(_closed_body()))


def _terminal_ok() -> ClientResponse:
    return ClientResponse(head=ResponseHead(status=200, headers=()), body=ResponseBody(_closed_body()))


class _ScriptedExchange:
    """A fake inner exchange that records requests and returns a scripted response list."""

    def __init__(self, *responses: ClientResponse) -> None:
        self.responses = list(responses)
        self.requests: list[ClientRequest] = []

    async def __call__(self, request: ClientRequest) -> ClientResponse:
        self.requests.append(request)
        return self.responses.pop(0)


_CREDENTIALED = (
    (b"authorization", b"Bearer secret-token"),
    (b"cookie", b"sid=session-value"),
    (b"proxy-authorization", b"Basic proxy-creds"),
    (b"accept", b"application/json"),
)


@pytest.mark.security("Authorization/Cookie are stripped when a redirect crosses origins")
async def test_follow_redirects_strips_credentials_on_a_cross_origin_hop() -> None:
    inner = _ScriptedExchange(_redirect_to("https://evil.test/"), _terminal_ok())
    exchange = follow_redirects()(inner)

    request = ClientRequest(method="GET", url="https://api.victim.test/data", headers=_CREDENTIALED)
    await exchange(request)

    followed = inner.requests[1]
    names = {name for name, _ in followed.headers}
    assert names == {b"accept"}


@pytest.mark.security("the cross-origin credential strip is scoped: a same-origin redirect keeps credentials")
async def test_follow_redirects_keeps_credentials_on_a_same_origin_hop() -> None:
    inner = _ScriptedExchange(_redirect_to("https://api.victim.test/next"), _terminal_ok())
    exchange = follow_redirects()(inner)

    request = ClientRequest(method="GET", url="https://api.victim.test/data", headers=_CREDENTIALED)
    await exchange(request)

    followed = inner.requests[1]
    assert followed.headers == _CREDENTIALED


@pytest.mark.security("a 303 redirect drops the original method and body (no cross-method replay)")
async def test_follow_redirects_downgrades_a_303_to_a_bodyless_get() -> None:
    inner = _ScriptedExchange(_redirect_to("https://api.victim.test/done", status=303), _terminal_ok())
    exchange = follow_redirects()(inner)

    request = ClientRequest(
        method="POST",
        url="https://api.victim.test/submit",
        headers=((b"content-type", b"application/json"), (b"content-length", b"9")),
    )
    await exchange(request)

    followed = inner.requests[1]
    assert followed.method == "GET"
    assert {name for name, _ in followed.headers} == set()


async def test_follow_redirects_keeps_a_get_across_a_303() -> None:
    inner = _ScriptedExchange(_redirect_to("https://api.victim.test/done", status=303), _terminal_ok())
    exchange = follow_redirects()(inner)

    request = ClientRequest(method="GET", url="https://api.victim.test/thing")
    response = await exchange(request)

    assert inner.requests[1].method == "GET"
    assert await response.body.read() == b""


@pytest.mark.security("an https->http redirect is refused, so credentials aren't replayed over cleartext")
async def test_follow_redirects_refuses_an_https_to_http_downgrade() -> None:
    inner = _ScriptedExchange(_redirect_to("http://api.victim.test/insecure"))
    exchange = follow_redirects()(inner)

    request = ClientRequest(method="GET", url="https://api.victim.test/data", headers=_CREDENTIALED)
    response = await exchange(request)

    assert response.head.status == 302
    assert len(inner.requests) == 1


async def test_checking_in_to_a_closed_host_pool_closes_the_connection() -> None:
    async with serving(sized_echo_app) as server, ConnectionPool() as pool:
        url = f"http://{server.host}:{server.port}/items"
        async with pool.request("GET", url) as (_head, body):
            assert await body.read() == b"GET /items body="
        host_pool = next(iter(pool._h11.values()))
        (connection,) = host_pool.idle
        host_pool.idle.clear()
        host_pool.closed = True

        await host_pool.checkin(connection, reusable=True)

        assert host_pool.idle == []
        assert connection._writer.is_closing()
