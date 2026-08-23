from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import trustme
from without_asgi import RawScope
from without_asgi import Receive
from without_asgi import Send
from without_http import ConnectionPool
from without_http import add_headers
from without_http import request
from without_http import serving
from without_http.client import _open
from without_http.client import _origin

from .helpers import HOST
from .helpers import chunks
from .helpers import tagged_echo_app


@pytest.fixture(scope="session")
def server_context_h11_only(authority: trustme.CA, tmp_path_factory: pytest.TempPathFactory) -> ssl.SSLContext:
    pem: Path = tmp_path_factory.mktemp("tls-h11") / "server.pem"
    authority.issue_cert(HOST).private_key_and_cert_chain_pem.write_to_path(pem)
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(pem)
    context.set_alpn_protocols(["http/1.1"])
    return context


async def test_open_reports_http_1_1_when_tls_alpn_declines_h2(
    server_context_h11_only: ssl.SSLContext, trusting_client_context_factory: Callable[[], ssl.SSLContext]
) -> None:
    async with serving(tagged_echo_app, ssl_context=server_context_h11_only) as server:
        context = trusting_client_context_factory()
        context.set_alpn_protocols(["h2", "http/1.1"])
        _reader, writer, protocol = await _open(HOST, server.port, ssl_context=context)
        try:
            assert protocol == "http/1.1"  # the server offers only http/1.1, so h2 is not negotiated
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()


async def test_an_https_request_with_http2_disabled_uses_http_1_1(
    server_context: ssl.SSLContext, trusting_client_context_factory: Callable[[], ssl.SSLContext]
) -> None:
    async with serving(tagged_echo_app, ssl_context=server_context) as server:
        async with ConnectionPool(allow_http2=False, ssl_context_factory=trusting_client_context_factory) as pool:
            async with request(pool, "GET", f"https://{HOST}:{server.port}/items") as (head, body):
                assert head.status == 200
                assert await body.read() == b"GET /items test= body="
            assert pool._h2 == {}


async def test_alpn_fallback_to_http_1_1_pools_and_reuses_an_h11_connection(
    server_context_h11_only: ssl.SSLContext, trusting_client_context_factory: Callable[[], ssl.SSLContext]
) -> None:
    async with serving(tagged_echo_app, ssl_context=server_context_h11_only) as server:
        async with ConnectionPool(ssl_context_factory=trusting_client_context_factory) as pool:
            url = f"https://{HOST}:{server.port}/items"
            async with request(pool, "GET", url) as (_head, body):
                assert await body.read() == b"GET /items test= body="
            origin = _origin(urlsplit(url))
            assert origin in pool._h11_only
            assert pool._h2 == {}
            async with request(pool, "GET", url) as (_head, body):  # pragma: no branch
                assert await body.read() == b"GET /items test= body="


async def test_an_unusable_pooled_h2_connection_is_replaced(
    server_context: ssl.SSLContext, trusting_client_context_factory: Callable[[], ssl.SSLContext]
) -> None:
    async with serving(tagged_echo_app, ssl_context=server_context) as server:
        async with ConnectionPool(ssl_context_factory=trusting_client_context_factory) as pool:
            url = f"https://{HOST}:{server.port}/items"
            async with request(pool, "GET", url) as (_head, body):
                await body.read()
            origin = _origin(urlsplit(url))
            stale = pool._h2[origin]
            await stale.aclose()  # the connection died while pooled
            async with request(pool, "GET", url) as (_head, body):
                assert await body.read() == b"GET /items test= body="
            assert pool._h2[origin] is not stale


async def test_client_round_trips_a_get_over_h2(
    server_context: ssl.SSLContext, trusting_client_context_factory: Callable[[], ssl.SSLContext]
) -> None:
    async with serving(tagged_echo_app, ssl_context=server_context) as server:
        async with ConnectionPool(ssl_context_factory=trusting_client_context_factory) as pool:
            async with request(pool, "GET", f"https://{HOST}:{server.port}/items") as (head, body):
                assert head.status == 200
                assert await body.read() == b"GET /items test= body="
            assert len(pool._h2) == 1


async def test_client_posts_a_body_over_h2(
    server_context: ssl.SSLContext, trusting_client_context_factory: Callable[[], ssl.SSLContext]
) -> None:
    async with serving(tagged_echo_app, ssl_context=server_context) as server:
        async with ConnectionPool(ssl_context_factory=trusting_client_context_factory) as pool:
            url = f"https://{HOST}:{server.port}/submit"
            async with request(pool, "POST", url, body=b"payload") as (_head, body):  # pragma: no branch
                assert await body.read() == b"POST /submit test= body=payload"


async def test_client_multiplexes_concurrent_requests_over_one_h2_connection(
    server_context: ssl.SSLContext, trusting_client_context_factory: Callable[[], ssl.SSLContext]
) -> None:
    async def fetch(pool: ConnectionPool, port: int, index: int) -> bytes:
        async with request(pool, "GET", f"https://{HOST}:{port}/n{index}") as (_head, body):
            return await body.read()

    async with serving(tagged_echo_app, ssl_context=server_context) as server:
        async with ConnectionPool(ssl_context_factory=trusting_client_context_factory) as pool:
            bodies = await asyncio.gather(*(fetch(pool, server.port, index) for index in range(8)))
            assert len(pool._h2) == 1

    assert bodies == [f"GET /n{index} test= body=".encode() for index in range(8)]


async def test_client_streams_a_request_body_over_h2(
    server_context: ssl.SSLContext, trusting_client_context_factory: Callable[[], ssl.SSLContext]
) -> None:
    async with serving(tagged_echo_app, ssl_context=server_context) as server:
        async with ConnectionPool(ssl_context_factory=trusting_client_context_factory) as pool:
            upload = chunks(b"ab", b"cd", b"ef")
            async with request(pool, "POST", f"https://{HOST}:{server.port}/up", body=upload) as (
                _head,
                body,
            ):  # pragma: no branch
                assert await body.read() == b"POST /up test= body=abcdef"


async def test_client_round_trips_a_body_larger_than_the_flow_control_window(
    server_context: ssl.SSLContext, trusting_client_context_factory: Callable[[], ssl.SSLContext]
) -> None:
    payload = b"z" * 200_000
    async with serving(tagged_echo_app, ssl_context=server_context) as server:
        async with ConnectionPool(ssl_context_factory=trusting_client_context_factory) as pool:
            async with request(pool, "POST", f"https://{HOST}:{server.port}/big", body=payload) as (
                _head,
                body,
            ):  # pragma: no branch
                assert await body.read() == b"POST /big test= body=" + payload


async def test_client_add_headers_middleware_reaches_the_server_over_h2(
    server_context: ssl.SSLContext, trusting_client_context_factory: Callable[[], ssl.SSLContext]
) -> None:
    async with (
        serving(tagged_echo_app, ssl_context=server_context) as server,
        ConnectionPool(ssl_context_factory=trusting_client_context_factory) as pool,
        request(add_headers((b"x-test", b"injected"))(pool), "GET", f"https://{HOST}:{server.port}/items") as (
            _head,
            body,
        ),
    ):
        assert await body.read() == b"GET /items test=injected body="


async def test_cleartext_stays_http_1_1_even_with_http2_enabled() -> None:
    async with serving(tagged_echo_app) as server, ConnectionPool(allow_http2=True) as pool:
        async with request(pool, "GET", f"http://{HOST}:{server.port}/items") as (_head, body):
            assert await body.read() == b"GET /items test= body="
        assert pool._h2 == {}


async def bidi_echo_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Uppercase each request-body chunk into a response chunk, interleaved (full duplex)."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await send(
        {"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"application/octet-stream")]}
    )
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":  # pragma: no cover - the client here never disconnects abruptly
            break
        chunk = message.get("body", b"")
        assert isinstance(chunk, bytes)
        if chunk:
            await send({"type": "http.response.body", "body": chunk.upper(), "more_body": True})
        if not message.get("more_body", False):
            break
    await send({"type": "http.response.body", "body": b"", "more_body": False})


async def test_h2_bidirectional_ping_pong_streams_both_ways() -> None:
    async with serving(bidi_echo_app) as server, ConnectionPool(force_http2_cleartext=True) as pool:
        outbound: asyncio.Queue[bytes | None] = asyncio.Queue()

        async def request_body() -> AsyncIterator[bytes]:
            while (item := await outbound.get()) is not None:
                yield item

        await outbound.put(b"one")  # client speaks first, then feeds more per response chunk
        url = f"http://{HOST}:{server.port}/bidi"
        received: list[bytes] = []
        async with request(pool, "POST", url, body=request_body()) as (head, body):
            assert head.status == 200
            async for chunk in body:
                if not chunk:  # skip the transport's trailing empty end-of-stream frame
                    continue
                received.append(chunk)
                await outbound.put(f"msg{len(received)}".encode() if len(received) < 3 else None)
    assert received == [b"ONE", b"MSG1", b"MSG2"]


async def test_h2_server_speaks_first_before_the_request_body_is_ready() -> None:
    async with serving(bidi_echo_app) as server, ConnectionPool(force_http2_cleartext=True) as pool:
        head_seen = asyncio.Event()

        async def request_body() -> AsyncIterator[bytes]:
            await head_seen.wait()  # withhold the first chunk until the head has arrived
            yield b"late"

        url = f"http://{HOST}:{server.port}/bidi"
        async with request(pool, "POST", url, body=request_body()) as (head, body):
            assert head.status == 200  # the head arrives though no body chunk has been produced yet
            head_seen.set()
            received = [chunk async for chunk in body if chunk]  # skip the trailing empty end-of-stream frame
        assert received == [b"LATE"]


@pytest.mark.no_mutation  # teardown-timing assertions below are perturbed by mutmut's trampoline; see pyproject
async def test_h2_abandoning_a_bidi_body_cancels_the_parked_sender() -> None:
    async with serving(bidi_echo_app) as server, ConnectionPool(force_http2_cleartext=True) as pool:
        outbound: asyncio.Queue[bytes | None] = asyncio.Queue()

        async def request_body() -> AsyncIterator[bytes]:
            while (
                item := await outbound.get()
            ) is not None:  # pragma: no branch - cancelled while parked, never ends via None
                yield item

        await outbound.put(b"hello")
        url = f"http://{HOST}:{server.port}/bidi"
        async with request(pool, "POST", url, body=request_body()) as (head, body):
            assert head.status == 200
            first = await anext(aiter(body))
            assert first == b"HELLO"
            conn = pool._h2[_origin(urlsplit(url))]
            (stream,) = conn._streams.values()
            send_task = stream.send_task
            assert send_task is not None  # parked on outbound.get()
            assert not send_task.done()
        # Exiting the request context cancels the parked sender. Await it to synchronize on the
        # cancellation landing rather than racing the event loop, then confirm it was cancelled
        # (not leaked still-parked, and not swallowed into a normal return).
        with pytest.raises(asyncio.CancelledError):
            await send_task
        assert conn._streams == {}  # and the stream was reset


async def test_h2_a_request_body_error_after_the_head_resets_the_stream() -> None:
    release = asyncio.Event()

    async def request_body() -> AsyncIterator[bytes]:
        yield b"one"
        await release.wait()  # hold until the caller has the head, then fail
        raise ValueError("h2 body blew up")

    async def exchange(pool: ConnectionPool, url: str) -> None:
        async with request(pool, "POST", url, body=request_body()) as (head, body):
            assert head.status == 200  # the head has arrived before the body fails
            release.set()
            await body.read()

    async with serving(bidi_echo_app) as server, ConnectionPool(force_http2_cleartext=True) as pool:
        url = f"http://{server.host}:{server.port}/bidi"
        with pytest.raises(ValueError, match="h2 body blew up"):
            await exchange(pool, url)
        assert pool._h2[_origin(urlsplit(url))]._streams == {}  # the failed stream was reset, not stranded


def test_pool_holds_one_produced_context_per_alpn_offer() -> None:
    # The pool owns (and may freely mutate) the contexts it opens with, producing one per
    # distinct ALPN offer from the factory rather than mutating a single caller-shared context.
    produced = 0

    def factory() -> ssl.SSLContext:
        nonlocal produced
        produced += 1
        return ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    pool = ConnectionPool(ssl_context_factory=factory)
    h2 = pool._context_for_connection(http2=True)
    h11 = pool._context_for_connection(http2=False)

    assert pool._context_for_connection(http2=True) is h2  # cached per offer, not rebuilt
    assert h2 is not h11  # a distinct context backs the http/1.1-only offer
    assert produced == 2  # produced once per distinct offer, never per connection
