from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from without.testing import yield_once
from without_asgi import RawScope
from without_asgi import Receive
from without_asgi import Send
from without_asgi import parse_http_scope
from without_http import Client
from without_http import ClientRequest
from without_http import ClientResponse
from without_http import ConnectionPool
from without_http import request
from without_http import stack
from without_http.testing import Endpoint
from without_http.testing import asgi_client
from without_http.testing import base_url
from without_http.testing import loopback_client
from without_http.testing import mock_client
from without_http.testing import pipe
from without_http.testing import respond
from without_http.testing import scope_from_client_request

from .test_client import echo_app


async def test_mock_client_answers_from_its_handler_without_a_server() -> None:
    def answer(outgoing: ClientRequest) -> ClientResponse:
        assert outgoing.url == "https://api.test/items"
        return respond(201, headers=((b"x-source", b"mock"),), body=b"mocked")

    async with request(mock_client(answer), "GET", "https://api.test/items") as (head, body):
        assert head.status == 201
        assert head.headers == ((b"x-source", b"mock"),)
        assert await body.read() == b"mocked"


async def test_mock_client_accepts_an_async_handler() -> None:
    async def answer(outgoing: ClientRequest) -> ClientResponse:
        await asyncio.sleep(0)
        return respond(202, body=outgoing.method.encode())

    async with request(mock_client(answer), "DELETE", "https://api.test/thing") as (head, body):
        assert head.status == 202
        assert await body.read() == b"DELETE"


async def test_mock_client_sees_the_body_the_caller_sent() -> None:
    seen: list[bytes] = []

    async def answer(outgoing: ClientRequest) -> ClientResponse:
        seen.append(b"".join([chunk async for chunk in outgoing.body]))
        return respond(200)

    async with request(mock_client(answer), "POST", "https://api.test/x", body=b"payload") as (head, _body):
        assert head.status == 200

    assert seen == [b"payload"]


async def test_respond_defaults_to_an_empty_body() -> None:
    async with request(mock_client(lambda _outgoing: respond(204)), "GET", "https://api.test/x") as (head, body):
        assert head.status == 204
        assert await body.read() == b""


async def test_respond_carries_trailers_to_a_reader_that_asks_for_them() -> None:
    client = mock_client(lambda _outgoing: respond(200, body=b"data", trailers=((b"grpc-status", b"0"),)))

    async with request(client, "GET", "https://api.test/stream") as (_head, body):
        data, trailers = await body.read_with_trailers()

    assert data == b"data"
    assert [block.headers for block in trailers] == [((b"grpc-status", b"0"),)]


async def test_a_failing_mock_handler_surfaces_to_the_caller() -> None:
    def answer(outgoing: ClientRequest) -> ClientResponse:
        raise RuntimeError("no route for this request")

    with pytest.raises(RuntimeError, match="no route"):
        async with request(mock_client(answer), "GET", "https://api.test/missing") as _response:
            pass  # pragma: no cover


async def test_base_url_resolves_a_relative_url_and_leaves_an_absolute_one() -> None:
    seen: list[str] = []

    def answer(outgoing: ClientRequest) -> ClientResponse:
        seen.append(outgoing.url)
        return respond(200)

    client = base_url("http://testserver/")(mock_client(answer))
    async with request(client, "GET", "/items") as _first:
        pass
    async with request(client, "GET", "https://elsewhere.test/other") as _second:
        pass

    assert seen == ["http://testserver/items", "https://elsewhere.test/other"]


async def test_asgi_client_drives_an_app_with_no_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    async def refuse(*args: object, **kwargs: object) -> None:
        raise AssertionError("the in-memory client must not open a connection")  # pragma: no cover

    monkeypatch.setattr(asyncio, "open_connection", refuse)
    async with asgi_client(echo_app) as client:
        async with request(client, "GET", "http://testserver/items") as (head, body):
            assert head.status == 200
            assert await body.read() == b"GET /items test= body="


async def test_asgi_client_sends_a_request_body() -> None:
    async with asgi_client(echo_app) as client:
        async with request(client, "POST", "http://testserver/submit", body=b"payload") as (_head, body):
            assert await body.read() == b"POST /submit test= body=payload"


async def test_asgi_client_composes_with_client_middleware() -> None:
    async with asgi_client(echo_app) as pool_free:
        client = stack(base_url("http://testserver"))(pool_free)
        async with request(client, "GET", "/items") as (_head, body):
            assert await body.read() == b"GET /items test= body="


async def streaming_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Answer immediately, then send one body chunk per request-body chunk read."""
    head = parse_http_scope(scope)
    assert head.path == "/duplex"
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    while True:
        message = await receive()
        chunk = message.get("body", b"")
        assert isinstance(chunk, bytes)
        if not message.get("more_body", False):
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            return
        await send({"type": "http.response.body", "body": chunk.upper(), "more_body": True})


async def test_asgi_client_streams_the_response_while_the_request_body_is_still_going() -> None:
    outbound: asyncio.Queue[bytes | None] = asyncio.Queue()

    async def upload() -> AsyncIterator[bytes]:
        while (chunk := await outbound.get()) is not None:
            yield chunk

    await outbound.put(b"one")
    async with asgi_client(streaming_app) as client:
        async with request(client, "POST", "http://testserver/duplex", body=upload()) as (head, body):
            # The head arrives before any body chunk exists, which is what makes this a
            # stream rather than a buffered round trip.
            assert head.status == 200
            chunks = aiter(body)
            assert await anext(chunks) == b"ONE"
            await outbound.put(b"two")
            assert await anext(chunks) == b"TWO"
            await outbound.put(None)
            assert [chunk async for chunk in chunks] == []


async def test_asgi_client_runs_the_lifespan_around_the_block() -> None:
    events: list[str] = []

    async def app(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            while True:
                message = await receive()
                if message["type"] == "lifespan.startup":
                    events.append("startup")
                    await send({"type": "lifespan.startup.complete"})
                else:
                    events.append("shutdown")
                    await send({"type": "lifespan.shutdown.complete"})
                    return
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": ",".join(events).encode()})

    async with asgi_client(app) as client:
        async with request(client, "GET", "http://testserver/") as (_head, body):
            assert await body.read() == b"startup"

    assert events == ["startup", "shutdown"]


async def test_an_app_that_reads_past_its_request_body_sees_a_disconnect() -> None:
    seen: list[str] = []

    async def nosy(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            raise RuntimeError("this app has no lifespan")
        while True:
            message = await receive()
            seen.append(str(message["type"]))
            if message["type"] == "http.disconnect":
                break
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    async with asgi_client(nosy) as client:
        async with request(client, "POST", "http://testserver/", body=b"payload") as (head, _body):
            assert head.status == 200

    assert seen == ["http.request", "http.request", "http.disconnect"]


async def test_an_app_can_send_trailers_and_an_early_hint() -> None:
    async def hinting(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            raise RuntimeError("this app has no lifespan")
        # A client discards a 103 the way the h11 one does, so it must not disturb the
        # response that follows it.
        await send({"type": "http.response.early_hint", "links": [b"</style.css>; rel=preload"]})
        await send({"type": "http.response.start", "status": 200, "headers": [], "trailers": True})
        await send({"type": "http.response.body", "body": b"body", "more_body": True})
        await send({"type": "http.response.trailers", "headers": [(b"grpc-status", b"0")], "more_trailers": False})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    async with asgi_client(hinting) as client:
        async with request(client, "GET", "http://testserver/") as (head, body):
            assert head.status == 200
            data, trailers = await body.read_with_trailers()

    assert data == b"body"
    assert [block.headers for block in trailers] == [((b"grpc-status", b"0"),)]


async def test_an_app_that_sends_an_extension_event_this_transport_never_offered_fails() -> None:
    async def pushing(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            raise RuntimeError("this app has no lifespan")
        await send({"type": "http.response.push", "path": "/style.css", "headers": []})

    async with asgi_client(pushing) as client:
        with pytest.raises(NotImplementedError, match="ServerPush"):
            async with request(client, "GET", "http://testserver/") as _response:
                pass  # pragma: no cover


async def test_an_app_that_stops_mid_body_without_failing_is_a_loud_failure() -> None:
    async def truncating(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            raise RuntimeError("this app has no lifespan")
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"half", "more_body": True})

    async def read_it(client: Client) -> bytes:
        async with request(client, "GET", "http://testserver/") as (_head, body):
            return await body.read()

    async with asgi_client(truncating) as client:
        with pytest.raises(RuntimeError, match="ended before finishing its response body"):
            await read_it(client)


async def test_an_app_that_crashes_before_responding_surfaces_to_the_caller() -> None:
    async def crashing(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            raise RuntimeError("this app has no lifespan")  # the standard "unsupported" signal
        raise ValueError("handler exploded")

    async with asgi_client(crashing) as client:
        with pytest.raises(ValueError, match="handler exploded"):
            async with request(client, "GET", "http://testserver/") as _response:
                pass  # pragma: no cover


async def test_an_app_that_crashes_mid_body_surfaces_at_the_read() -> None:
    async def half_written(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            raise RuntimeError("this app has no lifespan")
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"partial", "more_body": True})
        raise ValueError("exploded mid-body")

    async def read_it(client: Client) -> bytes:
        async with request(client, "GET", "http://testserver/") as (head, body):
            assert head.status == 200
            return await body.read()

    async with asgi_client(half_written) as client:
        with pytest.raises(ValueError, match="mid-body"):
            await read_it(client)


async def test_an_app_that_returns_without_responding_is_a_loud_failure() -> None:
    async def silent(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            raise RuntimeError("this app has no lifespan")

    async with asgi_client(silent) as client:
        with pytest.raises(RuntimeError, match="without starting a response"):
            async with request(client, "GET", "http://testserver/") as _response:
                pass  # pragma: no cover


async def test_abandoning_the_response_body_cancels_the_app() -> None:
    cancelled = asyncio.Event()

    async def endless(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            raise RuntimeError("this app has no lifespan")
        await send({"type": "http.response.start", "status": 200, "headers": []})
        try:
            while True:
                await send({"type": "http.response.body", "body": b"tick", "more_body": True})
        except asyncio.CancelledError:
            cancelled.set()
            raise

    async with asgi_client(endless) as client:
        async with request(client, "GET", "http://testserver/") as (_head, body):
            assert await anext(aiter(body)) == b"tick"  # then walk away, leaving the app mid-stream

    assert cancelled.is_set()


async def test_loopback_client_round_trips_over_the_real_wire_without_a_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def refuse(*args: object, **kwargs: object) -> None:
        raise AssertionError("the loopback client must not open a connection")  # pragma: no cover

    monkeypatch.setattr(asyncio, "open_connection", refuse)
    async with loopback_client(echo_app) as client:
        async with request(client, "GET", "http://testserver/items") as (head, body):
            assert head.status == 200
            assert await body.read() == b"GET /items test= body="


async def test_the_no_socket_probe_can_fail() -> None:
    # The proof that the two probes above assert something: the same patch, pointed at a
    # pool that does open a connection, stops it dead.
    async def refuse(*args: object, **kwargs: object) -> None:
        raise AssertionError("this pool must not open a connection")

    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(asyncio, "open_connection", refuse)
        async with ConnectionPool() as pool:
            with pytest.raises(AssertionError, match="must not open a connection"):  # pragma: no branch
                async with request(pool, "GET", "http://127.0.0.1:9/items") as _response:
                    pass  # pragma: no cover


async def test_loopback_client_posts_a_streamed_body_and_reuses_the_connection() -> None:
    async def upload() -> AsyncIterator[bytes]:
        yield b"ab"
        yield b"cd"

    async with loopback_client(echo_app) as client:
        async with request(client, "POST", "http://testserver/up", body=upload()) as (_head, body):
            assert await body.read() == b"POST /up test= body=abcd"
        async with request(client, "GET", "http://testserver/again") as (_head, body):
            assert await body.read() == b"GET /again test= body="
        assert isinstance(client, ConnectionPool)
        [host_pool] = client._h11.values()
        assert len(host_pool.idle) == 1  # the second request reused the first connection


async def test_loopback_client_runs_the_same_exchange_over_http2() -> None:
    async with loopback_client(echo_app, http2=True) as client:
        async with request(client, "GET", "http://testserver/items") as (head, body):
            assert head.status == 200
            assert await body.read() == b"GET /items test= body="
        assert isinstance(client, ConnectionPool)
        assert len(client._h2) == 1


async def test_loopback_client_turns_a_crashing_handler_into_a_500() -> None:
    async def crashing(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            raise RuntimeError("this app has no lifespan")
        raise ValueError("handler exploded")

    # Unlike asgi_client, the server is in the path here, so its isolation applies: the
    # failure becomes a response rather than reaching the caller.
    async with loopback_client(crashing) as client:
        async with request(client, "GET", "http://testserver/") as (head, body):
            assert head.status == 500
            assert await body.read() == b"internal server error\n"


async def test_loopback_client_refuses_an_https_url() -> None:
    async with loopback_client(echo_app) as client:
        with pytest.raises(ValueError, match="no TLS"):
            async with request(client, "GET", "https://testserver/items") as _response:
                pass  # pragma: no cover


@asynccontextmanager
async def _pipe(**kwargs: object) -> AsyncIterator[tuple[Endpoint, Endpoint]]:
    """A `pipe` whose four ends are closed on exit, so no writer is left dangling."""
    near, far = pipe(**kwargs)  # type: ignore[arg-type]
    try:
        yield near, far
    finally:
        for _reader, writer in (near, far):
            writer.close()
            await writer.wait_closed()


async def test_pipe_carries_bytes_and_a_half_close_in_both_directions() -> None:
    async with _pipe() as ((client_reader, client_writer), (server_reader, server_writer)):
        client_writer.write(b"ping")
        await client_writer.drain()
        assert await server_reader.readexactly(4) == b"ping"

        server_writer.write(b"pong")
        await server_writer.drain()
        assert await client_reader.readexactly(4) == b"pong"

        assert client_writer.get_extra_info("peername") == ("testserver", 80)
        assert client_writer.get_extra_info("sockname") == ("127.0.0.1", 51234)
        assert client_writer.get_extra_info("socket") is None
        assert client_writer.get_extra_info("ssl_object") is None

        client_writer.write_eof()
        assert await server_reader.read() == b""


async def test_pipe_applies_backpressure_when_the_reader_falls_behind() -> None:
    async with _pipe(limit=16) as ((_client_reader, client_writer), (server_reader, _server_writer)):
        client_writer.write(b"x" * 64)  # well past the reader's buffer limit
        drain = asyncio.create_task(client_writer.drain())
        await yield_once()
        assert not drain.done()  # the writer is parked until the reader catches up

        assert await server_reader.readexactly(64) == b"x" * 64
        await drain


async def test_pipe_close_ends_both_sides() -> None:
    async with _pipe() as ((_client_reader, client_writer), (server_reader, _server_writer)):
        client_writer.close()
        await client_writer.wait_closed()

        assert client_writer.is_closing()
        assert await server_reader.read() == b""


def test_scope_from_a_relative_url_fails_loudly() -> None:
    with pytest.raises(ValueError, match="must be absolute"):
        scope_from_client_request(ClientRequest("GET", "/items"))


def test_scope_carries_the_url_and_a_synthesized_host_header() -> None:
    scope = scope_from_client_request(ClientRequest("GET", "https://api.test:8443/a%20b?q=1&r=2"))

    assert scope.method == "GET"
    assert scope.scheme == "https"
    assert scope.path == "/a b"
    assert scope.raw_path == b"/a%20b"
    assert scope.query_string == b"q=1&r=2"
    assert scope.server == ("api.test", 8443)
    assert scope.headers == ((b"host", b"api.test:8443"),)


def test_scope_keeps_a_host_header_the_caller_set() -> None:
    outgoing = ClientRequest("GET", "http://api.test/x", ((b"host", b"chosen.test"),))

    assert scope_from_client_request(outgoing).headers == ((b"host", b"chosen.test"),)
