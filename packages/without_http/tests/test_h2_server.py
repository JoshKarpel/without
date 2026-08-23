from __future__ import annotations

import asyncio
from collections.abc import Callable

import h2.config
import h2.connection
import h2.events
import h2.settings
from without_asgi import ASGIApp
from without_asgi import RawMessage
from without_asgi import Receive
from without_asgi import Send
from without_http.testing import served_pipe

from .helpers import configured_app
from .helpers import crash_app
from .helpers import echo_app
from .helpers import h2_client
from .helpers import h2_request_headers

# Cleartext HTTP/2 by prior knowledge, driven over an in-memory pipe: the codec and the
# server's flow-control sender are the subject, and both run identically without a
# socket. The h2 path reached through ALPN needs a handshake, so it stays on a real one.


def fixed_body_app(size: int) -> ASGIApp:
    """A raw ASGI app that answers with a single body of exactly `size` bytes."""

    async def app(scope: RawMessage, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            raise RuntimeError("this app serves only http")
        await send(
            {"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"application/octet-stream")]}
        )
        await send({"type": "http.response.body", "body": b"x" * size})

    return app


async def big_app(scope: RawMessage, receive: Receive, send: Send) -> None:
    """A raw ASGI app that replies with a body larger than one flow-control window."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    payload = b"x" * 200_000
    await send(
        {"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"application/octet-stream")]}
    )
    await send({"type": "http.response.body", "body": payload})


class H2Result:
    """The status and body of one HTTP/2 response, collected over a cleartext stream."""

    def __init__(self, status: int, body: bytes) -> None:
        self.status = status
        self.body = body


async def _roundtrip(app: ASGIApp, method: str, path: str, body: bytes = b"") -> H2Result:
    """
    Drive one HTTP/2-over-cleartext request to completion via prior knowledge.

    A fresh connection per request keeps the helper small; multiplexing many streams
    over one connection is exercised through the TLS path instead.
    """
    async with served_pipe(app) as (reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, h2_request_headers(method, path), end_stream=not body)
        if body:
            conn.send_data(stream_id, body, end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()

        status = 0
        chunks: list[bytes] = []
        done = False
        while not done:
            data = await reader.read(65536)
            if not data:  # pragma: no cover - the server always ends the stream before EOF here
                break
            for event in conn.receive_data(data):
                match event:  # pragma: no branch - the helper drives a single known stream
                    case h2.events.ResponseReceived(stream_id=sid, headers=headers) if sid == stream_id:
                        status = int(next(value for name, value in headers if name == b":status").decode())
                    case h2.events.DataReceived(stream_id=sid, data=chunk, flow_controlled_length=length) if (
                        sid == stream_id
                    ):
                        chunks.append(chunk)
                        conn.acknowledge_received_data(length, stream_id)
                    case h2.events.StreamEnded(stream_id=sid) if sid == stream_id:
                        done = True
                    case _:
                        pass
            writer.write(conn.data_to_send())
            await writer.drain()
    return H2Result(status, b"".join(chunks))


async def test_serves_a_get_response_over_cleartext_prior_knowledge() -> None:
    result = await _roundtrip(echo_app, "GET", "/items")

    assert result.status == 200
    assert result.body == b"GET /items "


async def test_serves_a_post_body() -> None:
    result = await _roundtrip(echo_app, "POST", "/submit", b"payload")

    assert result.body == b"POST /submit payload"


async def test_a_head_request_omits_the_body() -> None:
    result = await _roundtrip(echo_app, "HEAD", "/items")

    assert result.status == 200
    assert result.body == b""


async def test_threads_lifespan_state_into_the_handler() -> None:
    result = await _roundtrip(configured_app(), "GET", "/where")

    assert result.body == b"configured-state:/where"


async def test_a_crashing_handler_returns_500() -> None:
    result = await _roundtrip(crash_app(), "GET", "/boom")

    assert result.status == 500


async def test_a_response_larger_than_the_flow_control_window_round_trips() -> None:
    result = await _roundtrip(big_app, "GET", "/big")

    assert result.status == 200
    assert result.body == b"x" * 200_000


async def scheme_app(scope: RawMessage, receive: Receive, send: Send) -> None:
    """Echo the transport-derived scheme back in the response body."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    scheme = scope["scheme"]
    assert isinstance(scheme, str)
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": scheme.encode()})


async def address_app(scope: RawMessage, receive: Receive, send: Send) -> None:
    """Report whether the scope carries a server and a client address."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    payload = f"{scope['server'] is not None}|{scope['client'] is not None}".encode()
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": payload})


async def test_a_cleartext_request_gets_the_http_scheme() -> None:
    result = await _roundtrip(scheme_app, "GET", "/s")

    assert result.body == b"http"


async def test_the_scope_carries_the_server_and_client_addresses() -> None:
    result = await _roundtrip(address_app, "GET", "/a")

    assert result.body == b"True|True"


async def _drive_blocked_then_bump(
    app: ASGIApp,
    *,
    initial_window: int,
    body_size: int,
    bump_after: int,
    bump: Callable[[h2.connection.H2Connection, int], None],
) -> bytes:
    """
    Drive a GET whose body exceeds the peer's flow-control window, so the server's
    sender blocks after `bump_after` bytes. Once that many bytes have arrived, apply
    `bump` (a window grant or settings change) and return the fully reassembled body.

    A sender that is never woken by the grant leaves the body short of `body_size`,
    so the surrounding read never completes and its `asyncio.timeout` fires.
    """
    async with served_pipe(app) as (reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        conn.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: initial_window})
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, h2_request_headers("GET", "/w"), end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()

        received = bytearray()
        bumped = False
        async with asyncio.timeout(10):
            while len(received) < body_size:
                data = await reader.read(65536)
                if not data:  # pragma: no cover - the woken sender completes the body before EOF
                    break
                for event in conn.receive_data(data):
                    if isinstance(event, h2.events.DataReceived):
                        received.extend(event.data)
                writer.write(conn.data_to_send())
                await writer.drain()
                if not bumped and len(received) >= bump_after:
                    bump(conn, stream_id)
                    writer.write(conn.data_to_send())
                    await writer.drain()
                    bumped = True
    return bytes(received)


async def test_a_settings_increase_wakes_a_window_blocked_sender() -> None:
    def bump(conn: h2.connection.H2Connection, _stream_id: int) -> None:
        conn.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 500})

    body = await _drive_blocked_then_bump(
        fixed_body_app(100), initial_window=10, body_size=100, bump_after=10, bump=bump
    )

    assert body == b"x" * 100


async def test_a_stream_window_update_wakes_the_blocked_stream() -> None:
    def bump(conn: h2.connection.H2Connection, stream_id: int) -> None:
        conn.increment_flow_control_window(500, stream_id=stream_id)

    body = await _drive_blocked_then_bump(
        fixed_body_app(100), initial_window=10, body_size=100, bump_after=10, bump=bump
    )

    assert body == b"x" * 100


async def test_a_connection_window_update_wakes_the_blocked_sender() -> None:
    def bump(conn: h2.connection.H2Connection, _stream_id: int) -> None:
        conn.increment_flow_control_window(200_000, stream_id=None)

    body = await _drive_blocked_then_bump(
        fixed_body_app(100_000), initial_window=1_000_000, body_size=100_000, bump_after=65_535, bump=bump
    )

    assert body == b"x" * 100_000


async def test_a_single_byte_window_still_sends_the_first_byte() -> None:
    # With a per-stream window of exactly one byte, a sender computing `sendable > 0`
    # sends that one byte, while a `sendable > 1` off-by-one would block without it.
    async with served_pipe(fixed_body_app(2)) as (reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        conn.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 1})
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, h2_request_headers("GET", "/one"), end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()

        first = b""
        async with asyncio.timeout(5):
            while not first:  # never acknowledged, so the window stays at one byte
                data = await reader.read(65536)
                if not data:  # pragma: no cover - the first byte arrives before EOF
                    break
                for event in conn.receive_data(data):
                    if isinstance(event, h2.events.DataReceived):
                        first += event.data
                writer.write(conn.data_to_send())
                await writer.drain()
        assert first == b"x"
