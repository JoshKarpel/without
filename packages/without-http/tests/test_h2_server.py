from __future__ import annotations

import asyncio
from contextlib import suppress

import h2.config
import h2.connection
import h2.events
from test_server import configured_app
from test_server import crash_app
from test_server import echo_app
from without_asgi import ASGIApp
from without_asgi import RawMessage
from without_asgi import Receive
from without_asgi import Send
from without_http import serving


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


async def h2c_roundtrip(host: str, port: int, method: str, path: str, body: bytes = b"") -> H2Result:
    """
    Drive one HTTP/2-over-cleartext request to completion via prior knowledge.

    A fresh connection per request keeps the helper small; multiplexing many streams
    over one connection is exercised through the TLS path instead.
    """
    reader, writer = await asyncio.open_connection(host, port)
    conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=True, header_encoding=None))
    conn.initiate_connection()
    stream_id = conn.get_next_available_stream_id()
    request_headers = [
        (b":method", method.encode()),
        (b":path", path.encode()),
        (b":scheme", b"http"),
        (b":authority", f"{host}:{port}".encode()),
    ]
    conn.send_headers(stream_id, request_headers, end_stream=not body)
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
    writer.close()
    with suppress(OSError):
        await writer.wait_closed()
    return H2Result(status, b"".join(chunks))


async def _roundtrip(app: ASGIApp, method: str, path: str, body: bytes = b"") -> H2Result:
    async with serving(app) as server:
        return await h2c_roundtrip(server.host, server.port, method, path, body)


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
