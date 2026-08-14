from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import h2.config
import h2.connection
import h2.events
from without_http import ConnectionPool
from without_http import ResponseBody
from without_http import ResponseTrailers
from without_http import request


async def _events(*items: bytes | ResponseTrailers) -> AsyncGenerator[bytes | ResponseTrailers]:
    for item in items:
        yield item


# `ResponseBody` over a hand-built event stream: the functional core, no sockets. These
# exercise the multiplicity the wire cannot (a single response puts at most one trailer
# block on the wire), so the body's 0/1/many handling is pinned here.


async def test_read_drops_trailers() -> None:
    body = ResponseBody(_events(b"ab", b"cd", ResponseTrailers(((b"x-checksum", b"9"),))))
    assert await body.read() == b"abcd"


async def test_iteration_drops_trailers_but_still_drains() -> None:
    body = ResponseBody(_events(b"ab", ResponseTrailers(((b"x-checksum", b"9"),))))
    chunks = [chunk async for chunk in body]
    assert chunks == [b"ab"]


async def test_read_with_trailers_collects_multiple_blocks_in_order() -> None:
    body = ResponseBody(
        _events(
            b"ab",
            b"cd",
            ResponseTrailers(((b"x-first", b"1"),)),
            ResponseTrailers(((b"x-second", b"2"),)),
        )
    )
    data, trailers = await body.read_with_trailers()
    assert data == b"abcd"
    assert trailers == (ResponseTrailers(((b"x-first", b"1"),)), ResponseTrailers(((b"x-second", b"2"),)))


async def test_read_with_trailers_is_empty_when_none_present() -> None:
    body = ResponseBody(_events(b"ab", b"cd"))
    data, trailers = await body.read_with_trailers()
    assert data == b"abcd"
    assert trailers == ()


async def test_events_yields_bytes_then_every_trailer_block() -> None:
    body = ResponseBody(
        _events(b"ab", ResponseTrailers(((b"x-first", b"1"),)), ResponseTrailers(((b"x-second", b"2"),)))
    )
    items = [item async for item in body.events()]
    assert items == [b"ab", ResponseTrailers(((b"x-first", b"1"),)), ResponseTrailers(((b"x-second", b"2"),))]


# Real-wire reception: arbitrary servers that send trailers, one block each (the wire
# limit), driven by raw `asyncio`/`h2` servers rather than without-http's own server,
# which does not yet emit trailers.


@asynccontextmanager
async def _raw_http11_server(response: bytes) -> AsyncIterator[tuple[str, int]]:
    """Serve a fixed raw HTTP/1.1 `response` to each request on a kept-alive connection."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while await reader.readuntil(
                b"\r\n\r\n"
            ):  # pragma: no branch - readuntil raises on EOF, never returns falsy
                writer.write(response)
                await writer.drain()
        except asyncio.IncompleteReadError, ConnectionResetError, OSError:
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    async with server:
        yield host, port


async def test_h11_client_surfaces_chunked_trailers() -> None:
    response = (
        b"HTTP/1.1 200 OK\r\n"
        b"transfer-encoding: chunked\r\n"
        b"trailer: x-checksum\r\n"
        b"\r\n"
        b"5\r\nhello\r\n"
        b"0\r\n"
        b"x-checksum: abc123\r\n"
        b"\r\n"
    )
    async with _raw_http11_server(response) as (host, port), ConnectionPool() as pool:
        async with request(pool, "GET", f"http://{host}:{port}/") as (head, body):
            assert head.status == 200
            data, trailers = await body.read_with_trailers()
    assert data == b"hello"
    assert trailers == (ResponseTrailers(((b"x-checksum", b"abc123"),)),)


async def test_h11_dropping_trailers_still_keeps_the_connection_alive() -> None:
    response = b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\nx-trace: t1\r\n\r\n"
    async with _raw_http11_server(response) as (host, port), ConnectionPool() as pool:
        async with request(pool, "GET", f"http://{host}:{port}/") as (_head, body):
            assert await body.read() == b"hello"  # drops the trailer block
        idle = sum(len(host_pool.idle) for host_pool in pool._h11.values())
    assert idle == 1


@asynccontextmanager
async def _raw_h2_server(
    status: int, data: bytes, trailers: list[tuple[bytes, bytes]]
) -> AsyncIterator[tuple[str, int]]:
    """Serve one h2c response carrying `data` then a trailing HEADERS block."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=False, header_encoding=None))
        conn.initiate_connection()
        writer.write(conn.data_to_send())
        await writer.drain()
        try:
            while chunk := await reader.read(65536):
                for event in conn.receive_data(chunk):
                    if isinstance(event, h2.events.RequestReceived):
                        conn.send_headers(event.stream_id, [(b":status", str(status).encode("ascii"))])
                        conn.send_data(event.stream_id, data, end_stream=False)
                        conn.send_headers(event.stream_id, trailers, end_stream=True)
                writer.write(conn.data_to_send())
                await writer.drain()
        except ConnectionResetError, OSError:  # pragma: no cover - the client closes cleanly, reaching EOF not a reset
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    async with server:
        yield host, port


async def test_h2_client_surfaces_trailers() -> None:
    async with (
        _raw_h2_server(200, b"hello", [(b"grpc-status", b"0")]) as (host, port),
        ConnectionPool(force_http2_cleartext=True) as pool,
        request(pool, "GET", f"http://{host}:{port}/") as (head, body),
    ):
        assert head.status == 200
        data, trailers = await body.read_with_trailers()
    assert data == b"hello"
    assert trailers == (ResponseTrailers(((b"grpc-status", b"0"),)),)
