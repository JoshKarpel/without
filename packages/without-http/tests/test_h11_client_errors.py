from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextlib import suppress

from without_http import ConnectionPool
from without_http import request


@asynccontextmanager
async def _raw_server(response: bytes) -> AsyncIterator[tuple[str, int]]:
    """Serve one connection: read the request line+headers, then write a fixed `response`."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            with suppress(asyncio.IncompleteReadError, ConnectionResetError, OSError):
                await reader.readuntil(b"\r\n\r\n")
            writer.write(response)
            await writer.drain()
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    async with server:
        yield host, port


async def test_read_head_skips_an_informational_response() -> None:
    response = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhello"
    async with _raw_server(response) as (host, port), ConnectionPool() as pool:
        async with request(pool, "GET", f"http://{host}:{port}/") as (head, body):
            assert head.status == 200
            assert await body.read() == b"hello"


async def test_a_connection_close_response_is_not_returned_to_the_pool() -> None:
    response = b"HTTP/1.1 200 OK\r\nconnection: close\r\ncontent-length: 2\r\n\r\nhi"
    async with _raw_server(response) as (host, port), ConnectionPool() as pool:
        async with request(pool, "GET", f"http://{host}:{port}/") as (head, body):
            assert head.status == 200
            assert await body.read() == b"hi"
        idle = sum(len(host_pool.idle) for host_pool in pool._h11.values())
    assert idle == 0
