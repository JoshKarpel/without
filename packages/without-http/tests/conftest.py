from __future__ import annotations

import asyncio
import socket
import ssl
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path

import pytest
import trustme
from without_http import server_ssl_context

HOST = "127.0.0.1"

type _Endpoint = tuple[asyncio.StreamReader, asyncio.StreamWriter]
type _StreamPairFactory = Callable[[], Awaitable[tuple[_Endpoint, _Endpoint]]]


@pytest.fixture
async def stream_pair() -> AsyncIterator[_StreamPairFactory]:
    """
    A factory for two connected asyncio stream endpoints over a local socketpair.

    Gives a test both ends of a real connection with nothing in between, so it can drive
    the exact bytes and (half-)close timing a connection-lifecycle test needs, without a
    `serving()` accept loop that would cancel the connection out from under it.
    """
    writers: list[asyncio.StreamWriter] = []

    async def make() -> tuple[_Endpoint, _Endpoint]:
        left, right = socket.socketpair()
        loop = asyncio.get_running_loop()

        async def wrap(sock: socket.socket) -> _Endpoint:
            reader = asyncio.StreamReader()
            protocol = asyncio.StreamReaderProtocol(reader)
            transport, _ = await loop.create_connection(lambda: protocol, sock=sock)
            writer = asyncio.StreamWriter(transport, protocol, reader, loop)
            writers.append(writer)
            return reader, writer

        return await wrap(left), await wrap(right)

    yield make
    for writer in writers:
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()


@pytest.fixture(scope="session")
def authority() -> trustme.CA:
    return trustme.CA()


@pytest.fixture(scope="session")
def server_context(authority: trustme.CA, tmp_path_factory: pytest.TempPathFactory) -> ssl.SSLContext:
    pem: Path = tmp_path_factory.mktemp("tls") / "server.pem"
    authority.issue_cert(HOST).private_key_and_cert_chain_pem.write_to_path(pem)
    return server_ssl_context(pem)


@pytest.fixture
def trusting_client_context_factory(authority: trustme.CA) -> Callable[[], ssl.SSLContext]:
    """A `ConnectionPool.ssl_context_factory` that trusts only the test CA."""

    def make() -> ssl.SSLContext:
        # Trust only the test CA. create_default_context() would additionally load the system
        # root store (~7ms per call), which these localhost tests never use.
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        authority.configure_trust(context)
        return context

    return make


# A single context for consumers that take one directly (httpx, WebSocketClient). They call
# `set_alpn_protocols(...)` on it, so it must stay function-scoped: a wider scope would let one
# test's ALPN choice leak into the next. ConnectionPool takes the factory above instead.
@pytest.fixture
def trusting_client_context(trusting_client_context_factory: Callable[[], ssl.SSLContext]) -> ssl.SSLContext:
    return trusting_client_context_factory()
