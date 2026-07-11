from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator
from collections.abc import Callable
from datetime import timedelta

import pytest
from without_http import ConnectionPool
from without_http import TCPKeepalive
from without_http.client import _open
from without_http.keepalive import apply_tcp_keepalive

HOST = "127.0.0.1"


@pytest.fixture
async def listener() -> AsyncIterator[tuple[str, int]]:
    """A bare TCP listener that accepts and holds connections, yielding its address."""
    accepted: list[asyncio.StreamWriter] = []

    async def hold(_reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        accepted.append(writer)

    server = await asyncio.start_server(hold, HOST, 0)
    host, port = server.sockets[0].getsockname()[:2]
    try:
        yield host, port
    finally:
        for writer in accepted:
            writer.transport.abort()
        server.close()
        await server.wait_closed()


def _keepalive_enabled(writer: asyncio.StreamWriter) -> bool:
    sock = writer.get_extra_info("socket")
    return bool(sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE))


async def test_open_enables_keepalive_when_configured(listener: tuple[str, int]) -> None:
    host, port = listener
    _reader, writer, _ = await _open(host, port, ssl_context=None, keepalive=TCPKeepalive())
    try:
        assert _keepalive_enabled(writer)
    finally:
        writer.close()


async def test_open_leaves_keepalive_off_when_disabled(listener: tuple[str, int]) -> None:
    host, port = listener
    _reader, writer, _ = await _open(host, port, ssl_context=None, keepalive=None)
    try:
        assert not _keepalive_enabled(writer)
    finally:
        writer.close()


# Every CI platform resolves all three probe knobs (the idle one via TCP_KEEPALIVE on
# macOS, TCP_KEEPIDLE elsewhere), so these assert against `socket_options()` rather than
# naming a platform-specific constant, and run everywhere instead of skipping (a skipped
# body would leave the test uncovered, and this repo measures test files at 100%).
def test_socket_options_enables_keepalive_then_the_configured_probe_values() -> None:
    keepalive = TCPKeepalive(idle=timedelta(seconds=45), interval=timedelta(seconds=7), count=3)
    options = keepalive.socket_options()
    assert options[0] == (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    assert all(level == socket.IPPROTO_TCP for level, _option, _value in options[1:])
    assert [value for _level, _option, value in options[1:]] == [45, 7, 3]


async def test_open_applies_the_configured_probe_values_to_the_socket(listener: tuple[str, int]) -> None:
    host, port = listener
    keepalive = TCPKeepalive(idle=timedelta(seconds=45), interval=timedelta(seconds=7), count=3)
    _reader, writer, _ = await _open(host, port, ssl_context=None, keepalive=keepalive)
    sock = writer.get_extra_info("socket")
    try:
        for level, option, value in keepalive.socket_options():
            if level == socket.IPPROTO_TCP:
                assert sock.getsockopt(level, option) == value
    finally:
        writer.close()


def test_apply_is_a_noop_when_the_transport_has_no_socket() -> None:
    class _SocketlessWriter:
        def get_extra_info(self, _name: str) -> None:
            return None

    apply_tcp_keepalive(_SocketlessWriter(), TCPKeepalive())  # type: ignore[arg-type]


def test_pool_enables_keepalive_by_default() -> None:
    assert ConnectionPool().tcp_keepalive == TCPKeepalive()


@pytest.mark.parametrize(
    "make",
    [lambda d: TCPKeepalive(idle=d), lambda d: TCPKeepalive(interval=d)],
    ids=["idle", "interval"],
)
def test_a_fractional_second_probe_duration_is_rejected(make: Callable[[timedelta], TCPKeepalive]) -> None:
    with pytest.raises(ValueError, match="must be a whole number of seconds"):
        make(timedelta(milliseconds=1500))
