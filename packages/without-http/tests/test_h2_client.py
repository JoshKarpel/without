from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncIterator
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import trustme
from test_client import echo_app
from without_http import ConnectionPool
from without_http import add_headers
from without_http import server_ssl_context
from without_http import serving
from without_http.client import _origin

_HOST = "127.0.0.1"


@pytest.fixture(scope="module")
def authority() -> trustme.CA:
    return trustme.CA()


@pytest.fixture(scope="module")
def server_context(authority: trustme.CA, tmp_path_factory: pytest.TempPathFactory) -> ssl.SSLContext:
    pem: Path = tmp_path_factory.mktemp("tls") / "server.pem"
    authority.issue_cert(_HOST).private_key_and_cert_chain_pem.write_to_path(pem)
    return server_ssl_context(pem)


@pytest.fixture(scope="module")
def server_context_h11_only(authority: trustme.CA, tmp_path_factory: pytest.TempPathFactory) -> ssl.SSLContext:
    pem: Path = tmp_path_factory.mktemp("tls-h11") / "server.pem"
    authority.issue_cert(_HOST).private_key_and_cert_chain_pem.write_to_path(pem)
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(pem)
    context.set_alpn_protocols(["http/1.1"])
    return context


@pytest.fixture
def trusting_client_context(authority: trustme.CA) -> ssl.SSLContext:
    context = ssl.create_default_context()
    authority.configure_trust(context)
    return context


async def test_an_https_request_with_http2_disabled_uses_http_1_1(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with serving(echo_app, ssl_context=server_context) as server:
        async with ConnectionPool(allow_http2=False, ssl_context=trusting_client_context) as pool:
            async with pool.request("GET", f"https://{_HOST}:{server.port}/items") as (head, body):
                assert head.status == 200
                assert await body.read() == b"GET /items test= body="
            assert pool._h2 == {}


async def test_alpn_fallback_to_http_1_1_pools_and_reuses_an_h11_connection(
    server_context_h11_only: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with serving(echo_app, ssl_context=server_context_h11_only) as server:
        async with ConnectionPool(ssl_context=trusting_client_context) as pool:
            url = f"https://{_HOST}:{server.port}/items"
            async with pool.request("GET", url) as (_head, body):
                assert await body.read() == b"GET /items test= body="
            origin = _origin(urlsplit(url))
            assert origin in pool._h11_only
            assert pool._h2 == {}
            async with pool.request("GET", url) as (_head, body):  # pragma: no branch
                assert await body.read() == b"GET /items test= body="


async def test_an_unusable_pooled_h2_connection_is_replaced(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with serving(echo_app, ssl_context=server_context) as server:
        async with ConnectionPool(ssl_context=trusting_client_context) as pool:
            url = f"https://{_HOST}:{server.port}/items"
            async with pool.request("GET", url) as (_head, body):
                await body.read()
            origin = _origin(urlsplit(url))
            stale = pool._h2[origin]
            await stale.aclose()  # the connection died while pooled
            async with pool.request("GET", url) as (_head, body):
                assert await body.read() == b"GET /items test= body="
            assert pool._h2[origin] is not stale


async def test_client_round_trips_a_get_over_h2(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with serving(echo_app, ssl_context=server_context) as server:
        async with ConnectionPool(ssl_context=trusting_client_context) as pool:
            async with pool.request("GET", f"https://{_HOST}:{server.port}/items") as (head, body):
                assert head.status == 200
                assert await body.read() == b"GET /items test= body="
            assert len(pool._h2) == 1


async def test_client_posts_a_body_over_h2(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with serving(echo_app, ssl_context=server_context) as server:
        async with ConnectionPool(ssl_context=trusting_client_context) as pool:
            url = f"https://{_HOST}:{server.port}/submit"
            async with pool.request("POST", url, body=b"payload") as (_head, body):  # pragma: no branch
                assert await body.read() == b"POST /submit test= body=payload"


async def test_client_multiplexes_concurrent_requests_over_one_h2_connection(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async def fetch(pool: ConnectionPool, port: int, index: int) -> bytes:
        async with pool.request("GET", f"https://{_HOST}:{port}/n{index}") as (_head, body):
            return await body.read()

    async with serving(echo_app, ssl_context=server_context) as server:
        async with ConnectionPool(ssl_context=trusting_client_context) as pool:
            bodies = await asyncio.gather(*(fetch(pool, server.port, index) for index in range(8)))
            assert len(pool._h2) == 1

    assert bodies == [f"GET /n{index} test= body=".encode() for index in range(8)]


async def _chunks(*parts: bytes) -> AsyncIterator[bytes]:
    for part in parts:
        yield part


async def test_client_streams_a_request_body_over_h2(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with serving(echo_app, ssl_context=server_context) as server:
        async with ConnectionPool(ssl_context=trusting_client_context) as pool:
            upload = _chunks(b"ab", b"cd", b"ef")
            async with pool.request("POST", f"https://{_HOST}:{server.port}/up", body=upload) as (
                _head,
                body,
            ):  # pragma: no branch
                assert await body.read() == b"POST /up test= body=abcdef"


async def test_client_round_trips_a_body_larger_than_the_flow_control_window(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    payload = b"z" * 200_000
    async with serving(echo_app, ssl_context=server_context) as server:
        async with ConnectionPool(ssl_context=trusting_client_context) as pool:
            async with pool.request("POST", f"https://{_HOST}:{server.port}/big", body=payload) as (
                _head,
                body,
            ):  # pragma: no branch
                assert await body.read() == b"POST /big test= body=" + payload


async def test_client_add_headers_middleware_reaches_the_server_over_h2(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with (
        serving(echo_app, ssl_context=server_context) as server,
        ConnectionPool(ssl_context=trusting_client_context, middleware=add_headers((b"x-test", b"injected"))) as pool,
        pool.request("GET", f"https://{_HOST}:{server.port}/items") as (_head, body),
    ):
        assert await body.read() == b"GET /items test=injected body="


async def test_cleartext_stays_http_1_1_even_with_http2_enabled() -> None:
    async with serving(echo_app) as server, ConnectionPool(allow_http2=True) as pool:
        async with pool.request("GET", f"http://{_HOST}:{server.port}/items") as (_head, body):
            assert await body.read() == b"GET /items test= body="
        assert pool._h2 == {}
