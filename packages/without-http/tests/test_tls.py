from __future__ import annotations

import asyncio
import ssl
from collections.abc import Iterator
from contextlib import suppress
from pathlib import Path

import httpx
import pytest
import trustme
from test_websocket import WebSocketClient
from without_asgi import RawScope
from without_asgi import Receive
from without_asgi import Send
from without_http import server_ssl_context
from without_http import serving
from wsproto.events import AcceptConnection
from wsproto.events import TextMessage

_HOST = "127.0.0.1"


async def scheme_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """A raw ASGI HTTP app that reports the scope's `scheme` as the body."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    scheme = scope["scheme"]
    assert isinstance(scheme, str)
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": scheme.encode()})


async def scheme_ws_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """A raw ASGI WebSocket app that accepts and sends back the scope's `scheme`."""
    if scope["type"] != "websocket":
        raise RuntimeError("this app serves only websocket")
    scheme = scope["scheme"]
    assert isinstance(scheme, str)
    while True:
        message = await receive()
        match message["type"]:
            case "websocket.connect":
                await send({"type": "websocket.accept"})
                await send({"type": "websocket.send", "text": scheme})
            case "websocket.disconnect":
                return


@pytest.fixture(scope="module")
def authority() -> trustme.CA:
    return trustme.CA()


@pytest.fixture(scope="module")
def server_context(authority: trustme.CA, tmp_path_factory: pytest.TempPathFactory) -> ssl.SSLContext:
    pem: Path = tmp_path_factory.mktemp("tls") / "server.pem"
    authority.issue_cert(_HOST).private_key_and_cert_chain_pem.write_to_path(pem)
    return server_ssl_context(pem)


@pytest.fixture
def trusting_client_context(authority: trustme.CA) -> Iterator[ssl.SSLContext]:
    context = ssl.create_default_context()
    authority.configure_trust(context)
    yield context


async def test_serves_https_with_the_scheme_marked_secure(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with serving(scheme_app, ssl_context=server_context) as (host, port):
        async with httpx.AsyncClient(verify=trusting_client_context) as client:
            response = await client.get(f"https://{host}:{port}/where")

    assert response.status_code == 200
    assert response.text == "https"


async def test_serves_wss_with_the_scheme_marked_secure(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with serving(scheme_ws_app, ssl_context=server_context) as (host, port):
        client = await WebSocketClient.connect(host, port, "/live", ssl_context=trusting_client_context)
        try:
            assert isinstance(await client.next_event(), AcceptConnection)
            reported = await client.next_event()
        finally:
            await client.aclose()

    assert isinstance(reported, TextMessage)
    assert reported.data == "wss"


async def test_serves_https_under_a_concurrency_cap(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with serving(scheme_app, ssl_context=server_context, max_concurrent_connections=1) as (host, port):
        async with httpx.AsyncClient(verify=trusting_client_context) as client:
            response = await client.get(f"https://{host}:{port}/where")

    assert response.status_code == 200
    assert response.text == "https"


async def test_alpn_negotiates_http_1_1_when_a_client_also_offers_h2(server_context: ssl.SSLContext) -> None:
    async with serving(scheme_app, ssl_context=server_context) as (host, port):
        client_context = ssl.create_default_context()
        client_context.check_hostname = False
        client_context.verify_mode = ssl.CERT_NONE
        client_context.set_alpn_protocols(["h2", "http/1.1"])
        _reader, writer = await asyncio.open_connection(host, port, ssl=client_context)
        try:
            negotiated = writer.get_extra_info("ssl_object").selected_alpn_protocol()
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()

    assert negotiated == "http/1.1"
