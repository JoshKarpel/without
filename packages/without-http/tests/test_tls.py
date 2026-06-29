from __future__ import annotations

import asyncio
import ssl
from contextlib import suppress
from pathlib import Path

import httpx
import pytest
import trustme
from test_server import echo_app
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
            case "websocket.disconnect":  # pragma: no branch - the client only connects then disconnects
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
def trusting_client_context(authority: trustme.CA) -> ssl.SSLContext:
    context = ssl.create_default_context()
    authority.configure_trust(context)
    return context


async def test_serves_https_with_the_scheme_marked_secure(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with serving(scheme_app, ssl_context=server_context) as server:
        async with httpx.AsyncClient(verify=trusting_client_context) as client:
            response = await client.get(f"https://{server.host}:{server.port}/where")

    assert response.status_code == 200
    assert response.text == "https"


async def test_serves_wss_with_the_scheme_marked_secure(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with serving(scheme_ws_app, ssl_context=server_context) as server:
        client = await WebSocketClient.connect(server.host, server.port, "/live", ssl_context=trusting_client_context)
        try:
            assert isinstance(await client.next_event(), AcceptConnection)
            reported = await client.next_event()
        finally:
            await client.aclose()

    assert isinstance(reported, TextMessage)
    assert reported.data == "wss"


async def _negotiated_alpn(host: str, port: int, offered: list[str]) -> str | None:
    client_context = ssl.create_default_context()
    client_context.check_hostname = False
    client_context.verify_mode = ssl.CERT_NONE
    client_context.set_alpn_protocols(offered)
    _reader, writer = await asyncio.open_connection(host, port, ssl=client_context)
    try:
        return writer.get_extra_info("ssl_object").selected_alpn_protocol()
    finally:
        writer.close()
        with suppress(OSError):
            await writer.wait_closed()


async def test_alpn_negotiates_h2_when_a_client_offers_it(server_context: ssl.SSLContext) -> None:
    async with serving(scheme_app, ssl_context=server_context) as server:
        negotiated = await _negotiated_alpn(server.host, server.port, ["h2", "http/1.1"])

    assert negotiated == "h2"


async def test_alpn_falls_back_to_http_1_1_for_a_client_without_h2(server_context: ssl.SSLContext) -> None:
    async with serving(scheme_app, ssl_context=server_context) as server:
        negotiated = await _negotiated_alpn(server.host, server.port, ["http/1.1"])

    assert negotiated == "http/1.1"


async def test_serves_https_over_h2_when_alpn_negotiates_it(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with serving(scheme_app, ssl_context=server_context) as server:
        async with httpx.AsyncClient(http2=True, verify=trusting_client_context) as client:
            response = await client.get(f"https://{server.host}:{server.port}/where")

    assert response.http_version == "HTTP/2"
    assert response.text == "https"


async def test_multiplexes_concurrent_requests_over_h2(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with (
        serving(echo_app, ssl_context=server_context) as server,
        httpx.AsyncClient(
            base_url=f"https://{server.host}:{server.port}", http2=True, verify=trusting_client_context
        ) as client,
    ):
        responses = await asyncio.gather(*(client.get(f"/n{index}") for index in range(8)))

    assert {response.http_version for response in responses} == {"HTTP/2"}
    assert [response.text for response in responses] == [f"GET /n{index} " for index in range(8)]


async def test_h2_round_trips_a_body_larger_than_the_flow_control_window(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    payload = b"z" * 200_000
    async with serving(echo_app, ssl_context=server_context) as server:
        async with httpx.AsyncClient(http2=True, verify=trusting_client_context) as client:
            response = await client.post(f"https://{server.host}:{server.port}/big", content=payload)

    assert response.http_version == "HTTP/2"
    assert response.text == "POST /big " + payload.decode()
