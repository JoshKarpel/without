from __future__ import annotations

import asyncio
import ssl
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path

import httpx
import pytest
import trustme
from without_asgi import RawScope
from without_asgi import Receive
from without_asgi import Send
from without_asgi import parse_tls
from without_http import distinguished_name
from without_http import server_ssl_context
from without_http import serving
from wsproto.events import AcceptConnection
from wsproto.events import TextMessage

from .helpers import WebSocketClient
from .helpers import echo_app


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


async def tls_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """A raw ASGI HTTP app that reports the scope's `tls` extension as the body."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": _reported(scope).encode()})


async def tls_ws_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """A raw ASGI WebSocket app that accepts and sends back the scope's `tls` extension."""
    if scope["type"] != "websocket":
        raise RuntimeError("this app serves only websocket")
    while True:
        message = await receive()
        match message["type"]:
            case "websocket.connect":
                await send({"type": "websocket.accept"})
                await send({"type": "websocket.send", "text": _reported(scope)})
            case "websocket.disconnect":  # pragma: no branch - the client only connects then disconnects
                return


def _reported(scope: RawScope) -> str:
    """The scope's TLS facts as a line a test can assert on, or `none` off TLS."""
    extensions = scope.get("extensions")
    assert extensions is None or isinstance(extensions, Mapping)
    tls = parse_tls(extensions)
    if tls is None:
        return "none"
    return f"{tls.tls_version}|{tls.client_cert_name}|{len(tls.client_cert_chain)}"


@pytest.fixture
def mutual_server_context(authority: trustme.CA, tmp_path: Path) -> ssl.SSLContext:
    """A server context that accepts, and trusts, a client certificate from the test CA."""
    pem = tmp_path / "server.pem"
    authority.issue_cert("127.0.0.1").private_key_and_cert_chain_pem.write_to_path(pem)
    context = server_ssl_context(pem)
    context.verify_mode = ssl.CERT_OPTIONAL
    authority.configure_trust(context)
    return context


@pytest.mark.parametrize(
    ("subject", "expected"),
    [
        pytest.param(((("commonName", "a client"),),), "CN=a client", id="one attribute"),
        pytest.param(
            ((("countryName", "US"),), (("organizationName", "Widgets"),), (("commonName", "a client"),)),
            "CN=a client,O=Widgets,C=US",
            id="most specific first",
        ),
        pytest.param(
            ((("commonName", "a client"), ("userId", "u-17")),),
            "CN=a client+UID=u-17",
            id="multi-valued name",
        ),
        pytest.param(((("commonName", "Smith, John"),),), "CN=Smith\\, John", id="escaped separator"),
        pytest.param(((("commonName", " padded "),),), "CN=\\ padded\\ ", id="escaped padding"),
        pytest.param(((("commonName", " "),),), "CN=\\ ", id="a lone space is escaped once"),
        pytest.param(((("1.2.3.4", "opaque"),),), "1.2.3.4=opaque", id="an unlisted type keeps its name"),
    ],
)
def test_renders_a_subject_as_an_rfc4514_distinguished_name(
    subject: tuple[tuple[tuple[str, str], ...], ...], expected: str
) -> None:
    assert distinguished_name(subject) == expected


async def test_an_https_scope_carries_the_tls_extension(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with serving(tls_app, ssl_context=server_context) as server:
        async with httpx.AsyncClient(verify=trusting_client_context) as client:
            response = await client.get(f"https://{server.host}:{server.port}/where")

    # TLS 1.3, no client certificate offered, so no chain and no subject name.
    assert response.text == f"{ssl.TLSVersion.TLSv1_3.value}|None|0"


async def test_an_h2_scope_carries_the_tls_extension(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with serving(tls_app, ssl_context=server_context) as server:
        async with httpx.AsyncClient(http2=True, verify=trusting_client_context) as client:
            response = await client.get(f"https://{server.host}:{server.port}/where")

    assert response.http_version == "HTTP/2"
    assert response.text == f"{ssl.TLSVersion.TLSv1_3.value}|None|0"


async def test_a_wss_scope_carries_the_tls_extension(
    server_context: ssl.SSLContext, trusting_client_context: ssl.SSLContext
) -> None:
    async with serving(tls_ws_app, ssl_context=server_context) as server:
        client = await WebSocketClient.connect(server.host, server.port, "/live", ssl_context=trusting_client_context)
        try:
            assert isinstance(await client.next_event(), AcceptConnection)
            reported = await client.next_event()
        finally:
            await client.aclose()

    assert isinstance(reported, TextMessage)
    assert reported.data == f"{ssl.TLSVersion.TLSv1_3.value}|None|0"


async def test_a_cleartext_scope_carries_no_tls_extension() -> None:
    async with serving(tls_app) as server:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://{server.host}:{server.port}/where")

    assert response.text == "none"


async def test_a_client_certificate_reaches_the_app_as_a_chain_and_a_subject_name(
    authority: trustme.CA, mutual_server_context: ssl.SSLContext, tmp_path: Path
) -> None:
    leaf = authority.issue_cert("client@example.com", common_name="a client")
    client_pem = tmp_path / "client.pem"
    leaf.private_key_and_cert_chain_pem.write_to_path(client_pem)
    client_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    authority.configure_trust(client_context)
    client_context.load_cert_chain(client_pem)

    async with serving(tls_app, ssl_context=mutual_server_context) as server:
        async with httpx.AsyncClient(verify=client_context) as client:
            response = await client.get(f"https://{server.host}:{server.port}/where")

    version, name, chain_length = response.text.split("|")
    assert version == str(ssl.TLSVersion.TLSv1_3.value)
    # The subject is most-specific first, and trustme salts the organizational unit.
    assert name.startswith("CN=a client,OU=Testing cert #")
    assert ",O=trustme" in name  # trustme stamps its own version into the organization name
    # The verified chain is the leaf plus the CA that issued it.
    assert chain_length == "2"


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


async def test_loads_the_key_from_a_separate_keyfile(
    authority: trustme.CA, trusting_client_context: ssl.SSLContext, tmp_path: Path
) -> None:
    leaf = authority.issue_cert("127.0.0.1")
    certfile = tmp_path / "cert.pem"
    keyfile = tmp_path / "key.pem"
    leaf.cert_chain_pems[0].write_to_path(certfile)
    leaf.private_key_pem.write_to_path(keyfile)
    context = server_ssl_context(certfile, keyfile)

    async with serving(scheme_app, ssl_context=context) as server:
        async with httpx.AsyncClient(verify=trusting_client_context) as client:
            response = await client.get(f"https://{server.host}:{server.port}/where")

    assert response.status_code == 200
    assert response.text == "https"
