from __future__ import annotations

import h11
import pytest
from without_asgi import WebsocketAccept
from without_asgi import WebsocketBinary
from without_asgi import WebsocketClose
from without_asgi import WebsocketSend
from without_asgi import WebsocketText
from without_http.ws_wire import is_websocket_upgrade
from without_http.ws_wire import websocket_scope_from_request
from without_http.ws_wire import ws_events_from_outbound
from wsproto.events import AcceptConnection
from wsproto.events import BytesMessage
from wsproto.events import CloseConnection
from wsproto.events import RejectConnection
from wsproto.events import TextMessage


def _handshake(target: str = "/live", *, protocols: str | None = None) -> h11.Request:
    headers = [("host", "example.test"), ("upgrade", "websocket"), ("connection", "upgrade")]
    if protocols is not None:
        headers.append(("sec-websocket-protocol", protocols))
    return h11.Request(method="GET", target=target, headers=headers, http_version="1.1")


def test_is_websocket_upgrade_detects_the_upgrade_header() -> None:
    assert is_websocket_upgrade(_handshake()) is True


def test_is_websocket_upgrade_is_false_for_a_plain_request() -> None:
    plain = h11.Request(method="GET", target="/", headers=[("host", "t")], http_version="1.1")

    assert is_websocket_upgrade(plain) is False


def test_websocket_scope_reads_the_handshake() -> None:
    scope = websocket_scope_from_request(
        _handshake("/live?room=lobby", protocols="chat, superchat"),
        scheme="ws",
        server=("example.test", 80),
        client=("198.51.100.7", 5000),
    )

    assert scope.path == "/live"
    assert scope.query_string == b"room=lobby"
    assert scope.subprotocols == ("chat", "superchat")
    assert scope.client == ("198.51.100.7", 5000)


def test_ws_events_from_outbound_renders_an_accept() -> None:
    events = ws_events_from_outbound(WebsocketAccept(subprotocol="chat"), accepted=False)

    assert events == [AcceptConnection(subprotocol="chat", extensions=[], extra_headers=[])]


@pytest.mark.parametrize(
    ("outbound", "expected"),
    [
        (WebsocketSend(WebsocketText("hi")), TextMessage(data="hi")),
        (WebsocketSend(WebsocketBinary(b"\x00\x01")), BytesMessage(data=b"\x00\x01")),
    ],
)
def test_ws_events_from_outbound_renders_data_frames(outbound: WebsocketSend, expected: object) -> None:
    assert ws_events_from_outbound(outbound, accepted=True) == [expected]


def test_ws_events_from_outbound_closes_an_accepted_connection() -> None:
    events = ws_events_from_outbound(WebsocketClose(code=1011, reason="boom"), accepted=True)

    assert events == [CloseConnection(code=1011, reason="boom")]


def test_ws_events_from_outbound_rejects_a_connection_closed_before_accept() -> None:
    events = ws_events_from_outbound(WebsocketClose(code=1000, reason=""), accepted=False)

    assert events == [RejectConnection(status_code=403)]
