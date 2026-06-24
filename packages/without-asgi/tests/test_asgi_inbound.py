from __future__ import annotations

import pytest
from without_asgi import Disconnect
from without_asgi import RawMessage
from without_asgi import RequestBody
from without_asgi import Shutdown
from without_asgi import Startup
from without_asgi import WebsocketBinary
from without_asgi import WebsocketConnect
from without_asgi import WebsocketDisconnect
from without_asgi import WebsocketReceive
from without_asgi import WebsocketText
from without_asgi import parse_inbound
from without_asgi import parse_lifespan_event
from without_asgi import parse_websocket_inbound


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ({"type": "http.request", "body": b"payload", "more_body": True}, RequestBody(body=b"payload", more_body=True)),
        ({"type": "http.request"}, RequestBody(body=b"", more_body=False)),
        ({"type": "http.disconnect"}, Disconnect()),
    ],
)
def test_parse_inbound_classifies_events(message: RawMessage, expected: object) -> None:
    assert parse_inbound(message) == expected


def test_parse_inbound_rejects_an_unknown_event() -> None:
    with pytest.raises(ValueError, match="unexpected http event"):
        parse_inbound({"type": "http.surprise"})


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ({"type": "lifespan.startup"}, Startup()),
        ({"type": "lifespan.shutdown"}, Shutdown()),
    ],
)
def test_parse_lifespan_event_classifies_events(message: RawMessage, expected: object) -> None:
    assert parse_lifespan_event(message) == expected


def test_parse_lifespan_event_rejects_an_unknown_event() -> None:
    with pytest.raises(ValueError, match="unexpected lifespan event"):
        parse_lifespan_event({"type": "lifespan.restart"})


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ({"type": "websocket.connect"}, WebsocketConnect()),
        ({"type": "websocket.receive", "text": "hello"}, WebsocketReceive(data=WebsocketText(text="hello"))),
        ({"type": "websocket.receive", "bytes": b"\x00\x01"}, WebsocketReceive(data=WebsocketBinary(data=b"\x00\x01"))),
        (
            {"type": "websocket.disconnect", "code": 1001, "reason": "going away"},
            WebsocketDisconnect(1001, "going away"),
        ),
        ({"type": "websocket.disconnect"}, WebsocketDisconnect(code=1005, reason="")),
    ],
)
def test_parse_websocket_inbound_classifies_events(message: RawMessage, expected: object) -> None:
    assert parse_websocket_inbound(message) == expected


def test_parse_websocket_inbound_rejects_a_message_with_both_text_and_bytes() -> None:
    with pytest.raises(ValueError, match="both text and bytes"):
        parse_websocket_inbound({"type": "websocket.receive", "text": "hi", "bytes": b"hi"})


def test_parse_websocket_inbound_rejects_a_message_with_neither_text_nor_bytes() -> None:
    with pytest.raises(ValueError, match="neither text nor bytes"):
        parse_websocket_inbound({"type": "websocket.receive"})


def test_parse_websocket_inbound_rejects_an_unknown_event() -> None:
    with pytest.raises(ValueError, match="unexpected websocket event"):
        parse_websocket_inbound({"type": "websocket.surprise"})
