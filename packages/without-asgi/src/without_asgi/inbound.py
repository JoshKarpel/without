from __future__ import annotations

from dataclasses import dataclass

from without_asgi.narrow import narrow_to_bytes, narrow_to_int, narrow_to_str
from without_asgi.types import RawMessage, WebsocketBinary, WebsocketData, WebsocketText


@dataclass(frozen=True, slots=True)
class RequestBody:
    body: bytes
    more_body: bool


@dataclass(frozen=True, slots=True)
class Disconnect:
    """The client went away before the request finished."""


type Inbound = RequestBody | Disconnect


@dataclass(frozen=True, slots=True)
class WebsocketConnect:
    """The client is opening a websocket and awaiting an accept or a close."""


@dataclass(frozen=True, slots=True)
class WebsocketReceive:
    data: WebsocketData


@dataclass(frozen=True, slots=True)
class WebsocketDisconnect:
    code: int
    reason: str


type WebsocketInbound = WebsocketConnect | WebsocketReceive | WebsocketDisconnect


@dataclass(frozen=True, slots=True)
class Startup:
    pass


@dataclass(frozen=True, slots=True)
class Shutdown:
    pass


type LifespanEvent = Startup | Shutdown


def _as_reason(value: object) -> str:
    return "" if value is None else narrow_to_str(value)


def _as_websocket_data(message: RawMessage) -> WebsocketData:
    text = message.get("text")
    binary = message.get("bytes")
    if text is not None and binary is not None:
        raise ValueError("websocket message has both text and bytes")
    if text is not None:
        return WebsocketText(text=narrow_to_str(text))
    if binary is not None:
        return WebsocketBinary(data=narrow_to_bytes(binary))
    raise ValueError("websocket message has neither text nor bytes")


def parse_inbound(message: RawMessage) -> Inbound:
    """Classify one inbound `http` event. An unknown event is a protocol fault, so it raises."""
    match message.get("type"):
        case "http.request":
            return RequestBody(
                body=narrow_to_bytes(message.get("body", b"")),
                more_body=bool(message.get("more_body", False)),
            )
        case "http.disconnect":
            return Disconnect()
        case other:
            raise ValueError(f"unexpected http event type: {other!r}")


def parse_websocket_inbound(message: RawMessage) -> WebsocketInbound:
    """Classify one inbound `websocket` event. An unknown event is a protocol fault, so it raises."""
    match message.get("type"):
        case "websocket.connect":
            return WebsocketConnect()
        case "websocket.receive":
            return WebsocketReceive(data=_as_websocket_data(message))
        case "websocket.disconnect":
            return WebsocketDisconnect(
                code=narrow_to_int(message.get("code", 1005)),
                reason=_as_reason(message.get("reason")),
            )
        case other:
            raise ValueError(f"unexpected websocket event type: {other!r}")


def parse_lifespan_event(message: RawMessage) -> LifespanEvent:
    """Classify one `lifespan` event. An unknown event is a protocol fault, so it raises."""
    match message.get("type"):
        case "lifespan.startup":
            return Startup()
        case "lifespan.shutdown":
            return Shutdown()
        case other:
            raise ValueError(f"unexpected lifespan event type: {other!r}")
