from __future__ import annotations

from dataclasses import dataclass
from typing import assert_never

from without_asgi.narrow import narrow
from without_asgi.types import RawMessage
from without_asgi.types import WebsocketData
from without_asgi.types import decode_websocket_data
from without_asgi.types import encode_websocket_data


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
    return "" if value is None else narrow(value, str)


def parse_inbound(message: RawMessage) -> Inbound:
    """Classify one inbound `http` event. An unknown event is a protocol fault, so it raises."""
    match message.get("type"):
        case "http.request":
            value = message.get("body", b"")
            more_body = bool(message.get("more_body", False))  # pragma: no mutate - None default is falsy like False
            return RequestBody(
                body=narrow(value, bytes),
                more_body=more_body,
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
            return WebsocketReceive(data=decode_websocket_data(message))
        case "websocket.disconnect":
            value = message.get("code", 1005)
            return WebsocketDisconnect(
                code=narrow(value, int),
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


def encode_inbound(event: Inbound) -> RawMessage:
    """
    Render one inbound `http` event as the raw dict an ASGI `receive` returns.

    The server-direction dual of `parse_inbound`: a transport that owns the wire
    (without-http) builds typed `Inbound` events and hands them to the app as the
    dicts ASGI `receive` yields.
    """
    match event:
        case RequestBody(body, more_body):
            return {"type": "http.request", "body": body, "more_body": more_body}
        case Disconnect():
            return {"type": "http.disconnect"}
        case _ as unreachable:
            assert_never(unreachable)


def encode_websocket_inbound(event: WebsocketInbound) -> RawMessage:
    """Render one inbound `websocket` event as the raw dict an ASGI `receive` returns."""
    match event:
        case WebsocketConnect():
            return {"type": "websocket.connect"}
        case WebsocketReceive(data):
            return {"type": "websocket.receive", **encode_websocket_data(data)}
        case WebsocketDisconnect(code, reason):
            return {"type": "websocket.disconnect", "code": code, "reason": reason}
        case _ as unreachable:
            assert_never(unreachable)


def encode_lifespan_event(event: LifespanEvent) -> RawMessage:
    """Render one `lifespan` event as the raw dict an ASGI `receive` returns."""
    match event:
        case Startup():
            return {"type": "lifespan.startup"}
        case Shutdown():
            return {"type": "lifespan.shutdown"}
        case _ as unreachable:
            assert_never(unreachable)
