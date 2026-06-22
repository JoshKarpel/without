from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass

# The raw ASGI surface, hand-rolled to keep this package's only runtime
# dependency `without`. These mirror the shapes an ASGI server passes to an
# application callable; the typed values below are what the boundary parses
# them into.
type Scope = Mapping[str, object]
type Message = Mapping[str, object]
type Receive = Callable[[], Awaitable[Message]]
type Send = Callable[[Message], Awaitable[None]]
type ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class HttpScope:
    """The per-request connection facts, known once when the request opens."""

    method: str
    path: str
    headers: tuple[tuple[bytes, bytes], ...]
    query_string: bytes


@dataclass(frozen=True, slots=True)
class RequestBody:
    body: bytes
    more_body: bool


@dataclass(frozen=True, slots=True)
class Disconnect:
    """The client went away before the request finished."""


type Inbound = RequestBody | Disconnect


@dataclass(frozen=True, slots=True)
class ResponseStart:
    status: int
    headers: tuple[tuple[bytes, bytes], ...]


@dataclass(frozen=True, slots=True)
class ResponseBody:
    body: bytes
    more_body: bool


type Outbound = ResponseStart | ResponseBody


@dataclass(frozen=True, slots=True)
class Response:
    """A whole response as one value, the common case behind the event pair."""

    status: int
    headers: tuple[tuple[bytes, bytes], ...]
    body: bytes


@dataclass(frozen=True, slots=True)
class Startup:
    pass


@dataclass(frozen=True, slots=True)
class Shutdown:
    pass


type LifespanEvent = Startup | Shutdown


@dataclass(frozen=True, slots=True)
class StartupComplete:
    pass


@dataclass(frozen=True, slots=True)
class ShutdownComplete:
    pass


@dataclass(frozen=True, slots=True)
class StartupFailed:
    message: str


@dataclass(frozen=True, slots=True)
class ShutdownFailed:
    message: str


type LifespanReply = StartupComplete | ShutdownComplete | StartupFailed | ShutdownFailed


def _as_str(value: object) -> str:
    if isinstance(value, str):
        return value
    raise TypeError(f"expected str, got {type(value).__name__}")


def _as_bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    raise TypeError(f"expected bytes, got {type(value).__name__}")


def _as_pair(item: object) -> tuple[bytes, bytes]:
    if isinstance(item, (list, tuple)) and len(item) == 2:
        return (_as_bytes(item[0]), _as_bytes(item[1]))
    raise TypeError(f"expected a (name, value) pair, got {item!r}")


def _as_headers(value: object) -> tuple[tuple[bytes, bytes], ...]:
    if not isinstance(value, Iterable):
        raise TypeError(f"expected an iterable of header pairs, got {type(value).__name__}")
    return tuple(_as_pair(item) for item in value)


def parse_http_scope(scope: Scope) -> HttpScope:
    """Read an `http` scope into the typed connection facts, validating at the boundary."""
    return HttpScope(
        method=_as_str(scope["method"]),
        path=_as_str(scope["path"]),
        headers=_as_headers(scope.get("headers", ())),
        query_string=_as_bytes(scope.get("query_string", b"")),
    )


def parse_inbound(message: Message) -> Inbound:
    """Classify one inbound `http` event. An unknown event is a protocol fault, so it raises."""
    match message.get("type"):
        case "http.request":
            return RequestBody(
                body=_as_bytes(message.get("body", b"")),
                more_body=bool(message.get("more_body", False)),
            )
        case "http.disconnect":
            return Disconnect()
        case other:
            raise ValueError(f"unexpected http event type: {other!r}")


def encode_outbound(event: Outbound) -> Message:
    """Render one outbound event as the raw dict an ASGI `send` expects."""
    match event:
        case ResponseStart(status, headers):
            return {
                "type": "http.response.start",
                "status": status,
                "headers": [[name, value] for name, value in headers],
            }
        case ResponseBody(body, more_body):
            return {"type": "http.response.body", "body": body, "more_body": more_body}


def encode_response(response: Response) -> tuple[Outbound, ...]:
    """Split a whole `Response` into its `ResponseStart` then final `ResponseBody`."""
    return (
        ResponseStart(status=response.status, headers=response.headers),
        ResponseBody(body=response.body, more_body=False),
    )


def parse_lifespan_event(message: Message) -> LifespanEvent:
    """Classify one `lifespan` event. An unknown event is a protocol fault, so it raises."""
    match message.get("type"):
        case "lifespan.startup":
            return Startup()
        case "lifespan.shutdown":
            return Shutdown()
        case other:
            raise ValueError(f"unexpected lifespan event type: {other!r}")


def encode_lifespan_reply(reply: LifespanReply) -> Message:
    """Render one lifespan reply as the raw dict an ASGI `send` expects."""
    match reply:
        case StartupComplete():
            return {"type": "lifespan.startup.complete"}
        case ShutdownComplete():
            return {"type": "lifespan.shutdown.complete"}
        case StartupFailed(message):
            return {"type": "lifespan.startup.failed", "message": message}
        case ShutdownFailed(message):
            return {"type": "lifespan.shutdown.failed", "message": message}


def scope_type(scope: Scope) -> str:
    """The `type` discriminator of any scope (`http`, `websocket`, `lifespan`)."""
    return _as_str(scope["type"])
