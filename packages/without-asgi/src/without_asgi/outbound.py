from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol
from typing import assert_never
from typing import runtime_checkable

from without_asgi.narrow import narrow_to_bytes
from without_asgi.narrow import narrow_to_int
from without_asgi.narrow import narrow_to_str
from without_asgi.types import RawHeaders
from without_asgi.types import RawMessage
from without_asgi.types import WebsocketData
from without_asgi.types import decode_websocket_data
from without_asgi.types import encode_websocket_data


@dataclass(frozen=True, slots=True)
class ResponseStart:
    status: int
    headers: RawHeaders = ()
    trailers: bool = False
    """Whether the app will send a `ResponseTrailers` after the body; requires the
    `http.response.trailers` extension."""


@dataclass(frozen=True, slots=True)
class ResponseBody:
    body: bytes = b""
    more_body: bool = False


@runtime_checkable
class SupportsFileno(Protocol):
    """An opened file object backed by a real OS file descriptor (for `os.sendfile`)."""

    def fileno(self) -> int: ...


@dataclass(frozen=True, slots=True)
class ServerPush:
    """An HTTP/2 server push (`http.response.push` extension)."""

    path: str
    headers: RawHeaders


@dataclass(frozen=True, slots=True)
class ZeroCopySend:
    """
    A zero-copy file-descriptor send (`http.response.zerocopysend` extension).

    The application is responsible for closing `file` afterwards.
    """

    file: SupportsFileno
    offset: int | None = None
    count: int | None = None
    more_body: bool = False


@dataclass(frozen=True, slots=True)
class PathSend:
    """Offload sending a file by absolute path (`http.response.pathsend` extension)."""

    path: str


@dataclass(frozen=True, slots=True)
class EarlyHint:
    """A 103 Early Hints informational response (`http.response.early_hint` extension)."""

    links: tuple[bytes, ...]


@dataclass(frozen=True, slots=True)
class ResponseTrailers:
    """Trailing headers sent after the final body (`http.response.trailers` extension)."""

    headers: RawHeaders
    more_trailers: bool = False


@dataclass(frozen=True, slots=True)
class ResponseDebug:
    """Debug information sent before the response start (`http.response.debug` extension)."""

    info: Mapping[str, object]


type Outbound = (
    ResponseStart | ResponseBody | ServerPush | ZeroCopySend | PathSend | EarlyHint | ResponseTrailers | ResponseDebug
)


@dataclass(frozen=True, slots=True)
class Response:
    """A whole response as one value, the common case behind the event pair."""

    status: int
    headers: RawHeaders = ()
    body: bytes = b""


@dataclass(frozen=True, slots=True)
class WebsocketAccept:
    subprotocol: str | None = None
    headers: RawHeaders = ()


@dataclass(frozen=True, slots=True)
class WebsocketSend:
    data: WebsocketData


@dataclass(frozen=True, slots=True)
class WebsocketClose:
    """
    Close a websocket connection, or reject it when sent before `WebsocketAccept`.

    `code` is a
    [WebSocket close code](https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent/code);
    `1000` is a normal closure. If sent before the handshake is accepted, the
    server discards `code`/`reason` and returns an HTTP `403` instead, so these
    only reach the client on a close *after* accept.
    """

    code: int = 1000
    reason: str = ""


@dataclass(frozen=True, slots=True)
class WebsocketResponseStart:
    """The start of an HTTP denial response (`websocket.http.response` extension)."""

    status: int
    headers: RawHeaders = ()


@dataclass(frozen=True, slots=True)
class WebsocketResponseBody:
    """A body chunk of an HTTP denial response (`websocket.http.response` extension)."""

    body: bytes = b""
    more_body: bool = False


type WebsocketOutbound = (
    WebsocketAccept | WebsocketSend | WebsocketClose | WebsocketResponseStart | WebsocketResponseBody
)


@dataclass(frozen=True, slots=True)
class StartupComplete:
    pass


@dataclass(frozen=True, slots=True)
class ShutdownComplete:
    pass


@dataclass(frozen=True, slots=True)
class StartupFailed:
    message: str = ""


@dataclass(frozen=True, slots=True)
class ShutdownFailed:
    message: str = ""


type LifespanReply = StartupComplete | ShutdownComplete | StartupFailed | ShutdownFailed


def encode_outbound(event: Outbound) -> RawMessage:
    """Render one outbound `http` event as the raw dict an ASGI `send` expects."""
    match event:
        case ResponseStart(status, headers, trailers):
            start: dict[str, object] = {
                "type": "http.response.start",
                "status": status,
                "headers": [[name, value] for name, value in headers],
            }
            if trailers:
                start["trailers"] = True
            return start
        case ResponseBody(body, more_body):
            return {"type": "http.response.body", "body": body, "more_body": more_body}
        case ServerPush(path, headers):
            return {"type": "http.response.push", "path": path, "headers": [[n, v] for n, v in headers]}
        case ZeroCopySend(file, offset, count, more_body):
            zerocopy: dict[str, object] = {"type": "http.response.zerocopysend", "file": file, "more_body": more_body}
            if offset is not None:
                zerocopy["offset"] = offset
            if count is not None:
                zerocopy["count"] = count
            return zerocopy
        case PathSend(path):
            return {"type": "http.response.pathsend", "path": path}
        case EarlyHint(links):
            return {"type": "http.response.early_hint", "links": list(links)}
        case ResponseTrailers(headers, more_trailers):
            return {
                "type": "http.response.trailers",
                "headers": [[n, v] for n, v in headers],
                "more_trailers": more_trailers,
            }
        case ResponseDebug(info):
            return {"type": "http.response.debug", "info": info}
        case _ as unreachable:
            assert_never(unreachable)


def encode_response(response: Response) -> tuple[Outbound, ...]:
    """Split a whole `Response` into its `ResponseStart` then final `ResponseBody`."""
    return (
        ResponseStart(status=response.status, headers=response.headers),
        ResponseBody(body=response.body),
    )


def encode_websocket_outbound(event: WebsocketOutbound) -> RawMessage:
    """Render one outbound `websocket` event as the raw dict an ASGI `send` expects."""
    match event:
        case WebsocketAccept(subprotocol, headers):
            return {
                "type": "websocket.accept",
                "subprotocol": subprotocol,
                "headers": [[n, v] for n, v in headers],
            }
        case WebsocketSend(data):
            return {"type": "websocket.send", **encode_websocket_data(data)}
        case WebsocketClose(code, reason):
            return {"type": "websocket.close", "code": code, "reason": reason}
        case WebsocketResponseStart(status, headers):
            return {
                "type": "websocket.http.response.start",
                "status": status,
                "headers": [[n, v] for n, v in headers],
            }
        case WebsocketResponseBody(body, more_body):
            return {"type": "websocket.http.response.body", "body": body, "more_body": more_body}
        case _ as unreachable:
            assert_never(unreachable)


def encode_lifespan_reply(reply: LifespanReply) -> RawMessage:
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
        case _ as unreachable:
            assert_never(unreachable)


def _as_pair(item: object) -> tuple[bytes, bytes]:
    if isinstance(item, (list, tuple)) and len(item) == 2:
        return narrow_to_bytes(item[0]), narrow_to_bytes(item[1])
    raise TypeError(f"expected a (bytes, bytes) pair, got {item!r}")


def _as_headers(value: object) -> RawHeaders:
    if not isinstance(value, Iterable):
        raise TypeError(f"expected an iterable of bytes pairs, got {type(value).__name__}")
    return tuple(_as_pair(item) for item in value)


def _as_links(value: object) -> tuple[bytes, ...]:
    if not isinstance(value, Iterable):
        raise TypeError(f"expected an iterable of link bytes, got {type(value).__name__}")
    return tuple(narrow_to_bytes(link) for link in value)


def _as_fileno(value: object) -> SupportsFileno:
    if isinstance(value, SupportsFileno):
        return value
    raise TypeError(f"expected a file object with fileno(), got {type(value).__name__}")


def _as_info(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"expected a debug info mapping, got {type(value).__name__}")
    return value


def parse_outbound(message: RawMessage) -> Outbound:
    """
    Classify one outbound `http` event from the raw dict an app passed to `send`.

    The server-direction dual of `encode_outbound`: a transport that owns the wire
    (without-http) reads the dicts the app sends and parses them into typed
    `Outbound` events at the boundary. An unknown event is a protocol fault, so it
    raises.
    """
    match message.get("type"):
        case "http.response.start":
            return ResponseStart(
                status=narrow_to_int(message["status"]),
                headers=_as_headers(message.get("headers", ())),
                trailers=bool(message.get("trailers", False)),
            )
        case "http.response.body":
            return ResponseBody(
                body=narrow_to_bytes(message.get("body", b"")),
                more_body=bool(message.get("more_body", False)),
            )
        case "http.response.push":
            return ServerPush(path=narrow_to_str(message["path"]), headers=_as_headers(message.get("headers", ())))
        case "http.response.zerocopysend":
            return ZeroCopySend(
                file=_as_fileno(message["file"]),
                offset=None if message.get("offset") is None else narrow_to_int(message["offset"]),
                count=None if message.get("count") is None else narrow_to_int(message["count"]),
                more_body=bool(message.get("more_body", False)),
            )
        case "http.response.pathsend":
            return PathSend(path=narrow_to_str(message["path"]))
        case "http.response.early_hint":
            return EarlyHint(links=_as_links(message.get("links", ())))
        case "http.response.trailers":
            return ResponseTrailers(
                headers=_as_headers(message.get("headers", ())),
                more_trailers=bool(message.get("more_trailers", False)),
            )
        case "http.response.debug":
            return ResponseDebug(info=_as_info(message["info"]))
        case other:
            raise ValueError(f"unexpected http event type: {other!r}")


def parse_websocket_outbound(message: RawMessage) -> WebsocketOutbound:
    """Classify one outbound `websocket` event from the raw dict an app passed to `send`."""
    match message.get("type"):
        case "websocket.accept":
            subprotocol = message.get("subprotocol")
            return WebsocketAccept(
                subprotocol=None if subprotocol is None else narrow_to_str(subprotocol),
                headers=_as_headers(message.get("headers", ())),
            )
        case "websocket.send":
            return WebsocketSend(data=decode_websocket_data(message))
        case "websocket.close":
            return WebsocketClose(
                code=narrow_to_int(message.get("code", 1000)),
                reason=narrow_to_str(message.get("reason", "")),
            )
        case "websocket.http.response.start":
            return WebsocketResponseStart(
                status=narrow_to_int(message["status"]),
                headers=_as_headers(message.get("headers", ())),
            )
        case "websocket.http.response.body":
            return WebsocketResponseBody(
                body=narrow_to_bytes(message.get("body", b"")),
                more_body=bool(message.get("more_body", False)),
            )
        case other:
            raise ValueError(f"unexpected websocket event type: {other!r}")


def parse_lifespan_reply(message: RawMessage) -> LifespanReply:
    """Classify one `lifespan` reply from the raw dict an app passed to `send`."""
    match message.get("type"):
        case "lifespan.startup.complete":
            return StartupComplete()
        case "lifespan.shutdown.complete":
            return ShutdownComplete()
        case "lifespan.startup.failed":
            return StartupFailed(message=narrow_to_str(message.get("message", "")))
        case "lifespan.shutdown.failed":
            return ShutdownFailed(message=narrow_to_str(message.get("message", "")))
        case other:
            raise ValueError(f"unexpected lifespan reply type: {other!r}")
