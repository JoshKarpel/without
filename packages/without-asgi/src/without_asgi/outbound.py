from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from without_asgi.types import RawHeaders
from without_asgi.types import RawMessage
from without_asgi.types import WebsocketBinary
from without_asgi.types import WebsocketData
from without_asgi.types import WebsocketText


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
    """A zero-copy file-descriptor send (`http.response.zerocopysend` extension).

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
    """Close a websocket connection, or reject it when sent before `WebsocketAccept`.

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


def encode_response(response: Response) -> tuple[Outbound, ...]:
    """Split a whole `Response` into its `ResponseStart` then final `ResponseBody`."""
    return (
        ResponseStart(status=response.status, headers=response.headers),
        ResponseBody(body=response.body),
    )


def _encode_websocket_data(data: WebsocketData) -> dict[str, object]:
    match data:
        case WebsocketText(text):
            return {"text": text}
        case WebsocketBinary(binary):
            return {"bytes": binary}


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
            return {"type": "websocket.send", **_encode_websocket_data(data)}
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
