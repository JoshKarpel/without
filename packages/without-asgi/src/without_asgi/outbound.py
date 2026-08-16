from __future__ import annotations

import json
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol
from typing import assert_never
from typing import runtime_checkable

from without_asgi.headers import merge
from without_asgi.narrow import narrow
from without_asgi.types import RawHeaders
from without_asgi.types import RawMessage
from without_asgi.types import WebsocketData
from without_asgi.types import decode_websocket_data
from without_asgi.types import encode_websocket_data
from without_asgi.types import narrow_headers


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
class Content:
    """
    A body and the headers that describe it: what a caller holds when it has a *value*
    rather than bytes.

    Encoding a value produces two things that must travel together, the bytes and the
    `content-type` naming what they are, and every caller that separates them gets to
    make the same mistake. This pairs them without deciding either: `Content` carries no
    policy, so `json_content` is one producer of it and a form, text, or msgpack encoder
    is another, all with equal standing.

    The body is `bytes` because a `Content` is a value the caller already holds whole.
    Streaming a body is a different situation (nothing to describe yet, and no length),
    and `without-http`'s `request` takes a `Stream[bytes]` directly for it.
    """

    body: bytes
    headers: RawHeaders = ()


@dataclass(frozen=True, slots=True)
class Response:
    """A whole response as one value, the common case behind the event pair."""

    status: int
    headers: RawHeaders = ()
    body: bytes = b""

    @classmethod
    def from_content(cls, status: int, content: Content, *, headers: RawHeaders = ()) -> Response:
        """
        A response carrying `content`, with `headers` layered over the ones it describes itself with.

        The caller wins on any name the content also sets, so a handler that wants
        `content-type: application/problem+json` over a JSON body says so here rather
        than rebuilding the body.
        """
        return cls(status=status, headers=merge(content.headers, headers), body=content.body)


JSON_MEDIA_TYPE = b"application/json"


def _dumps(payload: object) -> str:
    # `allow_nan` off because `NaN`/`Infinity` are not JSON: left on, a body encodes to
    # tokens a strict parser at the other end rejects, which is a failure the receiver
    # reports rather than the sender. Key order is left alone, since sorting is a policy
    # (a caller who wants byte-identical bodies passes its own `dumps`) and one paid on
    # every response.
    return json.dumps(payload, allow_nan=False)


def json_content(payload: object, *, dumps: Callable[[object], str] = _dumps) -> Content:
    """
    Encode `payload` as a JSON `Content`: the bytes plus `content-type: application/json`.

    `dumps` is the whole encoding policy, injected rather than fixed, so an app that
    needs sorted keys, a faster encoder, or one that knows its domain types passes its
    own (`json_content(order, dumps=orjson_dumps)`). The default is the stdlib's, because
    a default should add no dependency; what it costs is that a payload must be
    JSON-native, and a value the stdlib encoder has never heard of raises here rather
    than reaching the wire half-written.

    JSON ships as a function here, where the library otherwise leaves encoding to the
    app, because it is the one encoding *both* sides of this stack kept re-deriving:
    the same three lines (serializer, encode, content type) appeared in every app,
    helper, and test that answered or sent a JSON body.
    """
    return Content(dumps(payload).encode(), ((b"content-type", JSON_MEDIA_TYPE),))


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
                "headers": headers,
            }
            if trailers:
                start["trailers"] = True
            return start
        case ResponseBody(body, more_body):
            return {"type": "http.response.body", "body": body, "more_body": more_body}
        case ServerPush(path, headers):
            return {"type": "http.response.push", "path": path, "headers": headers}
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
                "headers": headers,
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
                "headers": headers,
            }
        case WebsocketSend(data):
            return {"type": "websocket.send", **encode_websocket_data(data)}
        case WebsocketClose(code, reason):
            return {"type": "websocket.close", "code": code, "reason": reason}
        case WebsocketResponseStart(status, headers):
            return {
                "type": "websocket.http.response.start",
                "status": status,
                "headers": headers,
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


def _as_links(value: object) -> tuple[bytes, ...]:
    if not isinstance(value, Iterable):
        raise TypeError(f"expected an iterable of link bytes, got {type(value).__name__}")
    return tuple(narrow(link, bytes) for link in value)


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
            trailers = bool(message.get("trailers", False))  # pragma: no mutate - None default is falsy like False
            return ResponseStart(
                status=narrow(message["status"], int),
                headers=narrow_headers(message.get("headers", ())),
                trailers=trailers,
            )
        case "http.response.body":
            more_body = bool(message.get("more_body", False))  # pragma: no mutate - None default is falsy like False
            return ResponseBody(
                body=narrow(message.get("body", b""), bytes),
                more_body=more_body,
            )
        case "http.response.push":
            return ServerPush(path=narrow(message["path"], str), headers=narrow_headers(message.get("headers", ())))
        case "http.response.zerocopysend":
            more_body = bool(message.get("more_body", False))  # pragma: no mutate - None default is falsy like False
            return ZeroCopySend(
                file=_as_fileno(message["file"]),
                offset=None if message.get("offset") is None else narrow(message["offset"], int),
                count=None if message.get("count") is None else narrow(message["count"], int),
                more_body=more_body,
            )
        case "http.response.pathsend":
            return PathSend(path=narrow(message["path"], str))
        case "http.response.early_hint":
            return EarlyHint(links=_as_links(message.get("links", ())))
        case "http.response.trailers":
            more_trailers = bool(message.get("more_trailers", False))  # pragma: no mutate - None default is falsy
            return ResponseTrailers(
                headers=narrow_headers(message.get("headers", ())),
                more_trailers=more_trailers,
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
                subprotocol=None if subprotocol is None else narrow(subprotocol, str),
                headers=narrow_headers(message.get("headers", ())),
            )
        case "websocket.send":
            return WebsocketSend(data=decode_websocket_data(message))
        case "websocket.close":
            return WebsocketClose(
                code=narrow(message.get("code", 1000), int),
                reason=narrow(message.get("reason", ""), str),
            )
        case "websocket.http.response.start":
            return WebsocketResponseStart(
                status=narrow(message["status"], int),
                headers=narrow_headers(message.get("headers", ())),
            )
        case "websocket.http.response.body":
            more_body = bool(message.get("more_body", False))  # pragma: no mutate - None default is falsy like False
            return WebsocketResponseBody(
                body=narrow(message.get("body", b""), bytes),
                more_body=more_body,
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
            value = message.get("message", "")
            return StartupFailed(message=narrow(value, str))
        case "lifespan.shutdown.failed":
            value1 = message.get("message", "")
            return ShutdownFailed(message=narrow(value1, str))
        case other:
            raise ValueError(f"unexpected lifespan reply type: {other!r}")
