from __future__ import annotations

from typing import assert_never
from urllib.parse import unquote

import h11
from without_asgi import WebsocketAccept
from without_asgi import WebsocketBinary
from without_asgi import WebsocketClose
from without_asgi import WebsocketOutbound
from without_asgi import WebsocketResponseBody
from without_asgi import WebsocketResponseStart
from without_asgi import WebsocketScope
from without_asgi import WebsocketSend
from without_asgi import WebsocketText
from wsproto.events import AcceptConnection
from wsproto.events import BytesMessage
from wsproto.events import CloseConnection
from wsproto.events import Event
from wsproto.events import RejectConnection
from wsproto.events import RejectData
from wsproto.events import TextMessage

from without_http.h11_wire import ASGI


def is_websocket_upgrade(request: h11.Request) -> bool:
    """Whether an `h11.Request` is a WebSocket handshake (`Upgrade: websocket`)."""
    return any(name.lower() == b"upgrade" and b"websocket" in value.lower() for name, value in request.headers)


def _subprotocols(request: h11.Request) -> tuple[str, ...]:
    for name, value in request.headers:
        if name.lower() == b"sec-websocket-protocol":
            tokens = [token.strip() for token in value.split(b",") if token.strip()]
            return tuple(token.decode("ascii") for token in tokens)  # pragma: no mutate - codec case
    return ()


def websocket_scope_from_request(
    request: h11.Request,
    *,
    scheme: str,
    server: tuple[str, int | None] | None,
    client: tuple[str, int] | None,
) -> WebsocketScope:
    """Build the typed `WebsocketScope` an ASGI app expects from the handshake `h11.Request`."""
    raw_path, _, query_string = request.target.partition(b"?")
    headers = tuple((bytes(name), bytes(value)) for name, value in request.headers)
    http_version = request.http_version.decode("ascii")  # pragma: no mutate - codec case
    path = unquote(raw_path.decode("ascii"))  # pragma: no mutate - codec case
    return WebsocketScope(
        asgi=ASGI,
        http_version=http_version,
        scheme=scheme,
        path=path,
        raw_path=raw_path,
        query_string=query_string,
        root_path="",
        headers=headers,
        client=client,
        server=server,
        subprotocols=_subprotocols(request),
        extensions=None,
    )


def ws_events_from_outbound(outbound: WebsocketOutbound, *, accepted: bool) -> list[Event]:
    """
    Render one typed `WebsocketOutbound` as the `wsproto` events that put it on the wire.

    `accepted` distinguishes the two meanings of a `WebsocketClose`: before the
    handshake is accepted it is a *rejection* (an HTTP response, here a `403`);
    after, it is a normal close frame. This mirrors the ASGI contract that a close
    sent before `websocket.accept` becomes an HTTP denial.
    """
    match outbound:
        case WebsocketAccept(subprotocol, headers):
            return [AcceptConnection(subprotocol=subprotocol, extra_headers=[(n, v) for n, v in headers])]
        case WebsocketSend(data):
            return [_data_message(data)]
        case WebsocketClose(code, reason):
            if accepted:
                return [CloseConnection(code=code, reason=reason)]
            return [RejectConnection(status_code=403)]
        case WebsocketResponseStart(status, headers):
            return [RejectConnection(status_code=status, headers=[(n, v) for n, v in headers], has_body=True)]
        case WebsocketResponseBody(body, more_body):
            return [RejectData(data=body, body_finished=not more_body)]
        case _ as unreachable:
            assert_never(unreachable)


def _data_message(data: WebsocketText | WebsocketBinary) -> TextMessage | BytesMessage:
    match data:
        case WebsocketText(text):
            return TextMessage(data=text)
        case WebsocketBinary(binary):
            return BytesMessage(data=binary)
        case _ as unreachable:
            assert_never(unreachable)
