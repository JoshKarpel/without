from __future__ import annotations

import pytest
from without_asgi import (
    EarlyHint,
    LifespanReply,
    PathSend,
    RawMessage,
    Response,
    ResponseBody,
    ResponseDebug,
    ResponseStart,
    ResponseTrailers,
    ServerPush,
    ShutdownComplete,
    StartupComplete,
    StartupFailed,
    WebsocketAccept,
    WebsocketBinary,
    WebsocketClose,
    WebsocketResponseBody,
    WebsocketResponseStart,
    WebsocketSend,
    WebsocketText,
    ZeroCopySend,
    encode_lifespan_reply,
    encode_outbound,
    encode_response,
    encode_websocket_outbound,
)


class _FileDescriptor:
    def fileno(self) -> int:
        return 7


def test_encode_outbound_renders_a_response_start() -> None:
    event = ResponseStart(status=404, headers=((b"content-type", b"application/json"),), trailers=False)

    assert encode_outbound(event) == {
        "type": "http.response.start",
        "status": 404,
        "headers": [[b"content-type", b"application/json"]],
    }


def test_encode_outbound_marks_a_response_start_with_trailers() -> None:
    event = ResponseStart(status=200, headers=(), trailers=True)

    assert encode_outbound(event) == {
        "type": "http.response.start",
        "status": 200,
        "headers": [],
        "trailers": True,
    }


def test_encode_outbound_renders_a_response_body() -> None:
    assert encode_outbound(ResponseBody(body=b"hello", more_body=False)) == {
        "type": "http.response.body",
        "body": b"hello",
        "more_body": False,
    }


def test_encode_response_splits_into_start_then_final_body() -> None:
    response = Response(status=200, headers=((b"content-type", b"text/plain"),), body=b"ok")

    assert encode_response(response) == (
        ResponseStart(status=200, headers=((b"content-type", b"text/plain"),), trailers=False),
        ResponseBody(body=b"ok", more_body=False),
    )


def test_encode_outbound_renders_a_server_push() -> None:
    event = ServerPush(path="/style.css", headers=((b"accept", b"text/css"),))

    assert encode_outbound(event) == {
        "type": "http.response.push",
        "path": "/style.css",
        "headers": [[b"accept", b"text/css"]],
    }


def test_encode_outbound_renders_a_zero_copy_send_with_offset_and_count() -> None:
    fd = _FileDescriptor()
    event = ZeroCopySend(file=fd, offset=64, count=2048, more_body=True)

    assert encode_outbound(event) == {
        "type": "http.response.zerocopysend",
        "file": fd,
        "offset": 64,
        "count": 2048,
        "more_body": True,
    }


def test_encode_outbound_omits_zero_copy_offset_and_count_when_absent() -> None:
    fd = _FileDescriptor()

    assert encode_outbound(ZeroCopySend(file=fd, offset=None, count=None, more_body=False)) == {
        "type": "http.response.zerocopysend",
        "file": fd,
        "more_body": False,
    }


def test_encode_outbound_renders_a_path_send() -> None:
    assert encode_outbound(PathSend(path="/var/www/big.iso")) == {
        "type": "http.response.pathsend",
        "path": "/var/www/big.iso",
    }


def test_encode_outbound_renders_an_early_hint() -> None:
    assert encode_outbound(EarlyHint(links=(b"</style.css>; rel=preload",))) == {
        "type": "http.response.early_hint",
        "links": [b"</style.css>; rel=preload"],
    }


def test_encode_outbound_renders_response_trailers() -> None:
    event = ResponseTrailers(headers=((b"digest", b"sha-256=abc"),), more_trailers=False)

    assert encode_outbound(event) == {
        "type": "http.response.trailers",
        "headers": [[b"digest", b"sha-256=abc"]],
        "more_trailers": False,
    }


def test_encode_outbound_renders_response_debug() -> None:
    assert encode_outbound(ResponseDebug(info={"trace_id": "xyz"})) == {
        "type": "http.response.debug",
        "info": {"trace_id": "xyz"},
    }


@pytest.mark.parametrize(
    ("reply", "expected"),
    [
        (StartupComplete(), {"type": "lifespan.startup.complete"}),
        (ShutdownComplete(), {"type": "lifespan.shutdown.complete"}),
        (StartupFailed(message="boom"), {"type": "lifespan.startup.failed", "message": "boom"}),
    ],
)
def test_encode_lifespan_reply(reply: LifespanReply, expected: RawMessage) -> None:
    assert encode_lifespan_reply(reply) == expected


def test_encode_websocket_outbound_renders_an_accept() -> None:
    event = WebsocketAccept(subprotocol="graphql-ws", headers=((b"x-app", b"flags"),))

    assert encode_websocket_outbound(event) == {
        "type": "websocket.accept",
        "subprotocol": "graphql-ws",
        "headers": [[b"x-app", b"flags"]],
    }


def test_encode_websocket_outbound_renders_a_text_send() -> None:
    assert encode_websocket_outbound(WebsocketSend(data=WebsocketText(text="ping"))) == {
        "type": "websocket.send",
        "text": "ping",
    }


def test_encode_websocket_outbound_renders_a_binary_send() -> None:
    assert encode_websocket_outbound(WebsocketSend(data=WebsocketBinary(data=b"\xff"))) == {
        "type": "websocket.send",
        "bytes": b"\xff",
    }


def test_encode_websocket_outbound_renders_a_close() -> None:
    assert encode_websocket_outbound(WebsocketClose(code=1011, reason="boom")) == {
        "type": "websocket.close",
        "code": 1011,
        "reason": "boom",
    }


def test_encode_websocket_outbound_renders_a_denial_response_start() -> None:
    event = WebsocketResponseStart(status=403, headers=((b"content-type", b"text/plain"),))

    assert encode_websocket_outbound(event) == {
        "type": "websocket.http.response.start",
        "status": 403,
        "headers": [[b"content-type", b"text/plain"]],
    }


def test_encode_websocket_outbound_renders_a_denial_response_body() -> None:
    assert encode_websocket_outbound(WebsocketResponseBody(body=b"denied", more_body=False)) == {
        "type": "websocket.http.response.body",
        "body": b"denied",
        "more_body": False,
    }
