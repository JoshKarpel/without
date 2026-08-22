from __future__ import annotations

import json
from collections.abc import AsyncIterator

import pytest
from without_asgi import Content
from without_asgi import EarlyHint
from without_asgi import FilePart
from without_asgi import LifespanReply
from without_asgi import Outbound
from without_asgi import PathSend
from without_asgi import RawHeaders
from without_asgi import RawMessage
from without_asgi import Response
from without_asgi import ResponseBody
from without_asgi import ResponseDebug
from without_asgi import ResponseStart
from without_asgi import ResponseTrailers
from without_asgi import ServerPush
from without_asgi import ShutdownComplete
from without_asgi import ShutdownFailed
from without_asgi import StartupComplete
from without_asgi import StartupFailed
from without_asgi import WebsocketAccept
from without_asgi import WebsocketBinary
from without_asgi import WebsocketClose
from without_asgi import WebsocketOutbound
from without_asgi import WebsocketResponseBody
from without_asgi import WebsocketResponseStart
from without_asgi import WebsocketSend
from without_asgi import WebsocketText
from without_asgi import ZeroCopySend
from without_asgi import encode_lifespan_reply
from without_asgi import encode_outbound
from without_asgi import encode_response
from without_asgi import encode_websocket_outbound
from without_asgi import form_content
from without_asgi import html_content
from without_asgi import json_content
from without_asgi import multipart_content
from without_asgi import parse_lifespan_reply
from without_asgi import parse_outbound
from without_asgi import parse_websocket_outbound

from .helpers import FileDescriptor


def test_encode_outbound_renders_a_response_start() -> None:
    event = ResponseStart(status=404, headers=((b"content-type", b"application/json"),), trailers=False)

    assert encode_outbound(event) == {
        "type": "http.response.start",
        "status": 404,
        "headers": ((b"content-type", b"application/json"),),
    }


def test_encode_outbound_marks_a_response_start_with_trailers() -> None:
    event = ResponseStart(status=200, headers=(), trailers=True)

    assert encode_outbound(event) == {
        "type": "http.response.start",
        "status": 200,
        "headers": (),
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


def test_json_content_pairs_the_encoding_with_the_content_type() -> None:
    content = json_content({"title": "write", "done": False})

    assert content == Content(b'{"title": "write", "done": false}', ((b"content-type", b"application/json"),))


def test_json_content_takes_the_encoder_as_the_whole_policy() -> None:
    content = json_content({"b": 2, "a": 1}, dumps=lambda payload: json.dumps(payload, sort_keys=True))

    assert content.body == b'{"a": 1, "b": 2}'


def test_json_content_refuses_a_payload_that_is_not_json() -> None:
    # `NaN` encodes to a token no strict parser accepts, so it fails at the sender rather
    # than at whoever reads the response.
    with pytest.raises(ValueError, match="Out of range float"):
        json_content({"ratio": float("nan")})


def test_form_content_pairs_the_encoding_with_the_content_type() -> None:
    content = form_content({"grant_type": "client_credentials", "scope": "read write"})

    assert content == Content(
        b"grant_type=client_credentials&scope=read+write",
        ((b"content-type", b"application/x-www-form-urlencoded"),),
    )


def test_form_content_takes_pairs_when_a_name_repeats() -> None:
    content = form_content([("tag", "alpha"), ("tag", "beta")])

    assert content.body == b"tag=alpha&tag=beta"


def test_form_content_percent_encodes_utf8() -> None:
    content = form_content({"name": "café"})

    assert content.body == b"name=caf%C3%A9"


def test_html_content_pairs_the_markup_with_the_content_type() -> None:
    content = html_content("<p>hello</p>")

    assert content == Content(b"<p>hello</p>", ((b"content-type", b"text/html; charset=utf-8"),))


def test_html_content_encodes_as_utf8() -> None:
    content = html_content("<p>café</p>")

    assert content.body == "<p>café</p>".encode()


async def test_multipart_content_frames_fields_and_files_with_the_boundary() -> None:
    content = multipart_content(
        {"purpose": "assistants"},
        [FilePart(name="upload", filename="notes.txt", body=b"line one", content_type=b"text/plain")],
        boundary=b"fixedboundary",
    )

    assert await content.buffered() == Content(
        b"--fixedboundary\r\n"
        b'content-disposition: form-data; name="purpose"\r\n'
        b"\r\n"
        b"assistants\r\n"
        b"--fixedboundary\r\n"
        b'content-disposition: form-data; name="upload"; filename="notes.txt"\r\n'
        b"content-type: text/plain\r\n"
        b"\r\n"
        b"line one\r\n"
        b"--fixedboundary--\r\n",
        ((b"content-type", b"multipart/form-data; boundary=fixedboundary"),),
    )


async def test_multipart_content_streams_a_file_part_between_its_framing() -> None:
    async def file_chunks() -> AsyncIterator[bytes]:
        yield b"first chunk "
        yield b"second chunk"

    content = multipart_content(
        files=[FilePart(name="upload", filename="big.bin", body=file_chunks())],
        boundary=b"bb",
    )

    assert (await content.buffered()).body == (
        b"--bb\r\n"
        b'content-disposition: form-data; name="upload"; filename="big.bin"\r\n'
        b"content-type: application/octet-stream\r\n"
        b"\r\n"
        b"first chunk second chunk\r\n"
        b"--bb--\r\n"
    )


async def test_multipart_content_defaults_a_file_part_to_octet_stream() -> None:
    content = multipart_content(files=[FilePart(name="blob", filename="a.bin", body=b"\x01")], boundary=b"bb")

    assert b"content-type: application/octet-stream\r\n" in (await content.buffered()).body


async def test_multipart_content_generates_a_boundary_named_in_the_content_type() -> None:
    content = multipart_content({"field": "value"})

    ((_, content_type),) = content.headers
    boundary = content_type.removeprefix(b"multipart/form-data; boundary=")
    assert boundary
    body = (await content.buffered()).body
    assert body.startswith(b"--" + boundary + b"\r\n")
    assert body.endswith(b"--" + boundary + b"--\r\n")
    ((_, second_content_type),) = multipart_content({"field": "value"}).headers
    assert boundary != second_content_type.removeprefix(b"multipart/form-data; boundary=")


async def test_multipart_content_escapes_quotes_and_newlines_in_names() -> None:
    content = multipart_content(
        {'na"me': "value"},
        [FilePart(name="file", filename='evil"\r\n.txt', body=b"x")],
        boundary=b"bb",
    )

    body = (await content.buffered()).body
    assert b'name="na%22me"' in body
    assert b'filename="evil%22%0D%0A.txt"' in body


def test_response_from_content_carries_the_body_and_its_headers() -> None:
    response = Response.from_content(201, json_content({"id": 1}))

    assert response == Response(status=201, headers=((b"content-type", b"application/json"),), body=b'{"id": 1}')


def test_response_from_content_lets_the_caller_override_what_the_content_described() -> None:
    extra: RawHeaders = ((b"content-type", b"application/problem+json"), (b"x-request-id", b"r-7"))

    response = Response.from_content(422, json_content({"error": "nope"}), headers=extra)

    assert response.headers == extra
    assert response.body == b'{"error": "nope"}'


def test_encode_outbound_renders_a_server_push() -> None:
    event = ServerPush(path="/style.css", headers=((b"accept", b"text/css"),))

    assert encode_outbound(event) == {
        "type": "http.response.push",
        "path": "/style.css",
        "headers": ((b"accept", b"text/css"),),
    }


def test_encode_outbound_renders_a_zero_copy_send_with_offset_and_count() -> None:
    fd = FileDescriptor()
    event = ZeroCopySend(file=fd, offset=64, count=2048, more_body=True)

    assert encode_outbound(event) == {
        "type": "http.response.zerocopysend",
        "file": fd,
        "offset": 64,
        "count": 2048,
        "more_body": True,
    }


def test_encode_outbound_omits_zero_copy_offset_and_count_when_absent() -> None:
    fd = FileDescriptor()

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
        "headers": ((b"digest", b"sha-256=abc"),),
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
        "headers": ((b"x-app", b"flags"),),
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
        "headers": ((b"content-type", b"text/plain"),),
    }


def test_encode_websocket_outbound_renders_a_denial_response_body() -> None:
    assert encode_websocket_outbound(WebsocketResponseBody(body=b"denied", more_body=False)) == {
        "type": "websocket.http.response.body",
        "body": b"denied",
        "more_body": False,
    }


def test_parse_outbound_classifies_a_response_start() -> None:
    message: RawMessage = {
        "type": "http.response.start",
        "status": 201,
        "headers": [[b"content-type", b"application/json"]],
        "trailers": True,
    }

    assert parse_outbound(message) == ResponseStart(
        status=201, headers=((b"content-type", b"application/json"),), trailers=True
    )


def test_parse_outbound_rejects_an_unknown_event() -> None:
    with pytest.raises(ValueError, match="unexpected http event"):
        parse_outbound({"type": "http.response.surprise"})


@pytest.mark.parametrize(
    ("message", "match"),
    [
        (
            {"type": "http.response.start", "status": 200, "headers": [[b"lonely"]]},
            "expected a \\(bytes, bytes\\) pair",
        ),
        ({"type": "http.response.start", "status": 200, "headers": 17}, "expected an iterable of bytes pairs"),
        ({"type": "http.response.zerocopysend", "file": "not-a-file"}, "expected a file object with fileno"),
        ({"type": "http.response.debug", "info": "not-a-mapping"}, "expected a debug info mapping"),
    ],
)
def test_parse_outbound_rejects_a_malformed_field(message: RawMessage, match: str) -> None:
    with pytest.raises(TypeError, match=match):
        parse_outbound(message)


def test_parse_websocket_outbound_rejects_an_unknown_event() -> None:
    with pytest.raises(ValueError, match="unexpected websocket event"):
        parse_websocket_outbound({"type": "websocket.surprise"})


def test_parse_lifespan_reply_rejects_an_unknown_reply() -> None:
    with pytest.raises(ValueError, match="unexpected lifespan reply"):
        parse_lifespan_reply({"type": "lifespan.surprise"})


@pytest.mark.parametrize(
    "event",
    [
        ResponseStart(status=200, headers=((b"x-app", b"flags"),), trailers=False),
        ResponseStart(status=204, headers=(), trailers=True),
        ResponseBody(body=b"hello", more_body=True),
        ResponseBody(body=b"", more_body=False),
        ServerPush(path="/style.css", headers=((b"accept", b"text/css"),)),
        ZeroCopySend(file=FileDescriptor(), offset=64, count=2048, more_body=True),
        ZeroCopySend(file=FileDescriptor(), offset=None, count=None, more_body=False),
        PathSend(path="/var/www/big.iso"),
        EarlyHint(links=(b"</style.css>; rel=preload",)),
        ResponseTrailers(headers=((b"digest", b"sha-256=abc"),), more_trailers=False),
        ResponseDebug(info={"trace_id": "xyz"}),
    ],
)
def test_parse_outbound_round_trips_through_encode_outbound(event: Outbound) -> None:
    assert parse_outbound(encode_outbound(event)) == event


@pytest.mark.parametrize(
    "event",
    [
        WebsocketAccept(subprotocol="graphql-ws", headers=((b"x-app", b"flags"),)),
        WebsocketAccept(subprotocol=None, headers=()),
        WebsocketSend(data=WebsocketText(text="ping")),
        WebsocketSend(data=WebsocketBinary(data=b"\xff")),
        WebsocketClose(code=1011, reason="boom"),
        WebsocketResponseStart(status=403, headers=((b"content-type", b"text/plain"),)),
        WebsocketResponseBody(body=b"denied", more_body=False),
    ],
)
def test_parse_websocket_outbound_round_trips_through_encode(event: WebsocketOutbound) -> None:
    assert parse_websocket_outbound(encode_websocket_outbound(event)) == event


@pytest.mark.parametrize(
    "reply",
    [StartupComplete(), ShutdownComplete(), StartupFailed(message="boom"), ShutdownFailed(message="late")],
)
def test_parse_lifespan_reply_round_trips_through_encode(reply: LifespanReply) -> None:
    assert parse_lifespan_reply(encode_lifespan_reply(reply)) == reply


@pytest.mark.parametrize(
    ("message", "match"),
    [
        (
            {"type": "http.response.zerocopysend", "file": "not-a-file"},
            r"^expected a file object with fileno\(\), got str$",
        ),
        ({"type": "http.response.debug", "info": "not-a-mapping"}, r"^expected a debug info mapping, got str$"),
        ({"type": "http.response.early_hint", "links": 42}, r"^expected an iterable of link bytes, got int$"),
    ],
)
def test_parse_outbound_type_error_names_the_offending_type(message: RawMessage, match: str) -> None:
    with pytest.raises(TypeError, match=match):
        parse_outbound(message)


def test_parse_outbound_defaults_response_start_headers_to_empty() -> None:
    assert parse_outbound({"type": "http.response.start", "status": 207}) == ResponseStart(
        status=207, headers=(), trailers=False
    )


def test_parse_outbound_defaults_response_body_to_empty_bytes() -> None:
    assert parse_outbound({"type": "http.response.body"}) == ResponseBody(body=b"", more_body=False)


def test_parse_outbound_defaults_zerocopysend_more_body_to_false() -> None:
    fd = FileDescriptor()

    assert parse_outbound({"type": "http.response.zerocopysend", "file": fd}) == ZeroCopySend(
        file=fd, offset=None, count=None, more_body=False
    )


def test_parse_outbound_defaults_server_push_headers_to_empty() -> None:
    assert parse_outbound({"type": "http.response.push", "path": "/main.js"}) == ServerPush(path="/main.js", headers=())


def test_parse_outbound_defaults_early_hint_links_to_empty() -> None:
    assert parse_outbound({"type": "http.response.early_hint"}) == EarlyHint(links=())


def test_parse_outbound_defaults_response_trailers_headers_to_empty() -> None:
    assert parse_outbound({"type": "http.response.trailers"}) == ResponseTrailers(headers=(), more_trailers=False)


def test_parse_outbound_reads_response_trailers_more_trailers_flag() -> None:
    message: RawMessage = {
        "type": "http.response.trailers",
        "headers": [[b"digest", b"sha-256=xyz"]],
        "more_trailers": True,
    }

    assert parse_outbound(message) == ResponseTrailers(headers=((b"digest", b"sha-256=xyz"),), more_trailers=True)


def test_parse_websocket_outbound_defaults_accept_headers_to_empty() -> None:
    assert parse_websocket_outbound({"type": "websocket.accept"}) == WebsocketAccept(subprotocol=None, headers=())


def test_parse_websocket_outbound_defaults_denial_start_headers_to_empty() -> None:
    assert parse_websocket_outbound({"type": "websocket.http.response.start", "status": 502}) == WebsocketResponseStart(
        status=502, headers=()
    )


def test_parse_websocket_outbound_defaults_close_code_and_reason() -> None:
    assert parse_websocket_outbound({"type": "websocket.close"}) == WebsocketClose(code=1000, reason="")


def test_parse_websocket_outbound_defaults_denial_body_to_empty_bytes() -> None:
    assert parse_websocket_outbound({"type": "websocket.http.response.body"}) == WebsocketResponseBody(
        body=b"", more_body=False
    )


def test_parse_websocket_outbound_reads_denial_body_more_body_flag() -> None:
    message: RawMessage = {"type": "websocket.http.response.body", "body": b"partial", "more_body": True}

    assert parse_websocket_outbound(message) == WebsocketResponseBody(body=b"partial", more_body=True)


def test_parse_lifespan_reply_defaults_startup_failed_message_to_empty() -> None:
    assert parse_lifespan_reply({"type": "lifespan.startup.failed"}) == StartupFailed(message="")


def test_parse_lifespan_reply_defaults_shutdown_failed_message_to_empty() -> None:
    assert parse_lifespan_reply({"type": "lifespan.shutdown.failed"}) == ShutdownFailed(message="")
