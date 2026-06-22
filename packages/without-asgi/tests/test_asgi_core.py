from __future__ import annotations

import pytest
from without_asgi.core import (
    Disconnect,
    HttpScope,
    LifespanReply,
    Message,
    RequestBody,
    Response,
    ResponseBody,
    ResponseStart,
    Shutdown,
    ShutdownComplete,
    Startup,
    StartupComplete,
    StartupFailed,
    encode_lifespan_reply,
    encode_outbound,
    encode_response,
    parse_http_scope,
    parse_inbound,
    parse_lifespan_event,
)


def test_parse_http_scope_reads_the_connection_facts() -> None:
    scope: Message = {
        "type": "http",
        "method": "POST",
        "path": "/flags",
        "headers": [[b"accept", b"application/json"]],
        "query_string": b"name=dark_mode",
    }

    assert parse_http_scope(scope) == HttpScope(
        method="POST",
        path="/flags",
        headers=((b"accept", b"application/json"),),
        query_string=b"name=dark_mode",
    )


def test_parse_http_scope_defaults_missing_headers_and_query() -> None:
    scope: Message = {"type": "http", "method": "GET", "path": "/flags"}

    assert parse_http_scope(scope) == HttpScope(method="GET", path="/flags", headers=(), query_string=b"")


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ({"type": "http.request", "body": b"payload", "more_body": True}, RequestBody(body=b"payload", more_body=True)),
        ({"type": "http.request"}, RequestBody(body=b"", more_body=False)),
        ({"type": "http.disconnect"}, Disconnect()),
    ],
)
def test_parse_inbound_classifies_events(message: Message, expected: object) -> None:
    assert parse_inbound(message) == expected


def test_parse_inbound_rejects_an_unknown_event() -> None:
    with pytest.raises(ValueError, match="unexpected http event"):
        parse_inbound({"type": "http.surprise"})


def test_encode_outbound_renders_a_response_start() -> None:
    event = ResponseStart(status=404, headers=((b"content-type", b"application/json"),))

    assert encode_outbound(event) == {
        "type": "http.response.start",
        "status": 404,
        "headers": [[b"content-type", b"application/json"]],
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
        ResponseStart(status=200, headers=((b"content-type", b"text/plain"),)),
        ResponseBody(body=b"ok", more_body=False),
    )


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ({"type": "lifespan.startup"}, Startup()),
        ({"type": "lifespan.shutdown"}, Shutdown()),
    ],
)
def test_parse_lifespan_event_classifies_events(message: Message, expected: object) -> None:
    assert parse_lifespan_event(message) == expected


def test_parse_lifespan_event_rejects_an_unknown_event() -> None:
    with pytest.raises(ValueError, match="unexpected lifespan event"):
        parse_lifespan_event({"type": "lifespan.restart"})


@pytest.mark.parametrize(
    ("reply", "expected"),
    [
        (StartupComplete(), {"type": "lifespan.startup.complete"}),
        (ShutdownComplete(), {"type": "lifespan.shutdown.complete"}),
        (StartupFailed(message="boom"), {"type": "lifespan.startup.failed", "message": "boom"}),
    ],
)
def test_encode_lifespan_reply(reply: LifespanReply, expected: Message) -> None:
    assert encode_lifespan_reply(reply) == expected
