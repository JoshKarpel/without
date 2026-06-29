from __future__ import annotations

import json
from dataclasses import dataclass

import pytest
from without_asgi import Asgi
from without_asgi import HttpScope
from without_asgi import RawHeaders
from without_asgi import WebsocketScope
from without_web import INT
from without_web import Body
from without_web import HeaderParam
from without_web import QueryParam
from without_web import Request
from without_web import Single
from without_web import body
from without_web import catch_all
from without_web import header_param
from without_web import http_scope
from without_web import into
from without_web import path_param
from without_web import query_param
from without_web import websocket_scope


def _scope(*, query: bytes = b"", headers: RawHeaders = ()) -> HttpScope:
    return HttpScope(
        asgi=Asgi(version="3.0", spec_version="2.0"),
        http_version="1.1",
        method="GET",
        scheme="http",
        path="/todos/7",
        raw_path=None,
        query_string=query,
        root_path="",
        headers=headers,
        client=None,
        server=None,
        extensions=None,
    )


def _request(
    *, query: bytes = b"", headers: RawHeaders = (), params: dict[str, object] | None = None, body: bytes = b""
) -> Request:
    return Request(scope=_scope(query=query, headers=headers), params=params or {}, body=body)


def test_path_param_reads_the_already_parsed_value_at_its_type() -> None:
    request = _request(params={"id": 7, "slug": "ship"})
    assert path_param("id", INT).extract(request) == 7


def test_catch_all_reads_the_rest_of_path_value_already_parsed_by_the_router() -> None:
    request = _request(params={"rest": "a/b/c", "id": 7})
    assert catch_all("rest").extract(request) == "a/b/c"


def test_query_param_hands_the_parser_every_value_for_its_name() -> None:
    request = _request(query=b"done=true&done=false&other=x")
    seen = query_param("done", lambda values: values, schema={"type": "string"}).extract(request)
    assert seen == ["true", "false"]


def test_query_param_hands_the_parser_an_empty_list_when_absent() -> None:
    request = _request(query=b"other=x")
    extractor = query_param("done", lambda values: values or ["missing"], schema={"type": "string"})
    assert extractor.extract(request) == ["missing"]


def test_header_param_matches_case_insensitively_and_keeps_order() -> None:
    request = _request(headers=((b"x-trace", b"first"), (b"content-type", b"text/plain"), (b"x-trace", b"second")))
    extractor = header_param("X-Trace", lambda values: values, schema={"type": "string"})
    assert extractor.extract(request) == [b"first", b"second"]


def test_body_parses_the_buffered_bytes() -> None:
    request = _request(body=b'{"count": 3}')
    assert body(json.loads, schema={"type": "object"}).extract(request) == {"count": 3}


def test_http_scope_hands_back_the_unparsed_http_scope() -> None:
    request = _request(query=b"done=true")
    assert http_scope().extract(request) is request.scope


def test_websocket_scope_hands_back_the_unparsed_websocket_scope() -> None:
    scope = WebsocketScope(
        asgi=Asgi(version="3.0", spec_version="2.0"),
        http_version="1.1",
        scheme="ws",
        path="/todos/7/events",
        raw_path=None,
        query_string=b"since=5",
        root_path="",
        headers=(),
        client=None,
        server=None,
        subprotocols=(),
        extensions=None,
    )
    request = Request(scope=scope, params={}, body=b"")
    assert websocket_scope().extract(request) is scope


def test_query_param_contributes_its_openapi_fragment() -> None:
    extractor = query_param("done", lambda values: values, schema={"type": "boolean"}, required=True)
    assert extractor.query == (QueryParam(name="done", schema={"type": "boolean"}, required=True),)
    assert extractor.request_body is None


def test_header_param_contributes_its_openapi_fragment() -> None:
    extractor = header_param("X-Trace", lambda values: values, schema={"type": "string"})
    assert extractor.headers == (HeaderParam(name="X-Trace", schema={"type": "string"}, required=False),)


def test_body_contributes_its_request_body_fragment() -> None:
    extractor = body(json.loads, schema={"type": "object"}, media_type="application/json")
    assert extractor.request_body == Body(media_type="application/json", shape=Single(schema={"type": "object"}))


def test_path_param_contributes_no_openapi_because_the_router_owns_it() -> None:
    extractor = path_param("id", INT)
    assert extractor.query == ()
    assert extractor.headers == ()
    assert extractor.request_body is None


@dataclass(frozen=True, slots=True)
class Coords:
    x: int
    y: int


def test_into_builds_a_value_by_reusing_the_existing_tokens() -> None:
    extractor = into(Coords, path_param("x", INT), path_param("y", INT))
    assert extractor.extract(_request(params={"x": 3, "y": 8})) == Coords(x=3, y=8)


def test_into_carries_the_constituent_query_fragments_for_openapi() -> None:
    extractor = into(
        lambda first, second: (first, second),
        query_param("a", lambda values: values, schema={"type": "string"}),
        query_param("b", lambda values: values, schema={"type": "string"}),
    )
    assert [param.name for param in extractor.query] == ["a", "b"]


@dataclass(frozen=True, slots=True)
class Span:
    low: int
    high: int

    def __post_init__(self) -> None:
        if self.low > self.high:
            raise ValueError("low must not exceed high")


def test_into_builds_the_value_when_the_constructor_accepts() -> None:
    extractor = into(Span, path_param("low", INT), path_param("high", INT))
    assert extractor.extract(_request(params={"low": 2, "high": 9})) == Span(low=2, high=9)


def test_into_propagates_a_constructor_validation_error() -> None:
    # The same shape a pydantic model's validator takes: construction rejects, the
    # error propagates for the router's exception handlers to map to a 4xx.
    extractor = into(Span, path_param("low", INT), path_param("high", INT))
    with pytest.raises(ValueError, match="low must not exceed high"):
        extractor.extract(_request(params={"low": 9, "high": 2}))
