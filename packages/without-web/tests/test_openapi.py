from __future__ import annotations

from collections.abc import Mapping

from without_asgi import HttpScope
from without_asgi import Response
from without_web import INT
from without_web import Body
from without_web import Match
from without_web import QueryParam
from without_web import ResponseSpec
from without_web import Router
from without_web import RouteSpec
from without_web import Sequence
from without_web import Single
from without_web import buffered
from without_web import describe
from without_web import json_response
from without_web import openapi
from without_web import path_param
from without_web import route


@buffered
def _fallback(state: object, match: Match[HttpScope], body: bytes) -> Response:
    return json_response(404, {})


@describe(
    RouteSpec(
        summary="Get one",
        responses={
            200: ResponseSpec(description="the thing", body=Body("application/json", Single({"type": "object"})))
        },
    )
)
@buffered
def _show(state: object, match: Match[HttpScope], body: bytes) -> Response:
    return json_response(200, {})


@describe(
    RouteSpec(
        query=(QueryParam(name="done", schema={"type": "boolean"}),),
        request_body=Body("application/json", Single({"type": "string"})),
        responses={201: ResponseSpec(description="made", body=Body("application/json", Single({"type": "integer"})))},
    )
)
@buffered
def _create(state: object, match: Match[HttpScope], body: bytes) -> Response:
    return json_response(201, {})


def _spec() -> dict[str, object]:
    router: Router[object] = Router(
        routes=(route(t"/things/{path_param('id', INT)}", get=_show), route("/things", post=_create)),
        fallback=_fallback,
    )
    return openapi(router, title="things", version="2.0.0")


def _operation(spec: Mapping[str, object], path: str, method: str) -> object:
    paths = spec["paths"]
    assert isinstance(paths, dict)
    item = paths[path]
    assert isinstance(item, dict)
    return item[method]


def test_openapi_carries_the_document_envelope() -> None:
    spec = _spec()
    assert spec["openapi"] == "3.2.0"
    assert spec["info"] == {"title": "things", "version": "2.0.0"}


def test_openapi_merges_the_router_path_param_half_with_the_describe_half() -> None:
    assert _operation(_spec(), "/things/{id}", "get") == {
        "summary": "Get one",
        "parameters": [{"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}}],
        "responses": {
            "200": {"description": "the thing", "content": {"application/json": {"schema": {"type": "object"}}}}
        },
    }


def test_openapi_takes_query_and_body_from_the_endpoints_describe() -> None:
    assert _operation(_spec(), "/things", "post") == {
        "parameters": [{"name": "done", "in": "query", "required": False, "schema": {"type": "boolean"}}],
        "requestBody": {"content": {"application/json": {"schema": {"type": "string"}}}},
        "responses": {"201": {"description": "made", "content": {"application/json": {"schema": {"type": "integer"}}}}},
    }


def test_openapi_resolves_a_type_through_the_injected_schema_for() -> None:
    @describe(RouteSpec(request_body=Body("application/json", Single(_Payload))))
    @buffered
    def create(state: object, match: Match[HttpScope], body: bytes) -> Response:
        return json_response(201, {})

    router: Router[object] = Router(routes=(route("/x", post=create),), fallback=_fallback)
    spec = openapi(router, schema_for=lambda annotation: {"type": "object", "title": annotation.__name__})
    assert _operation(spec, "/x", "post") == {
        "requestBody": {"content": {"application/json": {"schema": {"type": "object", "title": "_Payload"}}}},
        "responses": {},
    }


def test_openapi_renders_a_sequence_body_as_item_schema() -> None:
    @describe(
        RouteSpec(
            request_body=Body("application/x-ndjson", Sequence({"type": "object"})),
            responses={
                200: ResponseSpec(description="streamed", body=Body("text/event-stream", Sequence({"type": "string"})))
            },
        )
    )
    @buffered
    def upload(state: object, match: Match[HttpScope], body: bytes) -> Response:
        return json_response(200, {})

    router: Router[object] = Router(routes=(route("/feed", post=upload),), fallback=_fallback)
    assert _operation(openapi(router), "/feed", "post") == {
        "requestBody": {"content": {"application/x-ndjson": {"itemSchema": {"type": "object"}}}},
        "responses": {
            "200": {"description": "streamed", "content": {"text/event-stream": {"itemSchema": {"type": "string"}}}}
        },
    }


class _Payload:
    pass
