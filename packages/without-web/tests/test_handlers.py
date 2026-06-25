from __future__ import annotations

import json
from collections.abc import AsyncIterator

import pytest
from without import Stream
from without import stream
from without_asgi import Asgi
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import RequestBody
from without_asgi import Response
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_asgi import WebsocketHandler
from without_asgi import WebsocketInbound
from without_asgi import WebsocketOutbound
from without_asgi import WebsocketScope
from without_web import INT
from without_web import Describable
from without_web import Match
from without_web import body
from without_web import handle
from without_web import json_response
from without_web import path_param
from without_web import query_param
from without_web import ws


def _scope(*, query: bytes = b"") -> HttpScope:
    return HttpScope(
        asgi=Asgi(version="3.0", spec_version="2.0"),
        http_version="1.1",
        method="POST",
        scheme="http",
        path="/things/7",
        raw_path=None,
        query_string=query,
        root_path="",
        headers=(),
        client=None,
        server=None,
        extensions=None,
    )


async def _inbound(payload: bytes) -> AsyncIterator[Inbound]:
    yield RequestBody(body=payload, more_body=False)


async def _run(handler: HttpHandler, payload: bytes = b"") -> tuple[int, object]:
    events = [event async for event in handler(_inbound(payload))]
    start = events[0]
    assert isinstance(start, ResponseStart)
    raw = b"".join(event.body for event in events if isinstance(event, ResponseBody))
    return start.status, json.loads(raw)


async def test_handle_calls_the_function_with_the_typed_extracted_values() -> None:
    seen: dict[str, object] = {}

    def make(state: str, requested_id: int, payload: dict[str, object]) -> Response:
        seen.update(state=state, requested_id=requested_id, payload=payload)
        return json_response(201, {"ok": True})

    endpoint = handle(path_param("id", INT), body(json.loads, schema={"type": "object"}), fn=make)
    handler = endpoint("tenant", Match(_scope(), {"id": 7}))
    status, response = await _run(handler, b'{"title": "ship"}')

    assert status == 201 and response == {"ok": True}
    assert seen == {"state": "tenant", "requested_id": 7, "payload": {"title": "ship"}}


async def test_handle_relays_a_streamed_response_without_buffering_the_output() -> None:
    def make(state: str, requested_id: int) -> AsyncIterator[Outbound]:
        async def chunks() -> AsyncIterator[Outbound]:
            yield ResponseStart(status=206, headers=((b"content-type", b"text/plain"),))
            yield ResponseBody(body=b"part-", more_body=True)
            yield ResponseBody(body=f"{requested_id}".encode(), more_body=False)

        return chunks()

    handler = handle(path_param("id", INT), fn=make)("tenant", Match(_scope(), {"id": 5}))
    events = [event async for event in handler(_inbound(b""))]
    start = events[0]
    assert isinstance(start, ResponseStart) and start.status == 206
    assert b"".join(e.body for e in events if isinstance(e, ResponseBody)) == b"part-5"


async def test_handle_with_no_extractors_passes_only_the_state() -> None:
    def make(state: str) -> Response:
        return json_response(200, {"state": state})

    handler = handle(fn=make)("tenant", Match(_scope(), {}))
    assert await _run(handler) == (200, {"state": "tenant"})


def test_handle_recovers_its_openapi_from_the_extractors() -> None:
    endpoint: object = handle(
        query_param("done", lambda values: values, schema={"type": "boolean"}),
        body(json.loads, schema={"type": "object"}),
        fn=lambda state, done, payload: json_response(200, {}),
        summary="make a thing",
    )
    assert isinstance(endpoint, Describable)
    spec = endpoint.describe()
    assert spec.summary == "make a thing"
    assert [param.name for param in spec.query] == ["done"]
    assert spec.request_body is not None and spec.request_body.schema == {"type": "object"}


def test_handle_rejects_more_than_one_body_extractor() -> None:
    with pytest.raises(ValueError, match="more than one body"):
        handle(
            body(json.loads, schema={"type": "object"}),
            body(json.loads, schema={"type": "object"}),
            fn=lambda state, first, second: json_response(200, {}),
        )


def _ws_scope(*, query: bytes = b"") -> WebsocketScope:
    return WebsocketScope(
        asgi=Asgi(version="3.0", spec_version="2.0"),
        http_version="1.1",
        scheme="ws",
        path="/feed/7",
        raw_path=None,
        query_string=query,
        root_path="",
        headers=(),
        client=None,
        server=None,
        subprotocols=(),
        extensions=None,
    )


def _noop_ws(inputs: Stream[WebsocketInbound]) -> Stream[WebsocketOutbound]:
    return stream(())


def test_ws_ties_path_and_query_to_the_handler() -> None:
    seen: dict[str, object] = {}
    room = path_param("room", INT)
    since = query_param("since", lambda values: values, schema={"type": "string"})

    def make(state: str, room_id: int, since_values: list[str]) -> WebsocketHandler:
        seen.update(state=state, room_id=room_id, since=since_values)
        return _noop_ws

    route = ws(t"/feed/{room}", room, since)(make)
    route.endpoint("tenant", Match(_ws_scope(query=b"since=5&since=9"), {"room": 7}))
    assert seen == {"state": "tenant", "room_id": 7, "since": ["5", "9"]}


def test_ws_rejects_a_body_extractor() -> None:
    with pytest.raises(ValueError, match="no body"):
        ws(t"/feed", body(json.loads, schema={"type": "object"}))
