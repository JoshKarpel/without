from __future__ import annotations

import json
from collections.abc import AsyncIterator

import pytest
from helpers import json_response
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
from without_asgi import WebsocketInbound
from without_asgi import WebsocketOutbound
from without_asgi import WebsocketScope
from without_web import INT
from without_web import Describable
from without_web import Match
from without_web import Single
from without_web import body
from without_web import handle
from without_web import handle_stream
from without_web import path_param
from without_web import post
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

    async def make(state: str, requested_id: int, payload: dict[str, object]) -> Response:
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
    async def make(state: str) -> Response:
        return json_response(200, {"state": state})

    handler = handle(fn=make)("tenant", Match(_scope(), {}))
    assert await _run(handler) == (200, {"state": "tenant"})


def test_handle_recovers_its_openapi_from_the_extractors() -> None:
    endpoint: object = handle(
        query_param("done", lambda values: values, schema={"type": "boolean"}),
        body(json.loads, schema={"type": "object"}),
        fn=_ok,
        summary="make a thing",
    )
    assert isinstance(endpoint, Describable)
    spec = endpoint.describe()
    assert spec.summary == "make a thing"
    assert [param.name for param in spec.query] == ["done"]
    assert spec.request_body is not None and spec.request_body.shape == Single(schema={"type": "object"})


def test_handle_rejects_more_than_one_body_extractor() -> None:
    with pytest.raises(ValueError, match="more than one body"):
        handle(
            body(json.loads, schema={"type": "object"}),
            body(json.loads, schema={"type": "object"}),
            fn=_ok,
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

    def make(
        state: str, room_id: int, since_values: list[str], inputs: Stream[WebsocketInbound]
    ) -> Stream[WebsocketOutbound]:
        seen.update(state=state, room_id=room_id, since=since_values)
        return _noop_ws(inputs)

    route = ws(t"/feed/{room}", room, since)(make)
    processor = route.endpoint("tenant", Match(_ws_scope(query=b"since=5&since=9"), {"room": 7}))
    processor(stream(()))
    assert seen == {"state": "tenant", "room_id": 7, "since": ["5", "9"]}


def test_ws_rejects_a_body_extractor() -> None:
    with pytest.raises(ValueError, match="no body"):
        ws(t"/feed", body(json.loads, schema={"type": "object"}))


async def _chunks(*payloads: bytes) -> AsyncIterator[Inbound]:
    for index, payload in enumerate(payloads):
        yield RequestBody(body=payload, more_body=index < len(payloads) - 1)


async def test_handle_stream_hands_the_handler_the_live_inbound_stream() -> None:
    seen: dict[str, object] = {}

    async def make(state: str, requested_id: int, inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
        seen.update(state=state, requested_id=requested_id)
        yield ResponseStart(status=200, headers=((b"content-type", b"text/plain"),))
        async for event in inputs:
            assert isinstance(event, RequestBody)
            yield ResponseBody(body=event.body.upper(), more_body=event.more_body)

    endpoint = handle_stream(path_param("id", INT), fn=make)
    handler = endpoint("tenant", Match(_scope(), {"id": 7}))
    events = [event async for event in handler(_chunks(b"al", b"pha", b"!"))]

    assert seen == {"state": "tenant", "requested_id": 7}
    bodies = [event.body for event in events if isinstance(event, ResponseBody)]
    assert bodies == [b"AL", b"PHA", b"!"]


async def test_handle_stream_does_not_pre_consume_the_body() -> None:
    pulled: list[bytes] = []

    async def tracked() -> AsyncIterator[Inbound]:
        for chunk in (b"one", b"two"):
            pulled.append(chunk)
            yield RequestBody(body=chunk, more_body=chunk != b"two")

    async def make(state: str, inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
        yield ResponseStart(status=202, headers=())
        assert pulled == [], "the body was buffered before the handler ran"
        async for _ in inputs:
            pass

    handler = handle_stream(fn=make)("tenant", Match(_scope(), {}))
    events = [event async for event in handler(tracked())]

    assert isinstance(events[0], ResponseStart) and events[0].status == 202
    assert pulled == [b"one", b"two"]


async def test_handle_stream_buffers_its_output_when_the_handler_returns_a_response() -> None:
    async def collect(state: str, inputs: Stream[Inbound]) -> Response:
        total = 0
        async for event in inputs:
            assert isinstance(event, RequestBody)
            total += len(event.body)
        return json_response(201, {"state": state, "received": total})

    handler = handle_stream(fn=collect)("tenant", Match(_scope(), {}))
    assert await _run(handler, b"abcdef") == (201, {"state": "tenant", "received": 6})


def test_handle_stream_rejects_a_body_extractor() -> None:
    with pytest.raises(ValueError, match="cannot take a body extractor"):
        handle_stream(body(json.loads, schema={"type": "object"}), fn=lambda state, payload, inputs: _empty())


def test_handle_stream_recovers_its_openapi_from_the_extractors() -> None:
    endpoint: object = handle_stream(
        query_param("offset", lambda values: values, schema={"type": "integer"}),
        fn=lambda state, offset, inputs: _empty(),
        summary="upload a stream",
    )
    assert isinstance(endpoint, Describable)
    spec = endpoint.describe()
    assert spec.summary == "upload a stream"
    assert [param.name for param in spec.query] == ["offset"]
    assert spec.request_body is None


async def test_post_stream_decorator_builds_a_streaming_route() -> None:
    requested_id = path_param("id", INT)

    async def upload(state: str, target_id: int, inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
        total = 0
        async for event in inputs:
            assert isinstance(event, RequestBody)
            total += len(event.body)
        yield ResponseStart(status=200, headers=((b"content-type", b"application/json"),))
        yield ResponseBody(body=json.dumps({"id": target_id, "bytes": total}).encode(), more_body=False)

    route = post.stream(t"/uploads/{id}", requested_id, summary="Stream an upload")(upload)
    assert tuple(route.methods) == ("POST",)
    handler = route.methods["POST"]("tenant", Match(_scope(), {"id": 3}))
    events = [event async for event in handler(_chunks(b"ab", b"cde"))]

    start = events[0]
    assert isinstance(start, ResponseStart) and start.status == 200
    raw = b"".join(event.body for event in events if isinstance(event, ResponseBody))
    assert json.loads(raw) == {"id": 3, "bytes": 5}


async def test_handle_awaits_an_async_handler_that_returns_a_response() -> None:
    async def make(state: str, payload: dict[str, object]) -> Response:
        return json_response(200, {"state": state, "got": payload})

    endpoint = handle(body(json.loads, schema={"type": "object"}), fn=make)
    handler = endpoint("tenant", Match(_scope(), {}))
    assert await _run(handler, b'{"x": 1}') == (200, {"state": "tenant", "got": {"x": 1}})


def _empty() -> Stream[Outbound]:
    return stream(())


async def _ok(*args: object) -> Response:
    return json_response(200, {})
