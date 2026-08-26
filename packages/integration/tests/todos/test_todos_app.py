from __future__ import annotations

import json

from integration.todos.app import _recover
from integration.todos.app import session
from integration.todos.app import todos_app
from integration.todos.app import todos_openapi
from without_asgi import ASGIApp
from without_asgi import RawMessage
from without_asgi import WebsocketAccept
from without_asgi import WebsocketBinary
from without_asgi import WebsocketClose
from without_asgi import WebsocketConnect
from without_asgi import WebsocketReceive
from without_asgi import WebsocketText
from without_asgi.scope import parse_websocket_scope
from without_streams import collect
from without_streams import stream_from_iterable
from without_web import Match

from packages.integration.tests.helpers import a_todo_list
from packages.integration.tests.helpers import drive_websocket
from packages.integration.tests.helpers import running


async def _request(
    app: ASGIApp,
    method: str,
    path: str,
    *,
    query: bytes = b"",
    body: bytes = b"",
    headers: list[tuple[bytes, bytes]] | None = None,
) -> tuple[int, dict[str, list[bytes]], object]:
    scope: RawMessage = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "path": path,
        "headers": headers or [],
        "query_string": query,
    }
    request = iter([{"type": "http.request", "body": body, "more_body": False}])

    async def receive() -> RawMessage:
        return next(request)

    sent: list[RawMessage] = []

    async def send(message: RawMessage) -> None:
        sent.append(message)

    await app(scope, receive, send)
    start = sent[0]
    status = start["status"]
    assert isinstance(status, int)
    raw_headers = start["headers"]
    assert isinstance(raw_headers, (list, tuple))
    collected: dict[str, list[bytes]] = {}
    for name, value in raw_headers:
        collected.setdefault(name.decode(), []).append(value)
    payload = b"".join(m["body"] for m in sent if m["type"] == "http.response.body" and isinstance(m["body"], bytes))
    if not payload:  # pragma: no cover - every todos HTTP route returns a body, so the empty-body path is dead here
        return status, collected, None
    try:
        return status, collected, json.loads(payload)
    except json.JSONDecodeError:
        return status, collected, payload


async def _stream_request(app: ASGIApp, method: str, path: str, chunks: list[bytes]) -> tuple[int, list[bytes]]:
    scope: RawMessage = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "path": path,
        "headers": [],
        "query_string": b"",
    }
    events = iter(
        [
            {"type": "http.request", "body": chunk, "more_body": index < len(chunks) - 1}
            for index, chunk in enumerate(chunks)
        ]
    )

    async def receive() -> RawMessage:
        return next(events)

    sent: list[RawMessage] = []

    async def send(message: RawMessage) -> None:
        sent.append(message)

    await app(scope, receive, send)
    start = sent[0]
    status = start["status"]
    assert isinstance(status, int)
    bodies = [
        m["body"] for m in sent if m["type"] == "http.response.body" and isinstance(m["body"], bytes) and m["body"]
    ]
    return status, bodies


async def test_lists_todos_with_the_powered_by_header() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        status, headers, body = await _request(app, "GET", "/todos")
    assert status == 200
    assert headers["x-powered-by"] == [b"without-web"]
    assert body == {"todos": [{"id": 1, "title": "write", "done": False}, {"id": 2, "title": "ship", "done": True}]}


async def test_filters_todos_by_the_done_query_parameter() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        _status, _headers, body = await _request(app, "GET", "/todos", query=b"done=true")
    assert body == {"todos": [{"id": 2, "title": "ship", "done": True}]}


async def test_shows_one_todo_by_typed_path_parameter() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        status, _headers, body = await _request(app, "GET", "/todos/1")
    assert status == 200
    assert body == {"id": 1, "title": "write", "done": False}


async def test_a_missing_todo_is_a_mapped_404() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        status, _headers, body = await _request(app, "GET", "/todos/99")
    assert status == 404
    assert body == {"error": "no todo with id 99", "id": 99}


async def test_creating_a_todo_echoes_it_with_its_url_and_idempotency_key() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        status, _headers, body = await _request(
            app, "POST", "/todos", body=b'{"title": "deploy"}', headers=[(b"idempotency-key", b"abc-123")]
        )
    assert status == 201
    # The body carries the new todo plus its URL, reversed from the `show_todo` route.
    assert body == {"id": 3, "title": "deploy", "done": False, "url": "/todos/3", "idempotency_key": "abc-123"}


async def test_creating_a_todo_without_the_idempotency_key_is_a_422() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        status, _headers, body = await _request(app, "POST", "/todos", body=b'{"title": "deploy"}')
    assert status == 422
    assert isinstance(body, dict)
    assert body["error"] == "expected exactly one value, got none"
    assert body["field"] == "idempotency-key"


async def test_creating_a_todo_with_a_duplicated_idempotency_key_is_a_422() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        status, _headers, body = await _request(
            app,
            "POST",
            "/todos",
            body=b'{"title": "deploy"}',
            headers=[(b"idempotency-key", b"one"), (b"idempotency-key", b"two")],
        )
    assert status == 422
    assert isinstance(body, dict)
    assert body["error"] == "expected exactly one value, got 2"
    assert body["field"] == "idempotency-key"


async def test_import_echoes_each_todo_as_the_ndjson_stream_arrives() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        # The "beta" object straddles the first two chunks, so a correct decode
        # proves the handler reassembles across chunk boundaries as they arrive.
        status, bodies = await _stream_request(
            app,
            "POST",
            "/todos/import",
            [b'{"title": "alpha"}\n{"title": ', b'"beta", "done": true}\n', b'{"oops": 1}\n'],
        )
    assert status == 200
    records = [json.loads(line) for line in b"".join(bodies).splitlines() if line]
    assert records == [
        {"ok": True, "todo": {"id": 3, "title": "alpha", "done": False}},
        {"ok": True, "todo": {"id": 4, "title": "beta", "done": True}},
        {"ok": False, "errors": 1},
    ]


async def test_import_skips_blank_lines_and_imports_a_final_unterminated_line() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        # A blank line between records exercises the skip-empty branch; the final
        # chunk has no trailing newline, so it is imported from the leftover buffer
        # after the stream ends rather than inside the split loop.
        status, bodies = await _stream_request(
            app,
            "POST",
            "/todos/import",
            [b'{"title": "alpha"}\n\n', b'{"title": "omega"}'],
        )
    assert status == 200
    records = [json.loads(line) for line in b"".join(bodies).splitlines() if line]
    assert records == [
        {"ok": True, "todo": {"id": 3, "title": "alpha", "done": False}},
        {"ok": True, "todo": {"id": 4, "title": "omega", "done": False}},
    ]


async def test_recover_lets_an_unmapped_exception_propagate() -> None:
    assert await _recover(RuntimeError("not a todo error")) is None


async def test_an_invalid_body_is_a_mapped_422() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        status, _headers, body = await _request(
            app, "POST", "/todos", body=b"{}", headers=[(b"idempotency-key", b"abc-123")]
        )
    assert status == 422
    assert isinstance(body, dict)
    assert body["error"] == "invalid todo body"


async def test_a_known_path_with_the_wrong_method_is_405_with_allow() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        status, headers, _body = await _request(app, "PUT", "/todos")
    assert status == 405
    assert headers["allow"] == [b"GET, POST"]


async def test_an_unknown_path_is_the_404_fallback() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        status, _headers, body = await _request(app, "GET", "/nope")
    assert status == 404
    assert body == {"error": "no route for GET /nope"}


async def test_the_admin_router_is_grafted_under_its_prefix() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        status, _headers, body = await _request(app, "GET", "/admin/stats", headers=[(b"authorization", b"token")])
    assert status == 200
    assert body == {"total": 2, "done": 1}


async def test_the_mounted_admin_middleware_gates_the_subtree() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        admin_status, _admin_headers, admin_body = await _request(app, "GET", "/admin/stats")
        open_status, _open_headers, _open_body = await _request(app, "GET", "/todos")
    assert admin_status == 401
    assert admin_body == {"error": "admin requires authorization"}
    assert open_status == 200


async def test_the_opaque_mount_sees_the_prefix_trimmed_scope() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        _status, _headers, body = await _request(app, "GET", "/legacy/ping")
    assert body == {"path": "/ping", "root_path": "/legacy"}


def _dig(value: object, *keys: str) -> object:
    for key in keys:
        assert isinstance(value, dict)
        value = value[key]
    return value


async def test_openapi_merges_router_and_handler_halves() -> None:
    spec = todos_openapi()
    paths = spec["paths"]
    assert isinstance(paths, dict)
    # Opaque mounts are black boxes: neither the prefix nor its catch-all appear.
    assert set(paths) == {"/todos", "/todos/import", "/todos/{id}", "/admin/stats"}

    # The streaming-input route describes its inbound and outbound sequences with
    # `itemSchema` (one item's shape), not `schema` (the whole body): the body is a
    # stream of NDJSON records, not a single document. The media type is the app's.
    import_in = _dig(spec, "paths", "/todos/import", "post", "requestBody", "content", "application/x-ndjson")
    assert isinstance(import_in, dict)
    assert "schema" not in import_in
    assert "itemSchema" in import_in
    import_out = _dig(spec, "paths", "/todos/import", "post", "responses", "200", "content", "application/x-ndjson")
    assert isinstance(import_out, dict)
    item_schema = import_out["itemSchema"]
    assert isinstance(item_schema, dict)
    assert "oneOf" in item_schema

    show_params = _dig(spec, "paths", "/todos/{id}", "get", "parameters")
    assert isinstance(show_params, list)
    assert {"name": "id", "in": "path", "required": True, "schema": {"type": "integer"}} in show_params

    properties = _dig(
        spec, "paths", "/todos", "post", "requestBody", "content", "application/json", "schema", "properties"
    )
    assert isinstance(properties, dict)
    assert "title" in properties

    post_params = _dig(spec, "paths", "/todos", "post", "parameters")
    assert isinstance(post_params, list)
    assert {"name": "idempotency-key", "in": "header", "required": True, "schema": {"type": "string"}} in post_params

    list_params = _dig(spec, "paths", "/todos", "get", "parameters")
    assert isinstance(list_params, list)
    assert [param["in"] for param in list_params if isinstance(param, dict)] == ["query"]


def _sent_replies(sent: list[RawMessage]) -> list[object]:
    replies: list[object] = []
    for message in sent:
        if message["type"] != "websocket.send":
            continue
        text = message["text"]
        assert isinstance(text, str)
        replies.append(json.loads(text))
    return replies


async def test_the_session_folds_each_submission_into_the_running_list() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        sent = await drive_websocket(
            app,
            "/todos/session",
            [
                {"type": "websocket.connect"},
                {"type": "websocket.receive", "text": '{"title": "soon"}'},
                {"type": "websocket.receive", "text": '{"title": "later", "done": true}'},
                {"type": "websocket.disconnect", "code": 1000},
            ],
        )
    assert sent[0]["type"] == "websocket.accept"
    # The second reply's id and total advance past the first, proving the fold's
    # accumulator carries across frames rather than restarting from the seed each time.
    # Each reply carries the new todo's URL, reversed from the HTTP `show_todo` route
    # via the injected `url_for()` (the same path `POST /todos` sets as `Location`).
    assert _sent_replies(sent) == [
        {"ok": True, "todo": {"id": 3, "title": "soon", "done": False}, "url": "/todos/3", "total": 3},
        {"ok": True, "todo": {"id": 4, "title": "later", "done": True}, "url": "/todos/4", "total": 4},
    ]


async def test_the_session_answers_a_malformed_frame_without_closing() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        sent = await drive_websocket(
            app,
            "/todos/session",
            [
                {"type": "websocket.connect"},
                {"type": "websocket.receive", "text": "{not json}"},
                {"type": "websocket.disconnect", "code": 1000},
            ],
        )
    # Accepted, an error frame, and no close: the handshake is already accepted, so
    # a bad submission is reported in-band rather than tearing down the connection.
    assert [m["type"] for m in sent] == ["websocket.accept", "websocket.send"]
    (reply,) = _sent_replies(sent)
    assert isinstance(reply, dict)
    assert reply["ok"] is False
    assert reply["errors"] >= 1


async def test_session_closes_on_a_binary_frame_and_returns_when_the_stream_ends() -> None:
    # Drive the endpoint's processor directly with a finite frame stream: a binary
    # frame closes (text-only protocol) and the loop exits naturally when the stream
    # runs dry without a disconnect, the connection-end path the live socket reaches
    # only via a disconnect.
    scope = parse_websocket_scope(
        {"type": "websocket", "asgi": {"version": "3.0"}, "path": "/todos/session", "headers": []}
    )
    handler = session.endpoint(a_todo_list(), Match(scope, {}))

    outputs = await collect(
        handler(
            stream_from_iterable(
                [
                    WebsocketConnect(),
                    WebsocketReceive(WebsocketText(text='{"title": "soon"}')),
                    WebsocketReceive(WebsocketBinary(data=b"\x00\x01")),
                ]
            )
        )
    )

    assert outputs[0] == WebsocketAccept()
    assert outputs[-1] == WebsocketClose(code=1003, reason="text frames only")


async def test_an_unrouted_websocket_path_is_closed_by_the_fallback() -> None:
    app = todos_app(a_todo_list())
    async with running(app):
        sent = await drive_websocket(app, "/todos/nope", [{"type": "websocket.connect"}])
    assert sent == [{"type": "websocket.close", "code": 1000, "reason": ""}]
