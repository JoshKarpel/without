from __future__ import annotations

import json
from collections.abc import AsyncIterator
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import assert_never

from pydantic import BaseModel
from pydantic import ValidationError
from without import Stream
from without import stream
from without_asgi import ASGIApp
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import RequestBody
from without_asgi import Response
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_asgi import WebsocketAccept
from without_asgi import WebsocketBinary
from without_asgi import WebsocketClose
from without_asgi import WebsocketConnect
from without_asgi import WebsocketDisconnect
from without_asgi import WebsocketHandler
from without_asgi import WebsocketInbound
from without_asgi import WebsocketOutbound
from without_asgi import WebsocketReceive
from without_asgi import WebsocketScope
from without_asgi import WebsocketSend
from without_asgi import WebsocketText
from without_asgi import encode_response
from without_asgi import make_asgi_app
from without_asgi.routing import HttpMiddleware
from without_asgi.routing import stack
from without_asgi.routing import wrap
from without_web import INT
from without_web import Body
from without_web import Match
from without_web import Mount
from without_web import ResponseSpec
from without_web import Router
from without_web import Sequence
from without_web import WebsocketRouter
from without_web import body
from without_web import catching
from without_web import get
from without_web import handle
from without_web import http_scope
from without_web import openapi
from without_web import path_param
from without_web import post
from without_web import query_param
from without_web import ws

from integration.responses import json_response
from integration.todos.core import NewTodo
from integration.todos.core import Todo
from integration.todos.core import TodoList
from integration.todos.core import TodoNotFound

# A todo-list API on without-web: the canonical REST shape, chosen because it
# exercises the whole router design at once. `t"/todos/{todo_id}"` is a typed path
# parameter (the same `todo_id` token names the segment and is passed as the
# handler's typed `int` argument); `GET` vs `POST` on `/todos` is method dispatch
# (so a `PUT` is a 405, not a 404); `?done=` is a typed query filter; `/admin` is
# a grafted sub-router and `/legacy` an opaque mount; `TodoNotFound`/
# `ValidationError` are mapped by exception handlers; and the routes describe
# themselves for OpenAPI.
#
# Each handler is a plain function of *parsed values* declared by a `@get`/`@post`
# decorator that ties typed `Extractor`s to its parameters: `show_todo` takes an
# `int`, not a `Match` it must narrow, and `create_todo` takes a `NewTodo`, not
# raw bytes. The decorator only annotates and returns a `Route` value; the
# `Router` is still assembled explicitly from those values. Extractors are the
# single source of truth: the value that parses the query or body also
# contributes its OpenAPI fragment.
#
# State is a single immutable `TodoList` value held for the connection's life, so
# this example stays about routing and leaves a shared mutable store (the
# actor-model question) out of scope: `POST` validates and echoes the would-be
# todo without persisting it. Two endpoints read their input *live* rather than
# buffering it, folding a stream into a working list that never escapes the
# connection: `POST /todos/import` (a `@post.stream` route over an NDJSON upload)
# and the `/todos/session` websocket (the same fold kept open, bidirectionally).


def _render(todo: Todo) -> dict[str, object]:
    return {"id": todo.id, "title": todo.title, "done": todo.done}


def _done(values: list[str]) -> bool | None:
    """
    The `done` query filter: `True`/`False` when present, `None` when absent.

    A repeated `?done=` honors the last value, the usual last-wins convention.
    """
    return None if not values else values[-1] == "true"


done_query = query_param("done", _done, schema={"type": "boolean"})
new_todo_body = body(NewTodo.model_validate_json, schema=NewTodo)
# One token, declared once: it is the route's `{id}` segment (matched and schemed
# through `INT`) *and* the handler's typed `int` argument. No second place to keep
# "id" or "int" in sync.
todo_id = path_param("id", INT)

# The import stream carries todo bodies in and a result record per line back out.
# A malformed line is reported in its own record rather than failing the already
# committed `200`, so a result is a oneOf of the success and error shapes: the
# same multi-variant payload an SSE/event stream documents with `itemSchema` plus
# `oneOf`. `without-web` only carries this description; the handler frames the
# bytes (see `_ndjson`).
import_result_schema: Mapping[str, object] = {
    "oneOf": [
        {
            "type": "object",
            "properties": {"ok": {"const": True}, "todo": {"type": "object"}},
            "required": ["ok", "todo"],
        },
        {
            "type": "object",
            "properties": {"ok": {"const": False}, "errors": {"type": "integer"}},
            "required": ["ok", "errors"],
        },
    ],
}


@get("/todos", done_query, summary="List todos")
async def list_todos(todos: TodoList, done: bool | None) -> Response:
    return json_response(200, {"todos": [_render(todo) for todo in todos.matching(done)]})


@post("/todos", new_todo_body, summary="Create a todo")
async def create_todo(todos: TodoList, new: NewTodo) -> Response:
    _list, created = todos.added(new)
    return json_response(201, _render(created))


@post.stream(
    "/todos/import",
    summary="Bulk-import todos from an NDJSON stream",
    request_body=Body("application/x-ndjson", Sequence(NewTodo)),
    responses={
        200: ResponseSpec(
            description="one NDJSON result record per imported line",
            body=Body("application/x-ndjson", Sequence(import_result_schema)),
        )
    },
)
async def import_todos(todos: TodoList, inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
    """
    Fold a newline-delimited stream of todo bodies into the list *as it
    arrives*, echoing each created todo immediately rather than buffering the
    whole upload first. This is the streaming-input dual of `@post`: the route is
    declared with `@post.stream`, so the handler *is* the processor, taking the
    live inbound stream as its trailing argument (a `body` extractor would force
    the buffering this avoids, so one is rejected at build time).

    The `200` is committed before the request body is fully received, so a line in
    an early chunk is acknowledged while later chunks are still in flight, and a
    line straddling two chunks is reassembled here. The fold threads a local
    `TodoList` that never escapes the connection (it stays a value, not shared
    mutable state); like `POST /todos` it does not persist beyond the request. A
    malformed line is reported in its own record rather than failing the stream,
    since the `200` is already on the wire and cannot be rewritten to a `422`.
    """
    yield ResponseStart(status=200, headers=((b"content-type", b"application/x-ndjson"),))
    working = todos
    pending = b""
    async for event in inputs:
        assert isinstance(event, RequestBody)
        pending += event.body
        while b"\n" in pending:
            line, pending = pending.split(b"\n", 1)
            if line.strip():
                working, record = _imported(working, line)
                yield record
    if pending.strip():
        _working, record = _imported(working, pending)
        yield record
    yield ResponseBody(body=b"", more_body=False)


def _imported(working: TodoList, line: bytes) -> tuple[TodoList, ResponseBody]:
    try:
        new = NewTodo.model_validate_json(line)
    except ValidationError as exc:
        return working, _ndjson({"ok": False, "errors": exc.error_count()})
    updated, created = working.added(new)
    return updated, _ndjson({"ok": True, "todo": _render(created)})


def _ndjson(record: Mapping[str, object]) -> ResponseBody:
    return ResponseBody(body=json.dumps(record).encode() + b"\n", more_body=True)


@get(t"/todos/{todo_id}", todo_id, summary="Get one todo")
async def show_todo(todos: TodoList, requested_id: int) -> Response:
    return json_response(200, _render(todos.get(requested_id)))


@get("/stats", summary="Todo counts")
async def stats(todos: TodoList) -> Response:
    return json_response(200, {"total": len(todos.matching(None)), "done": len(todos.matching(True))})


async def not_found(todos: TodoList, scope: HttpScope) -> Response:
    return json_response(404, {"error": f"no route for {scope.method} {scope.path}"})


# The fallback is an endpoint, not a route (no method, no pattern), so it is built
# with `handle` rather than a method decorator.
fallback = handle(http_scope(), fn=not_found)


def legacy(todos: TodoList, head: HttpScope) -> HttpHandler:
    """
    An opaque `HttpRouter` mounted at `/legacy`: it never learns about its
    mount point beyond the trimmed scope, echoing the prefix-stripped `path` and
    the `root_path` the mount folded the prefix into.
    """

    def handler(inputs: Stream[Inbound]) -> Stream[Outbound]:
        return stream(encode_response(json_response(200, {"path": head.path, "root_path": head.root_path})))

    return handler


async def _recover(exc: Exception) -> Response | None:
    # The app's exception policy is just a function, not a registry: `match`
    # narrows each case to its real type (no `assert isinstance`), and the final
    # `case _` returns `None` to let anything unhandled propagate.
    match exc:
        case TodoNotFound():
            return json_response(404, {"error": str(exc), "id": exc.todo_id})
        case ValidationError():
            return json_response(422, {"error": "invalid todo body", "fields": exc.error_count()})
        case _:
            return None


async def _stamp(outputs: Stream[Outbound], _head: HttpScope) -> AsyncIterator[Outbound]:
    async for event in outputs:
        if isinstance(event, ResponseStart):
            yield ResponseStart(status=event.status, headers=(*event.headers, (b"x-powered-by", b"without-web")))
        else:
            yield event


# One middleware, to show an ordinary `without-asgi` `stack` still attaches to a
# `without-web` router unchanged.
powered_by: HttpMiddleware[object] = wrap(outbound=_stamp)


def require_authorization(handler: HttpHandler, _state: object, scope: HttpScope) -> HttpHandler:
    """
    Gate a request on an `Authorization` header, short-circuiting with a 401.

    The point is *where* it applies: it is the `admin` sub-router's own
    `middleware`, so the mount carries it to every route under `/admin` and nowhere
    else (the public todo routes stay open). A middleware can replace the handler
    outright: with no credential it returns one that never reads the request and
    emits the 401, so the wrapped endpoint never runs.
    """
    if any(name == b"authorization" for name, _ in scope.headers):
        return handler

    def reject(inputs: Stream[Inbound]) -> Stream[Outbound]:
        return stream(encode_response(json_response(401, {"error": "admin requires authorization"})))

    return reject


admin: Router[TodoList] = Router(routes=(stats,), fallback=fallback, middleware=require_authorization)

todos_router: Router[TodoList] = Router(
    routes=(
        list_todos,
        create_todo,
        import_todos,
        show_todo,
        Mount("/admin", admin),
        Mount("/legacy", legacy),
    ),
    fallback=fallback,
    # `catching` is innermost (last in the stack), so the exception-mapped response
    # still flows out through `powered_by` and gets stamped like any other.
    middleware=stack(powered_by, catching(_recover)),
)


@ws("/todos/session")
async def session(todos: TodoList, inputs: Stream[WebsocketInbound]) -> AsyncIterator[WebsocketOutbound]:
    """
    Fold a live stream of todo submissions into the list across the connection.

    The websocket sibling of `POST /todos/import`, but bidirectional and
    long-lived. The handler *is* the frame processor (the same shape as
    `import_todos`): it takes the live inbound frames as a trailing
    `Stream[WebsocketInbound]` and yields outbound frames directly, no inner
    function. The connection holds a working `TodoList`, seeded from the shared
    state, and each inbound text frame is a `NewTodo` JSON folded into it, with the
    created todo and the running total sent straight back. The accumulator is a
    plain value threaded across `async for` iterations, never shared mutable state,
    so the whole connection is a *scan* over its inbound frames: same shape as the
    import fold, just kept open. A malformed frame is answered with an error frame
    rather than closing (the handshake is already accepted, the websocket analog of
    the import stream's committed `200`); a binary frame closes, since this
    protocol is text. Nothing persists past the connection, matching `POST /todos`'
    echo stance.
    """
    working = todos
    async for event in inputs:
        match event:
            case WebsocketConnect():
                yield WebsocketAccept()
            case WebsocketReceive(data):
                match data:
                    case WebsocketText(text):
                        working, reply = _folded(working, text)
                        yield WebsocketSend(WebsocketText(text=reply))
                    case WebsocketBinary():
                        yield WebsocketClose(code=1003, reason="text frames only")
                    case _ as unreachable:
                        assert_never(unreachable)
            case WebsocketDisconnect():
                return
            case _ as unreachable:
                assert_never(unreachable)


def _folded(working: TodoList, frame: str) -> tuple[TodoList, str]:
    """Fold one submission into the working list, returning it and the reply text."""
    try:
        new = NewTodo.model_validate_json(frame)
    except ValidationError as exc:
        return working, json.dumps({"ok": False, "errors": exc.error_count()})
    updated, created = working.added(new)
    return updated, json.dumps({"ok": True, "todo": _render(created), "total": len(updated.todos)})


def refuse(todos: TodoList, match: Match[WebsocketScope]) -> WebsocketHandler:
    """The websocket fallback: close an unrouted path without reading any frames."""

    def handler(inputs: Stream[WebsocketInbound]) -> Stream[WebsocketOutbound]:
        return stream((WebsocketClose(),))

    return handler


sockets: WebsocketRouter[TodoList] = WebsocketRouter(
    routes=(session,),
    fallback=refuse,
)


@asynccontextmanager
async def _hold(todos: TodoList) -> AsyncIterator[TodoList]:
    yield todos


def todos_app(todos: TodoList) -> ASGIApp:
    """
    The ASGI app: `Router.dispatch` *is* an `HttpRouter`, so it snaps straight
    onto `make_asgi_app` with no adapter, and the websocket router likewise.
    """
    return make_asgi_app(lambda: _hold(todos), http=todos_router.dispatch, websocket=sockets.dispatch)


def _schema_for(model: type) -> Mapping[str, object]:
    assert issubclass(model, BaseModel)
    return model.model_json_schema()


def todos_openapi() -> dict[str, object]:
    """
    The merged OpenAPI document: the router's path/method/path-param half plus
    each endpoint's `describe()` half, with pydantic models resolved to schema by
    the injected `schema_for`.
    """
    return openapi(todos_router, title="todos", version="1.0.0", schema_for=_schema_for)
