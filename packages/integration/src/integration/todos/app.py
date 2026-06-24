from __future__ import annotations

from collections.abc import AsyncIterator
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import assert_never
from urllib.parse import parse_qs

from pydantic import BaseModel
from pydantic import ValidationError
from without import Stream
from without import stream
from without_asgi import ASGIApp
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import Response
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
from without_web import Match
from without_web import Mount
from without_web import QueryParam
from without_web import RequestBodySpec
from without_web import ResponseSpec
from without_web import Router
from without_web import RouteSpec
from without_web import WebsocketRouter
from without_web import buffered
from without_web import describe
from without_web import json_response
from without_web import openapi
from without_web import route
from without_web import ws_route

from integration.todos.core import NewTodo
from integration.todos.core import Todo
from integration.todos.core import TodoList
from integration.todos.core import TodoNotFound

# A todo-list API on without-web: the canonical REST shape, chosen because it
# exercises the whole router design at once. `/todos/{id:int}` is a typed path
# parameter; `GET` vs `POST` on `/todos` is method dispatch (so a `PUT` is a 405,
# not a 404); `?done=` is a handler-owned query filter; `/admin` is a grafted
# sub-router and `/legacy` an opaque mount; `TodoNotFound`/`ValidationError` are
# mapped by exception handlers; and the routes describe themselves for OpenAPI.
# State is a single immutable `TodoList` value held for the connection's life, so
# this example stays about routing and leaves a shared mutable store (the
# actor-model question) out of scope: `POST` validates and echoes the would-be
# todo without persisting it.

_CREATED_SCHEMA: Mapping[str, object] = {
    "type": "object",
    "properties": {"id": {"type": "integer"}, "title": {"type": "string"}, "done": {"type": "boolean"}},
}


def _render(todo: Todo) -> dict[str, object]:
    return {"id": todo.id, "title": todo.title, "done": todo.done}


def _path_id(match: Match[HttpScope]) -> int:
    # The `{id:int}` converter already parsed and proved the type; the params
    # map exposes its values as plain objects, so narrow once at the read.
    todo_id = match.params["id"]
    assert isinstance(todo_id, int)
    return todo_id


def done_filter(query_string: bytes) -> bool | None:
    """The `done` query filter: `True`/`False` when present, `None` when absent.

    Query parsing is the handler's, never the router's: it reads `query_string`
    off the scope and never affects which handler was selected.
    """
    values = parse_qs(query_string.decode()).get("done")
    return None if not values else values[0] == "true"


@describe(
    RouteSpec(
        summary="List todos",
        query=(QueryParam(name="done", schema={"type": "boolean"}),),
        responses={200: ResponseSpec(media_type="application/json", description="the matching todos")},
    )
)
@buffered
def list_todos(todos: TodoList, match: Match[HttpScope], body: bytes) -> Response:
    matching = todos.matching(done_filter(match.scope.query_string))
    return json_response(200, {"todos": [_render(todo) for todo in matching]})


@describe(
    RouteSpec(
        summary="Create a todo",
        request_body=RequestBodySpec(media_type="application/json", schema=NewTodo),
        responses={
            201: ResponseSpec(media_type="application/json", schema=_CREATED_SCHEMA, description="the created todo"),
            422: ResponseSpec(media_type="application/json", description="the body failed validation"),
        },
    )
)
@buffered
def create_todo(todos: TodoList, match: Match[HttpScope], body: bytes) -> Response:
    new = NewTodo.model_validate_json(body)
    _list, created = todos.added(new)
    return json_response(201, _render(created))


@describe(
    RouteSpec(
        summary="Get one todo",
        responses={
            200: ResponseSpec(media_type="application/json", description="the todo"),
            404: ResponseSpec(media_type="application/json", description="no todo with that id"),
        },
    )
)
@buffered
def show_todo(todos: TodoList, match: Match[HttpScope], body: bytes) -> Response:
    return json_response(200, _render(todos.get(_path_id(match))))


@describe(
    RouteSpec(
        summary="Todo counts",
        responses={200: ResponseSpec(media_type="application/json", description="totals")},
    )
)
@buffered
def stats(todos: TodoList, match: Match[HttpScope], body: bytes) -> Response:
    return json_response(200, {"total": len(todos.matching(None)), "done": len(todos.matching(True))})


@buffered
def not_found(todos: TodoList, match: Match[HttpScope], body: bytes) -> Response:
    return json_response(404, {"error": f"no route for {match.scope.method} {match.scope.path}"})


def legacy(todos: TodoList, head: HttpScope) -> HttpHandler:
    """An opaque `HttpRouter` mounted at `/legacy`: it never learns about its
    mount point beyond the trimmed scope, echoing the prefix-stripped `path` and
    the `root_path` the mount folded the prefix into."""

    def handler(inputs: Stream[Inbound]) -> Stream[Outbound]:
        return stream(encode_response(json_response(200, {"path": head.path, "root_path": head.root_path})))

    return handler


def _on_missing_todo(exc: Exception) -> Response:
    assert isinstance(exc, TodoNotFound)
    return json_response(404, {"error": str(exc), "id": exc.todo_id})


def _on_invalid_body(exc: Exception) -> Response:
    assert isinstance(exc, ValidationError)
    return json_response(422, {"error": "invalid todo body", "fields": exc.error_count()})


async def _stamp(outputs: Stream[Outbound], _head: HttpScope) -> AsyncIterator[Outbound]:
    async for event in outputs:
        if isinstance(event, ResponseStart):
            yield ResponseStart(status=event.status, headers=(*event.headers, (b"x-powered-by", b"without-web")))
        else:
            yield event


# One middleware, to show an ordinary `without-asgi` `stack` still attaches to a
# `without-web` router unchanged.
powered_by: HttpMiddleware = wrap(outbound=_stamp)


admin: Router[TodoList] = Router(
    routes=(route("/stats", get=stats),),
    fallback=not_found,
)

todos_router: Router[TodoList] = Router(
    routes=(
        route("/todos", get=list_todos, post=create_todo),
        route("/todos/{id:int}", get=show_todo),
        Mount("/admin", admin),
        Mount("/legacy", legacy),
    ),
    fallback=not_found,
    middleware=stack(powered_by),
    exception_handlers={TodoNotFound: _on_missing_todo, ValidationError: _on_invalid_body},
)


def feed(todos: TodoList, match: Match[WebsocketScope]) -> WebsocketHandler:
    """Stream a todo's title prefixed onto each text frame; reject an unknown id.

    The lookup runs *inside* the processor, before `WebsocketAccept`, so a
    `TodoNotFound` is raised while the connection can still be rejected and the
    websocket exception handler maps it to a close (the equivalent of the HTTP
    404)."""
    todo_id = match.params["id"]
    assert isinstance(todo_id, int)

    async def processor(inputs: Stream[WebsocketInbound]) -> AsyncIterator[WebsocketOutbound]:
        todo = todos.get(todo_id)
        async for event in inputs:
            match event:
                case WebsocketConnect():
                    yield WebsocketAccept()
                case WebsocketReceive(data):
                    match data:
                        case WebsocketText(text):
                            yield WebsocketSend(WebsocketText(text=f"{todo.title}: {text}"))
                        case WebsocketBinary():
                            yield WebsocketClose(code=1003, reason="text frames only")
                        case _ as unreachable:
                            assert_never(unreachable)
                case WebsocketDisconnect():
                    return
                case _ as unreachable:
                    assert_never(unreachable)

    return processor


def refuse(todos: TodoList, match: Match[WebsocketScope]) -> WebsocketHandler:
    """The websocket fallback: close an unrouted path without reading any frames."""

    def handler(inputs: Stream[WebsocketInbound]) -> Stream[WebsocketOutbound]:
        return stream((WebsocketClose(),))

    return handler


def _on_missing_todo_socket(exc: Exception) -> WebsocketClose:
    assert isinstance(exc, TodoNotFound)
    return WebsocketClose(code=4404, reason=str(exc))


sockets: WebsocketRouter[TodoList] = WebsocketRouter(
    routes=(ws_route("/todos/{id:int}/events", feed),),
    fallback=refuse,
    exception_handlers={TodoNotFound: _on_missing_todo_socket},
)


@asynccontextmanager
async def _hold(todos: TodoList) -> AsyncIterator[TodoList]:
    yield todos


def todos_app(todos: TodoList) -> ASGIApp:
    """The ASGI app: `Router.dispatch` *is* an `HttpRouter`, so it snaps straight
    onto `make_asgi_app` with no adapter, and the websocket router likewise."""
    return make_asgi_app(lambda: _hold(todos), http=todos_router.dispatch, websocket=sockets.dispatch)


def _schema_for(model: type) -> Mapping[str, object]:
    assert issubclass(model, BaseModel)
    return model.model_json_schema()


def todos_openapi() -> dict[str, object]:
    """The merged OpenAPI document: the router's path/method/path-param half plus
    each endpoint's `describe()` half, with pydantic models resolved to schema by
    the injected `schema_for`."""
    return openapi(todos_router, title="todos", version="1.0.0", schema_for=_schema_for)
