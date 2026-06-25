from __future__ import annotations

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
from without_web import INT
from without_web import Match
from without_web import Mount
from without_web import Router
from without_web import WebsocketRouter
from without_web import body
from without_web import get
from without_web import handle
from without_web import http_scope
from without_web import json_response
from without_web import openapi
from without_web import path_param
from without_web import post
from without_web import query_param
from without_web import ws

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
# todo without persisting it.


def _render(todo: Todo) -> dict[str, object]:
    return {"id": todo.id, "title": todo.title, "done": todo.done}


def _done(values: list[str]) -> bool | None:
    """The `done` query filter: `True`/`False` when present, `None` when absent.

    A repeated `?done=` honors the last value, the usual last-wins convention.
    """
    return None if not values else values[-1] == "true"


done_query = query_param("done", _done, schema={"type": "boolean"})
new_todo_body = body(NewTodo.model_validate_json, schema=NewTodo)
# One token, declared once: it is the route's `{id}` segment (matched and schemed
# through `INT`) *and* the handler's typed `int` argument, and it binds the
# websocket feed's id too. No second place to keep "id" or "int" in sync.
todo_id = path_param("id", INT)


@get("/todos", done_query, summary="List todos")
def list_todos(todos: TodoList, done: bool | None) -> Response:
    return json_response(200, {"todos": [_render(todo) for todo in todos.matching(done)]})


@post("/todos", new_todo_body, summary="Create a todo")
def create_todo(todos: TodoList, new: NewTodo) -> Response:
    _list, created = todos.added(new)
    return json_response(201, _render(created))


@get(t"/todos/{todo_id}", todo_id, summary="Get one todo")
def show_todo(todos: TodoList, requested_id: int) -> Response:
    return json_response(200, _render(todos.get(requested_id)))


@get("/stats", summary="Todo counts")
def stats(todos: TodoList) -> Response:
    return json_response(200, {"total": len(todos.matching(None)), "done": len(todos.matching(True))})


def not_found(todos: TodoList, scope: HttpScope) -> Response:
    return json_response(404, {"error": f"no route for {scope.method} {scope.path}"})


# The fallback is an endpoint, not a route (no method, no pattern), so it is built
# with `handle` rather than a method decorator.
fallback = handle(http_scope(), fn=not_found)


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


admin: Router[TodoList] = Router(routes=(stats,), fallback=fallback)

todos_router: Router[TodoList] = Router(
    routes=(
        list_todos,
        create_todo,
        show_todo,
        Mount("/admin", admin),
        Mount("/legacy", legacy),
    ),
    fallback=fallback,
    middleware=stack(powered_by),
    exception_handlers={TodoNotFound: _on_missing_todo, ValidationError: _on_invalid_body},
)


@ws(t"/todos/{todo_id}/events", todo_id)
def feed(todos: TodoList, requested_id: int) -> WebsocketHandler:
    """Stream a todo's title prefixed onto each text frame; reject an unknown id.

    The same `todo_id` token names the `{id}` segment and is passed as the typed
    `int` argument. The lookup runs *inside* the processor, before
    `WebsocketAccept`, so a `TodoNotFound` is raised while the connection can
    still be rejected and the websocket exception handler maps it to a close (the
    equivalent of the HTTP 404)."""

    async def processor(inputs: Stream[WebsocketInbound]) -> AsyncIterator[WebsocketOutbound]:
        todo = todos.get(requested_id)
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
    routes=(feed,),
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
