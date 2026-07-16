from __future__ import annotations

from typing import Annotated

from fastapi import FastAPI
from fastapi import Header
from fastapi import HTTPException
from integration.todos.app import todos_app
from integration.todos.core import NewTodo
from integration.todos.core import Todo
from integration.todos.core import TodoList
from integration.todos.core import TodoNotFound
from without_asgi import ASGIApp

# The two application frameworks under test, each a shell over the *same* todo
# core, so a benchmark measures framework + server overhead, not domain logic:
# `without_todos` is the without-web app (via `integration`), `fastapi_todos` is
# the idiomatic FastAPI equivalent. Both parse the same `NewTodo`, fold the same
# immutable `TodoList`, and render the same shape; neither persists past the
# request (the create path echoes the would-be todo), matching `integration`'s
# echo stance so both frameworks do identical work per request.


def seed() -> TodoList:
    """The fixed starting list both stacks serve, so every run is reproducible."""
    return TodoList(
        {
            1: Todo(id=1, title="write the paper", done=False),
            2: Todo(id=2, title="ship the release", done=True),
            3: Todo(id=3, title="water the plants", done=False),
        }
    )


def render(todo: Todo) -> dict[str, object]:
    return {"id": todo.id, "title": todo.title, "done": todo.done}


def without_todos() -> ASGIApp:
    return todos_app(seed())


def fastapi_todos() -> FastAPI:
    todos = seed()
    app = FastAPI()

    # `async def` handlers, to match without-web: FastAPI runs these on the event
    # loop, whereas a sync `def` handler is dispatched to an anyio worker thread, so
    # a sync version would measure threadpool hop + GIL contention, not the
    # framework. The bodies stay pure (no await); the point is the execution model.
    @app.get("/todos")
    async def list_todos(done: bool | None = None) -> dict[str, object]:
        return {"todos": [render(todo) for todo in todos.matching(done)]}

    @app.get("/todos/{todo_id}")
    async def show_todo(todo_id: int) -> dict[str, object]:
        try:
            return render(todos.get(todo_id))
        except TodoNotFound as exc:
            raise HTTPException(status_code=404, detail={"error": str(exc), "id": exc.todo_id}) from exc

    # `url_path_for` mirrors without-web's `url_for`: the URL is reverse-routed from
    # the `show_todo` route rather than formatted by hand, so both stacks pay for the
    # reversal rather than one of them getting a cheaper f-string.
    @app.post("/todos", status_code=201)
    async def create_todo(new: NewTodo, idempotency_key: Annotated[str, Header()]) -> dict[str, object]:
        _updated, created = todos.added(new)
        return {
            **render(created),
            "url": str(app.url_path_for("show_todo", todo_id=created.id)),
            "idempotency_key": idempotency_key,
        }

    return app
