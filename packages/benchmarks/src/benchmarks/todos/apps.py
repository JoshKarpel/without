from __future__ import annotations

from fastapi import FastAPI
from fastapi import HTTPException
from integration.todos.app import todos_app
from integration.todos.core import NewTodo
from integration.todos.core import Todo
from integration.todos.core import TodoList
from integration.todos.core import TodoNotFound
from without_asgi import ASGIApp

# Two shells over the *same* todo core, so a benchmark measures framework + server
# overhead, not domain logic: `without_todos` is the without-web app (via
# `integration`), `fastapi_todos` is the idiomatic FastAPI equivalent. Both parse
# the same `NewTodo`, fold the same immutable `TodoList`, and render the same
# shape; neither persists past the request (the create path echoes the would-be
# todo), matching `integration`'s echo stance so the two stacks do identical work.


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

    @app.get("/todos")
    def list_todos(done: bool | None = None) -> dict[str, object]:
        return {"todos": [render(todo) for todo in todos.matching(done)]}

    @app.get("/todos/{todo_id}")
    def show_todo(todo_id: int) -> dict[str, object]:
        try:
            return render(todos.get(todo_id))
        except TodoNotFound as exc:
            raise HTTPException(status_code=404, detail={"error": str(exc), "id": exc.todo_id}) from exc

    @app.post("/todos", status_code=201)
    def create_todo(new: NewTodo) -> dict[str, object]:
        _updated, created = todos.added(new)
        return render(created)

    return app
