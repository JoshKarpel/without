from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from pydantic import BaseModel
from pydantic import ConfigDict


@dataclass(frozen=True, slots=True)
class Todo:
    """One task in the list, a value keyed by its `id`."""

    id: int
    title: str
    done: bool


class NewTodo(BaseModel):
    """The request body for creating a todo, parsed at the boundary.

    A pydantic model rather than a bare dict, so a missing or mistyped field is
    a `ValidationError` the shell maps to a `422`, and the same model yields the
    request-body JSON Schema the router reports for OpenAPI (one declaration,
    two consumers)."""

    model_config = ConfigDict(frozen=True)

    title: str
    done: bool = False


class TodoNotFound(Exception):
    """No todo has the requested id. A domain error, carrying no status code: the
    shell decides that a missing todo is a `404` (over HTTP) or a close (over a
    websocket)."""

    def __init__(self, todo_id: int) -> None:
        self.todo_id = todo_id
        super().__init__(f"no todo with id {todo_id}")


@dataclass(frozen=True, slots=True)
class TodoList:
    """The whole list as one immutable value: lookups in, new lists out."""

    todos: Mapping[int, Todo]

    def get(self, todo_id: int) -> Todo:
        try:
            return self.todos[todo_id]
        except KeyError:
            raise TodoNotFound(todo_id) from None

    def matching(self, done: bool | None) -> tuple[Todo, ...]:
        """Every todo in id order, optionally filtered by completion."""
        ordered = (self.todos[todo_id] for todo_id in sorted(self.todos))
        return tuple(todo for todo in ordered if done is None or todo.done == done)

    def added(self, new: NewTodo) -> tuple[TodoList, Todo]:
        """A new list with `new` appended under the next id, plus the created todo."""
        todo_id = 1 + max(self.todos, default=0)
        created = Todo(id=todo_id, title=new.title, done=new.done)
        return TodoList({**self.todos, todo_id: created}), created
