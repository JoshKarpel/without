from __future__ import annotations

import pytest
from integration.todos.core import NewTodo
from integration.todos.core import Todo
from integration.todos.core import TodoList
from integration.todos.core import TodoNotFound


def _todos() -> TodoList:
    return TodoList({1: Todo(id=1, title="write", done=False), 2: Todo(id=2, title="ship", done=True)})


def test_matching_returns_every_todo_in_id_order() -> None:
    assert _todos().matching(None) == (Todo(1, "write", False), Todo(2, "ship", True))


@pytest.mark.parametrize(("done", "expected"), [(True, (Todo(2, "ship", True),)), (False, (Todo(1, "write", False),))])
def test_matching_filters_by_completion(done: bool, expected: tuple[Todo, ...]) -> None:
    assert _todos().matching(done) == expected


def test_get_returns_the_requested_todo() -> None:
    assert _todos().get(2) == Todo(2, "ship", True)


def test_get_raises_with_the_missing_id() -> None:
    with pytest.raises(TodoNotFound) as caught:
        _todos().get(99)
    assert caught.value.todo_id == 99


def test_added_appends_under_the_next_id_without_mutating_the_original() -> None:
    original = _todos()
    updated, created = original.added(NewTodo(title="deploy", done=True))
    assert created == Todo(3, "deploy", True)
    assert updated.get(3) == created
    assert 3 not in original.todos
