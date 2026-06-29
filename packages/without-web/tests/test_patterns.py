from __future__ import annotations

from without_web import split_path


def test_split_path_is_trailing_slash_insensitive() -> None:
    assert split_path("/todos") == ("todos",) == split_path("/todos/")


def test_split_path_of_root_is_empty() -> None:
    assert split_path("/") == ()


def test_split_path_keeps_interior_segments_in_order() -> None:
    assert split_path("/todos/42/events") == ("todos", "42", "events")
