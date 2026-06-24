from __future__ import annotations

import pytest
from without_web import CatchAll
from without_web import Literal
from without_web import Param
from without_web import parse_pattern
from without_web import split_path


def test_split_path_is_trailing_slash_insensitive() -> None:
    assert split_path("/todos") == ("todos",) == split_path("/todos/")


def test_split_path_of_root_is_empty() -> None:
    assert split_path("/") == ()


def test_parses_literal_typed_param_and_catchall() -> None:
    assert parse_pattern("/todos/{id:int}/{rest:path}") == (Literal("todos"), Param("id", "int"), CatchAll("rest"))


def test_a_bare_param_defaults_to_the_str_converter() -> None:
    assert parse_pattern("/users/{name}") == (Literal("users"), Param("name", "str"))


def test_catch_all_must_be_the_last_segment() -> None:
    with pytest.raises(ValueError):
        parse_pattern("/{rest:path}/tail")


def test_a_partial_segment_parameter_is_rejected() -> None:
    with pytest.raises(ValueError):
        parse_pattern("/report.{ext}")


def test_an_invalid_parameter_name_is_rejected() -> None:
    with pytest.raises(ValueError):
        parse_pattern("/{1st:int}")
