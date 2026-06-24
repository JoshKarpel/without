from __future__ import annotations

import pytest
from without_web import DEFAULT_CONVERTERS
from without_web import parse_pattern
from without_web.patterns import Segment
from without_web.trie import Node
from without_web.trie import build
from without_web.trie import walk


def _tree(*routes: tuple[str, str]) -> Node[str]:
    table: list[tuple[tuple[Segment, ...], str]] = [(parse_pattern(pattern), leaf) for pattern, leaf in routes]
    return build(table, DEFAULT_CONVERTERS)


def test_a_literal_segment_beats_a_typed_parameter() -> None:
    tree = _tree(("/todos/new", "literal"), ("/todos/{id:int}", "param"))
    found = walk(tree, ("todos", "new"), DEFAULT_CONVERTERS)
    assert found is not None and found.leaf == "literal"


def test_a_parameter_binds_the_converted_value() -> None:
    tree = _tree(("/todos/{id:int}", "show"))
    found = walk(tree, ("todos", "42"), DEFAULT_CONVERTERS)
    assert found is not None and found.leaf == "show" and found.params == {"id": 42}


def test_a_typed_parameter_is_tried_before_a_str_sibling() -> None:
    tree = _tree(("/x/{n:int}", "int"), ("/x/{n:str}", "str"))
    numeric = walk(tree, ("x", "7"), DEFAULT_CONVERTERS)
    assert numeric is not None and numeric.leaf == "int" and numeric.params == {"n": 7}


def test_a_rejected_converter_backtracks_to_a_str_sibling() -> None:
    tree = _tree(("/x/{n:int}", "int"), ("/x/{n:str}", "str"))
    word = walk(tree, ("x", "abc"), DEFAULT_CONVERTERS)
    assert word is not None and word.leaf == "str" and word.params == {"n": "abc"}


def test_a_deeper_dead_end_backtracks_to_a_sibling_branch() -> None:
    # The int branch converts "7" but then dead-ends (it only has `/done`); the
    # walk must fall back to the str branch, which has `/items`.
    tree = _tree(("/{a:int}/done", "int-done"), ("/{a:str}/items", "str-items"))
    found = walk(tree, ("7", "items"), DEFAULT_CONVERTERS)
    assert found is not None and found.leaf == "str-items" and found.params == {"a": "7"}


def test_a_catch_all_captures_the_remaining_segments_joined() -> None:
    tree = _tree(("/files/{rest:path}", "files"))
    found = walk(tree, ("files", "a", "b", "c"), DEFAULT_CONVERTERS)
    assert found is not None and found.params == {"rest": "a/b/c"}


def test_an_unmatched_path_returns_none() -> None:
    tree = _tree(("/todos", "todos"))
    assert walk(tree, ("nope",), DEFAULT_CONVERTERS) is None


def test_a_duplicate_route_is_a_build_error() -> None:
    with pytest.raises(ValueError):
        _tree(("/todos", "first"), ("/todos", "second"))


def test_an_unknown_converter_is_a_build_error() -> None:
    with pytest.raises(ValueError):
        _tree(("/{x:nonesuch}", "x"))
