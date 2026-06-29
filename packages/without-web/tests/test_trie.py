from __future__ import annotations

import pytest
from without_web import INT
from without_web import PATH
from without_web import STR
from without_web.patterns import CatchAll
from without_web.patterns import Literal
from without_web.patterns import Param
from without_web.patterns import Segment
from without_web.trie import Node
from without_web.trie import build
from without_web.trie import walk


def _tree(*routes: tuple[tuple[Segment, ...], str]) -> Node[str]:
    return build(list(routes))


def test_a_literal_segment_beats_a_typed_parameter() -> None:
    tree = _tree(
        ((Literal("todos"), Literal("new")), "literal"),
        ((Literal("todos"), Param("id", INT)), "param"),
    )
    found = walk(tree, ("todos", "new"))
    assert found is not None and found.leaf == "literal"


def test_a_parameter_binds_the_converted_value() -> None:
    tree = _tree(((Literal("todos"), Param("id", INT)), "show"))
    found = walk(tree, ("todos", "42"))
    assert found is not None and found.leaf == "show" and found.params == {"id": 42}


def test_a_typed_parameter_is_tried_before_a_str_sibling() -> None:
    tree = _tree(((Literal("x"), Param("n", INT)), "int"), ((Literal("x"), Param("n", STR)), "str"))
    numeric = walk(tree, ("x", "7"))
    assert numeric is not None and numeric.leaf == "int" and numeric.params == {"n": 7}


def test_a_rejected_converter_backtracks_to_a_str_sibling() -> None:
    tree = _tree(((Literal("x"), Param("n", INT)), "int"), ((Literal("x"), Param("n", STR)), "str"))
    word = walk(tree, ("x", "abc"))
    assert word is not None and word.leaf == "str" and word.params == {"n": "abc"}


def test_a_deeper_dead_end_backtracks_to_a_sibling_branch() -> None:
    # The int branch converts "7" but then dead-ends (it only has `/done`); the
    # walk must fall back to the str branch, which has `/items`.
    tree = _tree(
        ((Param("a", INT), Literal("done")), "int-done"),
        ((Param("a", STR), Literal("items")), "str-items"),
    )
    found = walk(tree, ("7", "items"))
    assert found is not None and found.leaf == "str-items" and found.params == {"a": "7"}


def test_a_catch_all_captures_the_remaining_segments_joined() -> None:
    tree = _tree(((Literal("files"), CatchAll("rest", PATH)), "files"))
    found = walk(tree, ("files", "a", "b", "c"))
    assert found is not None and found.params == {"rest": "a/b/c"}


def test_a_catch_all_binds_the_converted_remainder() -> None:
    tree = _tree(((Literal("ids"), CatchAll("n", INT)), "ids"))
    found = walk(tree, ("ids", "42"))
    assert found is not None and found.leaf == "ids" and found.params == {"n": 42}


def test_a_catch_all_whose_converter_rejects_the_remainder_returns_none() -> None:
    tree = _tree(((Literal("ids"), CatchAll("n", INT)), "ids"))
    assert walk(tree, ("ids", "a", "b", "c")) is None


def test_an_unmatched_path_returns_none() -> None:
    tree = _tree(((Literal("todos"),), "todos"))
    assert walk(tree, ("nope",)) is None


def test_a_duplicate_route_is_a_build_error() -> None:
    with pytest.raises(ValueError):
        _tree(((Literal("todos"),), "first"), ((Literal("todos"),), "second"))


def test_a_segment_after_a_catch_all_is_a_build_error() -> None:
    with pytest.raises(ValueError):
        _tree(((CatchAll("rest", PATH), Literal("more")), "unreachable"))
