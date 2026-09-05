from __future__ import annotations

import pytest
from without_web import INT
from without_web import PATH
from without_web import STR
from without_web import Converter
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
    assert found is not None
    assert found.leaf == "literal"


def test_a_parameter_binds_the_converted_value() -> None:
    tree = _tree(((Literal("todos"), Param("id", INT)), "show"))
    found = walk(tree, ("todos", "42"))
    assert found is not None
    assert found.leaf == "show"
    assert found.params == {"id": 42}


def test_a_typed_parameter_is_tried_before_a_str_sibling() -> None:
    tree = _tree(((Literal("x"), Param("n", INT)), "int"), ((Literal("x"), Param("n", STR)), "str"))
    numeric = walk(tree, ("x", "7"))
    assert numeric is not None
    assert numeric.leaf == "int"
    assert numeric.params == {"n": 7}


def test_a_typed_parameter_wins_even_when_registered_after_str() -> None:
    # STR is registered first, so precedence must reorder INT ahead of it; relying
    # on insertion order alone (or giving both the same precedence) matches str.
    tree = _tree(((Literal("x"), Param("n", STR)), "str"), ((Literal("x"), Param("n", INT)), "int"))
    numeric = walk(tree, ("x", "7"))
    assert numeric is not None
    assert numeric.leaf == "int"
    assert numeric.params == {"n": 7}


def test_a_rejected_converter_backtracks_to_a_str_sibling() -> None:
    tree = _tree(((Literal("x"), Param("n", INT)), "int"), ((Literal("x"), Param("n", STR)), "str"))
    word = walk(tree, ("x", "abc"))
    assert word is not None
    assert word.leaf == "str"
    assert word.params == {"n": "abc"}


def test_a_deeper_dead_end_backtracks_to_a_sibling_branch() -> None:
    # The int branch converts "7" but then dead-ends (it only has `/done`); the
    # walk must fall back to the str branch, which has `/items`.
    tree = _tree(
        ((Param("a", INT), Literal("done")), "int-done"),
        ((Param("a", STR), Literal("items")), "str-items"),
    )
    found = walk(tree, ("7", "items"))
    assert found is not None
    assert found.leaf == "str-items"
    assert found.params == {"a": "7"}


def test_a_catch_all_captures_the_remaining_segments_joined() -> None:
    tree = _tree(((Literal("files"), CatchAll("rest", PATH)), "files"))
    found = walk(tree, ("files", "a", "b", "c"))
    assert found is not None
    assert found.params == {"rest": "a/b/c"}


def test_a_catch_all_binds_the_converted_remainder() -> None:
    tree = _tree(((Literal("ids"), CatchAll("n", INT)), "ids"))
    found = walk(tree, ("ids", "42"))
    assert found is not None
    assert found.leaf == "ids"
    assert found.params == {"n": 42}


def test_a_catch_all_whose_converter_rejects_the_remainder_returns_none() -> None:
    tree = _tree(((Literal("ids"), CatchAll("n", INT)), "ids"))
    assert walk(tree, ("ids", "a", "b", "c")) is None


def test_an_unmatched_path_returns_none() -> None:
    tree = _tree(((Literal("todos"),), "todos"))
    assert walk(tree, ("nope",)) is None


def test_a_duplicate_route_is_a_build_error() -> None:
    with pytest.raises(ValueError, match=r"^duplicate route: two endpoints resolve to the same path$"):
        _tree(((Literal("todos"),), "first"), ((Literal("todos"),), "second"))


def test_a_segment_after_a_catch_all_is_a_build_error() -> None:
    with pytest.raises(ValueError, match=r"^invalid route: a catch-all must be the last segment$"):
        _tree(((CatchAll("rest", PATH), Literal("more")), "unreachable"))


def test_two_routes_that_differ_only_in_a_parameter_name_are_a_duplicate() -> None:
    # Branches are keyed on the converter alone, so what a route *calls* its
    # parameter cannot make it a different route: no request could tell
    # `/u/{id:int}` from `/u/{other:int}`, and both were silently reachable
    # before, first-wins.
    with pytest.raises(ValueError, match=r"^duplicate route: two endpoints resolve to the same path$"):
        _tree(
            ((Literal("u"), Param("id", INT)), "first"),
            ((Literal("u"), Param("other", INT)), "second"),
        )


def test_routes_sharing_a_branch_each_bind_their_own_parameter_name() -> None:
    # The other half of merging on the converter: one branch, but the name is a
    # property of the route, so each leaf reports what its own segment called it.
    tree = _tree(
        ((Literal("u"), Param("id", INT), Literal("a")), "leaf-a"),
        ((Literal("u"), Param("other", INT), Literal("b")), "leaf-b"),
    )
    first = walk(tree, ("u", "7", "a"))
    second = walk(tree, ("u", "7", "b"))
    assert first is not None
    assert second is not None
    assert (first.leaf, first.params) == ("leaf-a", {"id": 7})
    assert (second.leaf, second.params) == ("leaf-b", {"other": 7})


def test_two_catch_alls_under_one_parent_are_a_build_error() -> None:
    # A catch-all consumes every remaining segment, so it has no more specific
    # sibling to be tried before: whichever the walk reaches first answers for
    # both however they convert. `PATH` accepts anything at all, so a second
    # catch-all under a parent that already has one (which is what `delegate`
    # mounts) would be silently unreachable rather than merely lower-precedence.
    numeric = Converter(
        name="numbers",
        parse=lambda raw: tuple(int(part) for part in raw.split("/")),
        schema={"type": "string"},
    )
    with pytest.raises(ValueError, match=r"^ambiguous route: two catch-alls resolve at the same path$"):
        _tree(
            ((Literal("f"), CatchAll("rest", PATH)), "route-text"),
            ((Literal("f"), CatchAll("numbers", numeric)), "route-numeric"),
        )


def test_two_catch_alls_with_one_converter_are_a_duplicate_route() -> None:
    # The other spelling of the same fault: sharing a converter they share a
    # branch, so it is the leaf that collides rather than the segment.
    with pytest.raises(ValueError, match=r"^duplicate route: two endpoints resolve to the same path$"):
        _tree(
            ((Literal("f"), CatchAll("rest", PATH)), "first"),
            ((Literal("f"), CatchAll("other", PATH)), "second"),
        )


def test_a_route_binds_the_value_from_the_converter_it_declared() -> None:
    # Converters sharing a name but not a parse are distinct branch keys, so
    # neither route can be handed the other's value. Comparing `parse` is what
    # buys this: by `name` alone these two would merge onto one node and the
    # first one inserted would supply the parse for both.
    #
    # Every shipped converter is a module-level singleton, so equal names have
    # always meant the same object and the distinction never showed. A converter
    # *factory* (one per enum, say) is what surfaces it.
    first = Converter(name="colour", parse=lambda raw: f"first:{raw}", schema={"type": "string"})
    second = Converter(name="colour", parse=lambda raw: f"second:{raw}", schema={"type": "string"})
    tree = _tree(
        ((Literal("x"), Param("c", first), Literal("one")), "route-one"),
        ((Literal("x"), Param("c", second), Literal("two")), "route-two"),
    )
    found = walk(tree, ("x", "red", "two"))
    assert found is not None
    assert found.leaf == "route-two"
    assert found.params == {"c": "second:red"}


def test_a_segment_is_converted_once_when_siblings_share_a_converter() -> None:
    # The *parameter* name is part of the branch key too, so two params that
    # differ only in what they bind become siblings that match exactly the same
    # segments. Reaching `/b` then converts "7" against the `id` branch, dead-ends
    # under it, and converts the identical segment again against `other`.
    #
    # Both routes do resolve; the cost is the redundant conversion, which grows
    # with the number of same-converter siblings. A trie that keyed the branch on
    # the converter alone and carried parameter names down to the leaf would
    # convert each segment once.
    converted: list[str] = []

    def counting(raw: str) -> int:
        converted.append(raw)
        return int(raw)

    number = Converter(name="int", parse=counting, schema={"type": "integer"})
    tree = _tree(
        ((Literal("u"), Param("id", number), Literal("a")), "leaf-a"),
        ((Literal("u"), Param("other", number), Literal("b")), "leaf-b"),
    )
    found = walk(tree, ("u", "7", "b"))
    assert found is not None
    assert found.leaf == "leaf-b"
    assert converted == ["7"]
