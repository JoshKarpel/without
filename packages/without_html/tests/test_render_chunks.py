from __future__ import annotations

import pytest
from markupsafe import Markup
from without_html import DOCTYPE
from without_html import Element
from without_html import Node
from without_html import body
from without_html import div
from without_html import head
from without_html import html
from without_html import img
from without_html import li
from without_html import p
from without_html import render
from without_html import render_chunks
from without_html import span
from without_html import title
from without_html import ul

from .helpers import Spacer
from .helpers import Widget


def page(rows: int) -> Node:
    return [
        DOCTYPE,
        html(
            children=[
                head(children=title(children="runs & things")),
                body(
                    children=[
                        img(attrs={"src": "/logo.png"}),
                        p(children=Markup("<em>trusted</em>")),
                        ul(children=(li(cls="row", children=f"run {n} < {n + 1}") for n in range(rows))),
                    ]
                ),
            ]
        ),
    ]


TREES: dict[str, Node] = {
    "empty element": div(),
    "bare text": "2 < 3",
    "fragment": [p(children="one"), p(children="two")],
    "void element": img(attrs={"src": "/x.png"}),
    "nothing": None,
    "small page": page(3),
    "large page": page(2000),
    "text then element": div(children=["2 < 3", span(children="x")]),
    "subclass, no children": Widget("x-widget", (("id", "w"),), ()),
    "subclass, one text child": Widget("x-widget", (), ("body & more",)),
    "subclass, one element child": Widget("x-widget", (), (Element("b", (), ("c",)),)),
    "subclass, several children": Widget("x-widget", (), ("a", Element("b", (), ("c",)))),
    "void subclass": Spacer("x-spacer", (("size", "3"),)),
}


@pytest.mark.parametrize("tree", TREES.values(), ids=TREES.keys())
@pytest.mark.parametrize("fragments_per_chunk", [1, 2, 512])
def test_the_chunks_join_to_exactly_what_render_produces(tree: Node, fragments_per_chunk: int) -> None:
    assert "".join(render_chunks(tree, fragments_per_chunk=fragments_per_chunk)) == render(tree)


def test_a_large_page_arrives_in_many_chunks() -> None:
    # The point of the walk: a caller sees markup before the tree is finished, rather
    # than one chunk that is the whole string by another name.
    assert len(list(render_chunks(page(2000)))) > 10


def test_a_small_tree_arrives_in_one_chunk() -> None:
    assert list(render_chunks(div(children="hi"))) == ["<div>hi</div>"]


def test_a_batch_that_joins_to_nothing_does_not_end_the_stream() -> None:
    # Empty text children make a chunk that joins to nothing; the rest of the tree still
    # has to arrive.
    tree = div(children=["" for _ in range(2000)])
    assert "".join(render_chunks(tree, fragments_per_chunk=8)) == "<div></div>"


def test_nothing_renders_to_no_chunks() -> None:
    assert list(render_chunks(None)) == []


def test_the_first_chunk_arrives_without_walking_the_whole_tree() -> None:
    # The property that makes this worth having over `render`: a client can be reading
    # the head of the page while the tail is still unrendered.
    visited: list[int] = []

    class Counted:
        def __init__(self, index: int) -> None:
            self.index = index

        def __html__(self) -> str:
            visited.append(self.index)
            return f"<i>{self.index}</i>"

    tree = div(children=[li(children=Counted(n)) for n in range(1000)])
    first = next(render_chunks(tree, fragments_per_chunk=8))

    assert first.startswith("<div><li><i>0</i></li>")
    assert len(visited) < 10


def test_an_element_streams_the_same_bytes_twice() -> None:
    listing = ul(children=(li(children=str(n)) for n in (1, 2)))
    assert "".join(render_chunks(listing)) == "".join(render_chunks(listing))


def test_a_markup_subclass_still_streams_verbatim() -> None:
    class Fragment(Markup):
        __slots__ = ()

    assert "".join(render_chunks(p(children=Fragment("<em>x</em>")))) == "<p><em>x</em></p>"


def test_a_string_subclass_is_still_escaped_while_streaming() -> None:
    class Loud(str):
        __slots__ = ()

    assert "".join(render_chunks(p(children=Loud("2 < 3")))) == "<p>2 &lt; 3</p>"


def test_none_between_children_streams_as_nothing() -> None:
    assert "".join(render_chunks(div(children=[None, p(children="kept"), None]))) == "<div><p>kept</p></div>"


def test_a_value_that_cannot_stream_is_rejected() -> None:
    with pytest.raises(TypeError, match="not renderable"):
        list(render_chunks(div(children=[object()])))  # type: ignore[list-item]


def test_a_nested_iterable_is_rejected_with_the_unpacking_it_needs() -> None:
    # The same rejection `render` gives, naming the same way out: the two walks are
    # generated from one source, and the diagnostic is part of what has to stay one.
    nested = div(children=[span(children="a"), [span(children="b")]])  # type: ignore[list-item]
    with pytest.raises(TypeError, match=r"unpack it with `\*`"):
        list(render_chunks(nested))


def test_escaping_matches_render_exactly() -> None:
    # `render` and `render_chunks` are generated from one walk; this is the assertion
    # that keeps them one walk if that ever stops being true.
    hostile = div(cls='a"b', attrs={"data-x": "<&>"}, children=["<script>", Markup("<em>ok</em>"), span()])
    assert "".join(render_chunks(hostile, fragments_per_chunk=1)) == render(hostile)
    assert "<script>" not in "".join(render_chunks(hostile))
