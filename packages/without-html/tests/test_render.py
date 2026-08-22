from __future__ import annotations

import pytest
import without_html
from markupsafe import Markup
from without_html import DOCTYPE
from without_html import br
from without_html import div
from without_html import element
from without_html import head
from without_html import html
from without_html import img
from without_html import li
from without_html import p
from without_html import render
from without_html import script
from without_html import span
from without_html import style
from without_html import title
from without_html import ul


def test_empty_element_renders_open_and_close_tags() -> None:
    assert render(div()) == "<div></div>"


def test_text_child_renders_between_the_tags() -> None:
    assert render(p(children="hello")) == "<p>hello</p>"


def test_children_render_in_order() -> None:
    assert render(div(children=[span(children="first"), span(children="second")])) == (
        "<div><span>first</span><span>second</span></div>"
    )


def test_nested_elements_render_depth_first() -> None:
    assert render(div(children=p(children=span(children="deep")))) == "<div><p><span>deep</span></p></div>"


def test_none_children_render_nothing() -> None:
    assert render(div(children=[None, p(children="kept"), None])) == "<div><p>kept</p></div>"


def test_a_generator_of_children_is_flattened() -> None:
    assert render(ul(children=(li(children=str(n)) for n in (7, 8)))) == "<ul><li>7</li><li>8</li></ul>"


def test_an_unpacked_iterable_of_children_renders_in_order() -> None:
    rest = [span(children="b"), span(children="c")]
    assert render(div(children=[span(children="a"), *rest])) == (
        "<div><span>a</span><span>b</span><span>c</span></div>"
    )


def test_a_nested_iterable_is_rejected_with_the_unpacking_it_needs() -> None:
    # Flattening one level is what `children_of` does; flattening further would mean an
    # element could hold a list, which is neither hashable nor safe from the caller.
    nested = div(children=[span(children="a"), [span(children="b")]])  # type: ignore[list-item]
    with pytest.raises(TypeError, match=r"unpack it with `\*`"):
        render(nested)


def test_an_element_built_from_a_generator_renders_the_same_twice() -> None:
    # The generator is consumed when the element is built, so the element stays a value
    # rather than a one-shot view of an exhausted iterator.
    listing = ul(children=(li(children=str(n)) for n in (1, 2)))
    assert render(listing) == render(listing)


def test_a_bare_list_of_nodes_renders_as_a_fragment() -> None:
    assert render([p(children="one"), p(children="two")]) == "<p>one</p><p>two</p>"


def test_a_bare_string_renders_as_escaped_text() -> None:
    assert render("2 < 3") == "2 &lt; 3"


def test_the_doctype_prefixes_a_document() -> None:
    assert render([DOCTYPE, html(children=head(children=title(children="t")))]) == (
        "<!doctype html><html><head><title>t</title></head></html>"
    )


def test_a_void_element_has_no_closing_tag() -> None:
    assert render(br()) == "<br>"


def test_a_void_element_still_carries_attributes() -> None:
    assert render(img(attrs={"src": "/logo.png", "alt": "logo"})) == '<img src="/logo.png" alt="logo">'


def test_the_generic_factory_rejects_a_void_tag() -> None:
    # Void tags all have named constructors that return a `VoidElement`, and a custom
    # element is never void, so there is no case for building one this way.
    with pytest.raises(ValueError, match="void element"):
        element("br")


def test_raw_text_content_is_not_escaped() -> None:
    assert render(script(children=Markup("if (a < b && c) f();"))) == "<script>if (a < b && c) f();</script>"


def test_a_raw_text_element_rejects_unmarked_content() -> None:
    with pytest.raises(ValueError, match="must be `Markup`"):
        script(children="alert(1)")  # type: ignore[arg-type]


@pytest.mark.parametrize("tag", ["script", "style"])
def test_the_generic_factory_rejects_a_raw_text_tag(tag: str) -> None:
    with pytest.raises(ValueError, match="not parsed as markup"):
        element(tag)


def test_a_style_element_takes_markup_content() -> None:
    assert render(style(children=Markup("a > b { color: red }"))) == "<style>a > b { color: red }</style>"


def test_an_unknown_tag_renders_with_a_closing_tag() -> None:
    assert render(element("x-chart", attrs={"data-series": "[1,2]"})) == '<x-chart data-series="[1,2]"></x-chart>'


def test_a_markup_subclass_still_renders_verbatim() -> None:
    # `children_of` dispatches on exact type first for speed, so a subclass has to reach
    # the `isinstance` checks behind that. Without them a `Markup` subclass is a plain
    # iterable of characters, and its markup comes out escaped.
    class Fragment(Markup):
        __slots__ = ()

    assert render(p(children=Fragment("<em>x</em>"))) == "<p><em>x</em></p>"


def test_a_value_that_cannot_render_is_rejected() -> None:
    with pytest.raises(TypeError, match="not renderable"):
        render(div(children=[object()]))  # type: ignore[list-item]


GENERATED = {
    name: constructor
    for name in without_html.__all__
    if getattr(constructor := getattr(without_html, name), "__module__", None) == "without_html.elements"
}


@pytest.mark.parametrize("name", GENERATED, ids=GENERATED)
def test_every_generated_constructor_builds_its_own_tag(name: str) -> None:
    # The constructors are generated from one tag list, so what is worth pinning is that
    # each one carries the tag it is named for: a slip in the generator would otherwise
    # produce a whole vocabulary of elements that render as something else.
    tag = name.removesuffix("_")
    empty = GENERATED[name]()
    assert empty.tag == tag
    assert render(empty) in (f"<{tag}>", f"<{tag}></{tag}>")
