from __future__ import annotations

import pytest
import without_html
from markupsafe import Markup
from without_html import DOCTYPE
from without_html import Element
from without_html import RawTextElementConstructor
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
from without_html.nodes import RAW_TEXT_TAGS
from without_html.nodes import VOID_TAGS
from without_html.render import TAG_MARKUP_CAPACITY
from without_html.render import tag_markup

from .helpers import Spacer
from .helpers import Widget


def test_empty_element_renders_open_and_close_tags() -> None:
    assert render(div()) == "<div></div>"


def test_text_child_renders_between_the_tags() -> None:
    assert render(p(children="hello")) == "<p>hello</p>"


def test_children_render_in_order() -> None:
    assert render(div(children=[span(children="first"), span(children="second")])) == (
        "<div><span>first</span><span>second</span></div>"
    )


def test_text_followed_by_an_element_renders_both() -> None:
    # A lone text child closes its element in place; a second child sends the whole run
    # back through the stack, so the two-child case is what pins where that line falls.
    assert render(div(children=["2 < 3", span(children="x")])) == "<div>2 &lt; 3<span>x</span></div>"


def test_nested_elements_render_depth_first() -> None:
    assert render(div(children=p(children=span(children="deep")))) == "<div><p><span>deep</span></p></div>"


def test_none_children_render_nothing() -> None:
    assert render(div(children=[None, p(children="kept"), None])) == "<div><p>kept</p></div>"


def test_a_generator_of_children_is_flattened() -> None:
    assert render(ul(children=(li(children=str(n)) for n in (7, 8)))) == "<ul><li>7</li><li>8</li></ul>"


def test_an_iterator_that_is_not_a_generator_is_still_flattened() -> None:
    # Lists, tuples, and generators are the shapes `children_of` names outright; anything
    # else iterable arrives through the `Iterable` check behind them.
    rows = reversed([li(children="7"), li(children="8")])
    assert render(ul(children=rows)) == "<ul><li>8</li><li>7</li></ul>"


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


@pytest.mark.parametrize(
    ("construct", "tag"),
    [(script, "script"), (style, "style")],
    ids=["script", "style"],
)
def test_a_raw_text_element_rejects_unmarked_content(construct: RawTextElementConstructor, tag: str) -> None:
    message = rf"^<{tag}> content is not parsed as markup, so it must be `Markup`$"
    with pytest.raises(ValueError, match=message):
        construct(children="alert(1)")  # type: ignore[arg-type]


@pytest.mark.parametrize("tag", ["script", "style"])
def test_the_generic_factory_rejects_a_raw_text_tag(tag: str) -> None:
    with pytest.raises(ValueError, match="not parsed as markup"):
        element(tag)


def test_a_style_element_takes_markup_content() -> None:
    assert render(style(children=Markup("a > b { color: red }"))) == "<style>a > b { color: red }</style>"


def test_an_unknown_tag_renders_with_a_closing_tag() -> None:
    assert render(element("x-chart", cls="wide", attrs={"data-series": "[1,2]"}, children="fallback")) == (
        '<x-chart class="wide" data-series="[1,2]">fallback</x-chart>'
    )


def test_tag_markup_is_built_once_per_tag_and_reused() -> None:
    # The renderer's per-tag constants are memoized, so the first element with a tag
    # builds them and every later one reads back the same objects. Asserted cold first,
    # because a warm memo satisfies the identity check below without ever building
    # anything.
    assert tag_markup.cache_info().currsize == 0
    built = tag_markup("x-memo")
    assert built == ("<x-memo", Markup("</x-memo>"))
    assert tag_markup("x-memo") is built


def test_the_tag_memo_stops_growing_at_its_capacity() -> None:
    # Keyed on a tag, and a tag can be built from outside input, so an unbounded memo
    # would grow for the life of the process on markup an attacker chooses.
    for n in range(TAG_MARKUP_CAPACITY * 2):
        tag_markup(f"x-{n}")
    assert tag_markup.cache_info().currsize == TAG_MARKUP_CAPACITY


def test_a_markup_subclass_still_renders_verbatim() -> None:
    # `children_of` dispatches on exact type first for speed, so a subclass has to reach
    # the `isinstance` checks behind that. Without them a `Markup` subclass is a plain
    # iterable of characters, and its markup comes out escaped.
    class Fragment(Markup):
        __slots__ = ()

    assert render(p(children=Fragment("<em>x</em>"))) == "<p><em>x</em></p>"


def test_a_string_subclass_child_is_still_escaped() -> None:
    # The other side of the same dispatch: a `str` subclass that is not `Markup` carries
    # no promise about its content, so it goes through the escape like any other text.
    class Loud(str):
        __slots__ = ()

    assert render(p(children=Loud("2 < 3"))) == "<p>2 &lt; 3</p>"


def test_a_string_subclass_that_knows_its_own_markup_renders_verbatim() -> None:
    # Django's `SafeString` is exactly this shape: a `str` carrying `__html__` and no
    # relation to `Markup`. It reaches the `str` arm before the `SupportsHtml` one, so
    # a `Markup` test there would take it for ordinary text and escape markup its author
    # had already declared safe.
    class SafeText(str):
        __slots__ = ()

        def __html__(self) -> str:
            return str(self)

    assert render(p(children=SafeText("<em>x</em>"))) == "<p><em>x</em></p>"


def test_a_mapping_in_a_child_position_is_rejected_rather_than_rendering_its_keys() -> None:
    # A `Mapping` satisfies `Iterable[Child]`, so the type checker passes it and the
    # flattening arm would render its keys and silently drop everything else.
    with pytest.raises(TypeError, match="renders only its keys"):
        render(div(children={"label": "value"}))


def test_a_set_in_a_child_position_is_rejected_rather_than_rendering_in_any_order() -> None:
    with pytest.raises(TypeError, match="varies between runs"):
        render(div(children={"a", "b"}))


@pytest.mark.parametrize(
    ("children", "expected"),
    [
        ((), '<x-widget id="w"></x-widget>'),
        (("body & more",), '<x-widget id="w">body &amp; more</x-widget>'),
        ((Element("b", (), ("c",)),), '<x-widget id="w"><b>c</b></x-widget>'),
        (("a", Element("b", (), ("c",))), '<x-widget id="w">a<b>c</b></x-widget>'),
    ],
    ids=["no children", "one text child", "one element child", "several children"],
)
def test_an_element_subclass_renders_as_its_element(children: tuple[object, ...], expected: str) -> None:
    # A lone text child closes in place; anything else goes back through the stack, and
    # which side of that a single non-text child falls on is the part worth pinning.
    assert render(Widget("x-widget", (("id", "w"),), children)) == expected  # type: ignore[arg-type]


def test_a_void_element_subclass_renders_as_its_element() -> None:
    assert render(Spacer("x-spacer", (("size", "3"),))) == '<x-spacer size="3">'


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


@pytest.mark.parametrize("name", GENERATED, ids=GENERATED)
def test_every_generated_constructor_carries_its_arguments_into_its_element(name: str) -> None:
    # The other half of what the generator can get wrong: a constructor that renders the
    # right tag but drops an argument produces a plausible element with no classes, no
    # attributes, or no content, which nothing downstream can tell from an empty one.
    tag = name.removesuffix("_")
    opening = f'<{tag} class="card" data-k="v"'
    if tag in VOID_TAGS:
        assert render(GENERATED[name](cls="card", attrs={"data-k": "v"})) == f"{opening}>"
        return
    content = Markup("<em>x</em>") if tag in RAW_TEXT_TAGS else span(children="x")
    built = GENERATED[name](cls="card", attrs={"data-k": "v"}, children=content)
    assert render(built) == f"{opening}>{render(content)}</{tag}>"
