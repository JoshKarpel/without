from __future__ import annotations

import pytest
from markupsafe import Markup
from without_html import Element
from without_html import br
from without_html import div
from without_html import img
from without_html import p
from without_html import render
from without_html import script
from without_html import span


def test_an_attribute_the_element_did_not_have_is_added() -> None:
    assert render(div(cls="card").with_attributes(attrs={"id": "runs"})) == '<div class="card" id="runs"></div>'


def test_an_attribute_the_element_had_is_replaced_where_it_stood() -> None:
    # Not appended: HTML keeps the *first* of a duplicated attribute, so an appended one
    # would leave the old value in force and the new one inert, silently.
    element = div(attrs={"id": "old", "data-page": "3"}).with_attributes(attrs={"id": "new"})
    assert render(element) == '<div id="new" data-page="3"></div>'


def test_a_replaced_attribute_matches_the_name_case_insensitively() -> None:
    element = div(attrs={"Data-Id": "old"}).with_attributes(attrs={"data-id": "new"})
    assert render(element) == '<div data-id="new"></div>'


@pytest.mark.parametrize("value", [None, False])
def test_an_attribute_set_to_nothing_is_removed(value: bool | None) -> None:
    element = div(attrs={"id": "runs", "data-page": "3"}).with_attributes(attrs={"id": value})
    assert render(element) == '<div data-page="3"></div>'


def test_removing_an_attribute_the_element_did_not_have_changes_nothing() -> None:
    assert render(div(attrs={"id": "runs"}).with_attributes(attrs={"hidden": None})) == '<div id="runs"></div>'


def test_classes_are_replaced_when_given() -> None:
    assert render(div(cls="card", attrs={"id": "x"}).with_attributes(cls="panel")) == '<div class="panel" id="x"></div>'


def test_classes_are_left_alone_when_not_given() -> None:
    # `cls` at its default means "not mentioned", so no transform that touches attributes
    # can drop an element's classes by omission.
    assert render(div(cls="card").with_attributes(attrs={"id": "x"})) == '<div class="card" id="x"></div>'


def test_classes_are_extended_by_naming_the_old_ones() -> None:
    assert render(div(cls="card").with_attributes(cls=("card", "active"))) == '<div class="card active"></div>'


def test_empty_classes_remove_the_attribute() -> None:
    assert render(div(cls="card", attrs={"id": "x"}).with_attributes(cls="")) == '<div id="x"></div>'


def test_an_added_attribute_value_is_escaped() -> None:
    # The whole reason this exists rather than `dataclasses.replace`: a value that reaches
    # the attribute list unescaped opens an attribute of the supplier's choosing.
    markup = render(div().with_attributes(attrs={"title": '" onmouseover="steal()'}))
    assert markup == '<div title="&#34; onmouseover=&#34;steal()"></div>'


def test_a_replaced_attribute_value_is_escaped() -> None:
    markup = render(div(attrs={"title": "old"}).with_attributes(attrs={"title": '"><script>'}))
    assert markup == '<div title="&#34;&gt;&lt;script&gt;"></div>'


def test_class_as_an_attribute_is_rejected_here_too() -> None:
    with pytest.raises(ValueError, match=r"^set classes with `cls`, not as an attribute$"):
        div().with_attributes(attrs={"class": "card"})


def test_an_attribute_name_that_could_break_out_is_rejected_here_too() -> None:
    with pytest.raises(ValueError, match="invalid attribute name"):
        div().with_attributes(attrs={"x onclick=steal()": "1"})


def test_children_are_untouched_by_an_attribute_change() -> None:
    assert render(div(children=p(children="text")).with_attributes(attrs={"id": "x"})) == (
        '<div id="x"><p>text</p></div>'
    )


def test_the_original_element_is_unchanged() -> None:
    original = div(cls="card")
    original.with_attributes(cls="panel", attrs={"id": "x"})
    assert render(original) == '<div class="card"></div>'


def test_a_void_element_takes_attributes_the_same_way() -> None:
    assert render(img(attrs={"src": "/a.png"}).with_attributes(attrs={"alt": "a"})) == '<img src="/a.png" alt="a">'


def test_a_void_element_stays_void() -> None:
    assert br().with_attributes(attrs={"id": "x"}) == br(attrs={"id": "x"})


def test_children_are_replaced_wholesale() -> None:
    assert render(div(children="old").with_children(span(children="new"))) == "<div><span>new</span></div>"


def test_children_are_appended_by_naming_the_old_ones() -> None:
    element = div(children=p(children="a"))
    assert render(element.with_children([*element.children, p(children="b")])) == "<div><p>a</p><p>b</p></div>"


def test_replacement_children_are_escaped() -> None:
    assert render(div(children="old").with_children("<script>")) == "<div>&lt;script&gt;</div>"


def test_replacement_children_are_flattened() -> None:
    assert render(div().with_children(p(children=str(n)) for n in (1, 2))) == "<div><p>1</p><p>2</p></div>"


def test_attributes_are_untouched_by_a_child_change() -> None:
    assert render(div(cls="card", attrs={"id": "x"}).with_children("y")) == '<div class="card" id="x">y</div>'


def test_a_raw_text_element_keeps_its_rule_about_content() -> None:
    # `dataclasses.replace` would put an escaped string inside a `<script>`, where nothing
    # is escaped and `&amp;&amp;` is what the program would run.
    with pytest.raises(ValueError, match="not parsed as markup"):
        script().with_children("a && b")


def test_a_raw_text_element_takes_markup() -> None:
    assert render(script().with_children(Markup("a && b"))) == "<script>a && b</script>"


def test_a_nonce_is_added_to_every_script_in_a_walk() -> None:
    # The transform the guide names as what having a tree is for, written out: it is one
    # expression per element, and the escaping is the constructors' and not the caller's.
    def with_nonce(node: Element) -> Element:
        nonce = 'x" onload="alert(1)'
        tagged = node.with_attributes(attrs={"nonce": nonce}) if node.tag == "script" else node
        return tagged.with_children(
            [with_nonce(child) if isinstance(child, Element) else child for child in tagged.children]
        )

    page = div(children=[p(children="text"), div(children=script(children=Markup("f()")))])
    assert render(with_nonce(page)) == (
        '<div><p>text</p><div><script nonce="x&#34; onload=&#34;alert(1)">f()</script></div></div>'
    )
