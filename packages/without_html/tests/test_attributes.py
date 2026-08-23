from __future__ import annotations

import pytest
from without_html import AttributeValue
from without_html import div
from without_html import input_
from without_html import render
from without_html.nodes import CHECKED_ATTRIBUTE_NAMES
from without_html.nodes import CHECKED_NAME_CAPACITY


def test_attributes_render_in_the_order_they_were_given() -> None:
    assert render(div(attrs={"id": "runs", "data-page": "3"})) == '<div id="runs" data-page="3"></div>'


def test_a_class_string_renders_as_the_class_attribute() -> None:
    assert render(div(cls="card")) == '<div class="card"></div>'


def test_several_classes_are_joined_with_spaces() -> None:
    assert render(div(cls=["card", "card-wide"])) == '<div class="card card-wide"></div>'


def test_a_class_generator_is_accepted() -> None:
    assert render(div(cls=(name for name in ("a", "b")))) == '<div class="a b"></div>'


def test_an_empty_class_list_renders_no_attribute() -> None:
    assert render(div(cls=[])) == "<div></div>"


def test_a_none_class_entry_is_dropped() -> None:
    # What makes a conditional class writable inline, the same way `None` in a child
    # position is: no filtering, no branch, no stray separator where the entry was.
    active = False
    assert render(div(cls=("card", "card-active" if active else None))) == '<div class="card"></div>'


def test_an_empty_class_entry_is_dropped_rather_than_doubling_the_separator() -> None:
    assert render(div(cls=("card", "", "wide"))) == '<div class="card wide"></div>'


def test_a_class_entry_may_itself_hold_several_names() -> None:
    # So a caller never has to know which form a part arrived in.
    assert render(div(cls=("card", "p-2 m-2"))) == '<div class="card p-2 m-2"></div>'


def test_all_class_entries_being_dropped_renders_no_attribute() -> None:
    assert render(div(cls=(None, None))) == "<div></div>"


def test_the_class_attribute_comes_first() -> None:
    assert render(div(cls="card", attrs={"id": "x"})) == '<div class="card" id="x"></div>'


def test_class_as_an_attribute_is_rejected() -> None:
    # Classes have one channel, not two kept in sync, so this fails the first time it is
    # written rather than when someone later adds `cls` beside it.
    with pytest.raises(ValueError, match=r"^set classes with `cls`, not as an attribute$"):
        div(attrs={"class": "card"})


def test_class_as_an_attribute_is_rejected_alongside_cls() -> None:
    with pytest.raises(ValueError, match=r"^set classes with `cls`, not as an attribute$"):
        div(cls="card", attrs={"class": "other"})


@pytest.mark.parametrize("name", ["Class", "CLASS", "cLaSs"])
def test_class_as_an_attribute_is_rejected_however_it_is_spelled(name: str) -> None:
    # Attribute names are case-insensitive to a parser, so a spelling that got past the
    # rejection would not get past the browser: it would see two `class` attributes on one
    # element, keep the first, and drop the other without a word.
    with pytest.raises(ValueError, match=r"^set classes with `cls`, not as an attribute$"):
        div(cls="card", attrs={name: "other"})


def test_a_proven_attribute_name_is_admitted_to_the_cache() -> None:
    # The name check is a cache lookup, so a name proven once never reaches the check
    # again. Without the admission every occurrence would take the slow path instead.
    # Asserted absent first, because a name already admitted satisfies the check below
    # without the call under test doing anything.
    assert "data-admitted" not in CHECKED_ATTRIBUTE_NAMES
    div(attrs={"data-admitted": "1"})
    assert "data-admitted" in CHECKED_ATTRIBUTE_NAMES


def test_the_name_memo_stops_admitting_at_its_capacity() -> None:
    # A name built from outside input would otherwise grow the memo for the life of the
    # process. Past the cap a name is checked on every use rather than once, so it still
    # renders; only the memo stops growing.
    for n in range(CHECKED_NAME_CAPACITY * 2):
        div(attrs={f"data-{n}": "1"})
    assert len(CHECKED_ATTRIBUTE_NAMES) == CHECKED_NAME_CAPACITY
    assert render(div(attrs={"data-beyond-the-cap": "1"})) == '<div data-beyond-the-cap="1"></div>'


def test_a_true_value_renders_a_bare_attribute() -> None:
    assert render(input_(attrs={"disabled": True})) == "<input disabled>"


@pytest.mark.parametrize("value", [False, None])
def test_a_false_or_missing_value_drops_the_attribute(value: AttributeValue) -> None:
    assert render(input_(attrs={"readonly": value, "name": "q"})) == '<input name="q">'


def test_an_integer_value_renders_as_digits() -> None:
    assert render(div(attrs={"tabindex": 2})) == '<div tabindex="2"></div>'


def test_attribute_names_pass_through_verbatim() -> None:
    # No underscore mangling: the names arrive in a mapping, so hyphenated and
    # camelCase names (`hx-get`, `viewBox`) need no escape hatch.
    markup = render(div(attrs={"hx-get": "/runs", "hx-trigger": "every 5s", "viewBox": "0 0 1 1"}))
    assert markup == '<div hx-get="/runs" hx-trigger="every 5s" viewBox="0 0 1 1"></div>'


@pytest.mark.parametrize(
    "name",
    [
        'onclick="steal()" x',
        "with space",
        "quote'd",
        "sla/sh",
        "eq=uals",
        "close>",
        "",
    ],
)
def test_an_attribute_name_that_could_break_out_is_rejected(name: str) -> None:
    with pytest.raises(ValueError, match="invalid attribute name"):
        div(attrs={name: "x"})
