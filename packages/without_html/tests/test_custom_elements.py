from __future__ import annotations

import pytest
import without_html
from markupsafe import Markup
from without_html import div
from without_html import element
from without_html import element_type
from without_html import render
from without_html import void_element_type
from without_html.nodes import RAW_TEXT_TAGS


def test_a_defined_element_renders_like_a_named_one() -> None:
    chart = element_type("x-chart")
    assert render(chart(cls="wide", attrs={"data-series": "[1,2]"}, children="fallback")) == (
        '<x-chart class="wide" data-series="[1,2]">fallback</x-chart>'
    )


def test_a_defined_element_nests_inside_a_named_one() -> None:
    chart = element_type("x-chart")
    assert render(div(children=chart())) == "<div><x-chart></x-chart></div>"


def test_a_defined_element_takes_the_tag_as_its_name() -> None:
    chart = element_type("x-chart")
    assert chart.__name__ == "x-chart"
    assert chart.__doc__ == "The `<x-chart>` element."


def test_a_defined_void_element_has_no_closing_tag() -> None:
    spacer = void_element_type("x-spacer")
    assert render(spacer(cls="wide", attrs={"size": "8"})) == '<x-spacer class="wide" size="8">'


@pytest.mark.parametrize("tag", ["br", "img", "input"])
def test_defining_a_void_html_tag_as_an_element_is_rejected(tag: str) -> None:
    with pytest.raises(ValueError, match="void element"):
        element_type(tag)


def test_the_raw_text_tags_are_the_ones_a_parser_treats_that_way() -> None:
    # Pinned as a literal because every other test here is parametrized over the set
    # itself, so a tag dropped from it would take its own coverage with it and the suite
    # would stay green while the content of that element started rendering entity-encoded.
    #
    # The membership is HTML's, checked against a conforming parser rather than reasoned
    # about: feeding `a &amp; b` to html5lib comes back as those five characters for each
    # of these and as `a & b` for everything else. `noscript` is the near miss and is
    # deliberately absent: it is raw text only where scripting is on, which is exactly
    # where its content is never shown. `textarea` and `title` are RCDATA, so entities
    # are decoded in them and escaping is right.
    assert {"script", "style", "iframe", "noembed", "noframes", "xmp"} == RAW_TEXT_TAGS


@pytest.mark.parametrize("tag", sorted(RAW_TEXT_TAGS))
def test_defining_a_raw_text_html_tag_is_rejected(tag: str) -> None:
    with pytest.raises(ValueError, match="not parsed as markup"):
        element_type(tag)


@pytest.mark.parametrize("tag", sorted(RAW_TEXT_TAGS))
def test_every_raw_text_tag_has_the_constructor_its_refusal_names(tag: str) -> None:
    # The refusal above tells the caller to use `<tag>`'s own constructor, so a raw-text
    # tag with no constructor would make that message name something that does not exist,
    # leaving no way at all to build the element. None of HTML's raw-text tags is a Python
    # keyword, so the constructor is named for the tag with no suffix.
    construct = getattr(without_html, tag)
    assert render(construct(children=Markup("a && b"))) == f"<{tag}>a && b</{tag}>"


@pytest.mark.parametrize("tag", sorted(RAW_TEXT_TAGS))
def test_a_raw_text_element_refuses_content_that_would_have_been_escaped(tag: str) -> None:
    # Entities are not decoded inside these, so escaped text renders as the entity itself
    # and the content is wrong in a way that points nowhere near the code that caused it.
    construct = getattr(without_html, tag)
    with pytest.raises(ValueError, match="not parsed as markup"):
        construct(children="a && b")


def test_a_defined_element_escapes_its_content_like_any_other() -> None:
    chart = element_type("x-chart")
    assert render(chart(children="<script>")) == "<x-chart>&lt;script&gt;</x-chart>"


@pytest.mark.parametrize("tag", sorted(RAW_TEXT_TAGS))
def test_defining_a_raw_text_html_tag_as_a_void_element_is_rejected(tag: str) -> None:
    # A raw-text tag declared void renders with no closing tag, which leaves everything
    # after it in script or stylesheet context however carefully it was escaped.
    with pytest.raises(ValueError, match="not parsed as markup"):
        void_element_type(tag)


TAGS_THAT_COULD_BREAK_OUT = [
    'div onclick="steal()"',
    "with space",
    "quote'd",
    "sla/sh",
    "eq=uals",
    "close>",
    "x><script>alert(1)</script><x",
    "open<",
    "",
    # `<!--` opens a comment that runs to the next `-->`, not to the `>` that ends the tag,
    # so this one swallows the document from here on and resumes markup at an offset its
    # supplier chooses. `<?` opens a bogus comment, which drops the element without a word.
    "!--x",
    "!doctype html",
    "?x",
    "-x",
    "1x",
]


@pytest.mark.parametrize("tag", TAGS_THAT_COULD_BREAK_OUT)
def test_defining_an_element_with_a_tag_that_could_break_out_is_rejected(tag: str) -> None:
    # A tag is written into `<...>` verbatim, so one assembled from outside input is an
    # injection point that escaping the *values* does nothing about.
    with pytest.raises(ValueError, match="invalid tag name"):
        element_type(tag)


@pytest.mark.parametrize("tag", TAGS_THAT_COULD_BREAK_OUT)
def test_defining_a_void_element_with_a_tag_that_could_break_out_is_rejected(tag: str) -> None:
    with pytest.raises(ValueError, match="invalid tag name"):
        void_element_type(tag)


@pytest.mark.parametrize("tag", TAGS_THAT_COULD_BREAK_OUT)
def test_the_generic_factory_rejects_a_tag_that_could_break_out(tag: str) -> None:
    with pytest.raises(ValueError, match="invalid tag name"):
        element(tag, children="x")
