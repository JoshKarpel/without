from __future__ import annotations

import pytest
from without_html import div
from without_html import element_type
from without_html import render
from without_html import void_element_type


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


@pytest.mark.parametrize("tag", ["script", "style"])
def test_defining_a_raw_text_html_tag_is_rejected(tag: str) -> None:
    with pytest.raises(ValueError, match="not parsed as markup"):
        element_type(tag)


def test_a_defined_element_escapes_its_content_like_any_other() -> None:
    chart = element_type("x-chart")
    assert render(chart(children="<script>")) == "<x-chart>&lt;script&gt;</x-chart>"
