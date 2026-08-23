from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser

from hypothesis import given
from hypothesis import strategies as st
from markupsafe import Markup
from markupsafe import escape
from without_html import div
from without_html import p
from without_html import render
from without_html.markup import escape_attribute

# Text that a conforming HTML parser round-trips unchanged: surrogates cannot be
# encoded, and control characters (including NUL, which parsers replace with U+FFFD,
# and the carriage returns that newline normalization rewrites) are not preserved by
# the parse half of the round trip.
#
# Drawn from two strategies rather than one, because unrestricted text almost never
# contains the handful of characters that matter here: a round-trip property fed only
# by `st.text()` passes against a renderer that does no escaping at all.
MARKUP_CHARACTERS = st.text(st.sampled_from("<>&\"'/= \na1"))
PARSEABLE_TEXT = MARKUP_CHARACTERS | st.text(st.characters(exclude_categories=("Cs", "Cc")))


@dataclass(slots=True)
class Parsed(HTMLParser):
    """Everything a real parser recovers from a rendered fragment."""

    tags: list[tuple[str, dict[str, str | None]]]
    text: list[str]

    @classmethod
    def of(cls, markup: str) -> Parsed:
        parsed = cls(tags=[], text=[])
        HTMLParser.__init__(parsed, convert_charrefs=True)
        parsed.feed(markup)
        parsed.close()
        return parsed

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.tags.append((tag, dict(attrs)))

    def handle_data(self, data: str) -> None:
        self.text.append(data)


def test_markup_characters_in_text_are_escaped() -> None:
    assert render(p(children="<script>alert(1)</script>")) == "<p>&lt;script&gt;alert(1)&lt;/script&gt;</p>"


def test_ampersands_in_text_are_escaped_once() -> None:
    assert render(p(children="a & b")) == "<p>a &amp; b</p>"


def test_an_already_escaped_entity_is_escaped_again() -> None:
    # Escaping is not idempotent on purpose: text is text, so a literal `&amp;` typed by
    # a visitor renders as those five characters rather than silently becoming `&`.
    assert render(p(children="&amp;")) == "<p>&amp;amp;</p>"


def test_quotes_in_an_attribute_cannot_close_it() -> None:
    markup = render(div(attrs={"title": '" onmouseover="steal()'}))
    assert 'onmouseover="steal()"' not in markup
    assert Parsed.of(markup).tags == [("div", {"title": '" onmouseover="steal()'})]


def test_markup_renders_verbatim() -> None:
    assert render(p(children=Markup("<em>trusted</em>"))) == "<p><em>trusted</em></p>"


def test_markup_concatenated_with_text_escapes_the_text() -> None:
    # MarkupSafe's own operator behaviour, relied on rather than reimplemented: the
    # result of mixing trusted and untrusted text is safe.
    assert render(p(children=Markup("<em>a</em>") + "<b>")) == "<p><em>a</em>&lt;b&gt;</p>"


def test_an_object_that_knows_its_own_markup_renders_verbatim() -> None:
    @dataclass(frozen=True, slots=True)
    class Badge:
        label: str

        def __html__(self) -> str:
            return f"<span class='badge'>{self.label}</span>"

    assert render(div(children=Badge("ok"))) == "<div><span class='badge'>ok</span></div>"


@given(text=PARSEABLE_TEXT)
def test_attribute_escaping_matches_markupsafe_character_for_character(text: str) -> None:
    # The attribute path uses a guarded local escape rather than MarkupSafe's, purely
    # because MarkupSafe allocates a `Markup` whether or not anything changed. Pinning it
    # against the original is what keeps that an optimization rather than a divergence.
    assert escape_attribute(text) == str(escape(text))


@given(text=PARSEABLE_TEXT)
def test_any_text_survives_a_round_trip_through_a_real_parser(text: str) -> None:
    parsed = Parsed.of(render(p(children=text)))
    assert parsed.tags == [("p", {})]
    assert "".join(parsed.text) == text


@given(value=PARSEABLE_TEXT)
def test_any_attribute_value_survives_a_round_trip_through_a_real_parser(value: str) -> None:
    parsed = Parsed.of(render(div(attrs={"data-value": value})))
    assert parsed.tags == [("div", {"data-value": value})]
