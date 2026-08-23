from __future__ import annotations

from datetime import UTC
from datetime import datetime

import pytest
from without_asgi.selection import Head
from without_asgi.selection import NotModified
from without_asgi.selection import Selection
from without_asgi.selection import Span
from without_asgi.selection import Unsatisfiable
from without_asgi.selection import Whole
from without_asgi.selection import http_date
from without_asgi.selection import parse_http_date
from without_asgi.selection import selection_for
from without_asgi.types import RawHeaders

# A representation big enough that clamping and suffix arithmetic are distinguishable,
# and a modification time that is not "now", so a test can never accidentally pass by
# comparing a value against itself.
_SIZE = 1000
_MODIFIED = datetime(2026, 3, 14, 15, 9, 26, tzinfo=UTC)
_MODIFIED_HTTP = b"Sat, 14 Mar 2026 15:09:26 GMT"
_STRONG = b'"e9d71f5ee7c92d6d"'
_WEAK = b'W/"e9d71f5ee7c92d6d"'


def _headers(*fields: tuple[bytes, bytes]) -> RawHeaders:
    return fields


def _decide(
    *fields: tuple[bytes, bytes],
    size: int = _SIZE,
    method: str = "GET",
    etag: bytes | None = _STRONG,
    last_modified: datetime | None = _MODIFIED,
) -> Selection:
    return selection_for(
        size=size,
        method=method,
        request_headers=_headers(*fields),
        etag=etag,
        last_modified=last_modified,
    )


class TestMethods:
    @pytest.mark.parametrize(
        "method",
        [
            pytest.param("POST", id="post-ignores-conditionals-and-ranges"),
            pytest.param("PUT", id="put-ignores-conditionals-and-ranges"),
            pytest.param("DELETE", id="delete-ignores-conditionals-and-ranges"),
        ],
    )
    def test_a_write_method_always_gets_the_whole_representation(self, method: str) -> None:
        decision = _decide(
            (b"range", b"bytes=0-9"),
            (b"if-none-match", _STRONG),
            method=method,
        )

        assert decision == Whole()

    def test_head_is_revalidated_like_a_get(self) -> None:
        assert _decide((b"if-none-match", _STRONG), method="HEAD") == NotModified()

    def test_head_ignores_a_range_because_only_get_defines_range_handling(self) -> None:
        # RFC 9110 §14.2: GET is the only method for which range handling is defined.
        assert _decide((b"range", b"bytes=10-19"), method="HEAD") == Head()

    def test_head_selects_a_head_so_the_representation_is_never_read(self) -> None:
        assert _decide(method="HEAD") == Head()

    def test_a_plain_get_with_no_conditions_gets_the_whole_representation(self) -> None:
        assert _decide() == Whole()


class TestConditionalRequests:
    def test_a_matching_entity_tag_is_not_modified(self) -> None:
        assert _decide((b"if-none-match", _STRONG)) == NotModified()

    def test_a_different_entity_tag_is_served(self) -> None:
        assert _decide((b"if-none-match", b'"something-else"')) == Whole()

    def test_a_star_matches_any_existing_representation(self) -> None:
        assert _decide((b"if-none-match", b"*")) == NotModified()

    @pytest.mark.parametrize(
        "offered",
        [
            pytest.param(b'*, "other"', id="a-star-before-another-tag"),
            pytest.param(b'"other", *', id="a-star-after-another-tag"),
        ],
    )
    def test_a_star_is_found_among_other_list_elements(self, offered: bytes) -> None:
        # A bare `*` is the only form most clients send, so the scanner's handling of it
        # mid-list goes untested unless it is asked for directly.
        assert _decide((b"if-none-match", offered), etag=b'"unrelated"') == NotModified()

    def test_a_star_matches_even_when_no_validator_is_published(self) -> None:
        assert _decide((b"if-none-match", b"*"), etag=None) == NotModified()

    def test_if_none_match_uses_weak_comparison_so_strength_does_not_block_a_match(self) -> None:
        # §8.8.3.2: the weak comparison ignores the W/ prefix on either side.
        assert _decide((b"if-none-match", _WEAK), etag=_STRONG) == NotModified()
        assert _decide((b"if-none-match", _STRONG), etag=_WEAK) == NotModified()

    def test_any_tag_in_the_list_may_match(self) -> None:
        offered = b'"first", W/"second", ' + _STRONG
        assert _decide((b"if-none-match", offered)) == NotModified()

    @pytest.mark.parametrize(
        "offered",
        [
            pytest.param(b'"first",' + _STRONG, id="no-space-after-the-comma"),
            pytest.param(b'"first", ' + _STRONG, id="a-space-after-the-comma"),
            pytest.param(b'"first",\t' + _STRONG, id="a-tab-after-the-comma"),
            pytest.param(b'  "first" ,  ' + _STRONG + b" ", id="whitespace-scattered-throughout"),
            pytest.param(b'"first",,' + _STRONG, id="an-empty-list-element"),
        ],
    )
    def test_the_list_separators_a_client_may_use_are_all_read(self, offered: bytes) -> None:
        assert _decide((b"if-none-match", offered)) == NotModified()

    def test_a_list_split_across_repeated_fields_is_read_whole(self) -> None:
        decision = _decide((b"if-none-match", b'"first"'), (b"if-none-match", _STRONG))

        assert decision == NotModified()

    def test_a_comma_inside_an_entity_tag_does_not_end_it(self) -> None:
        # §8.8.3: etagc admits a comma, so only the quoting says where a tag ends.
        assert _decide((b"if-none-match", b'"a,b"'), etag=b'"a,b"') == NotModified()

    def test_a_comma_inside_a_tag_does_not_manufacture_a_match(self) -> None:
        assert _decide((b"if-none-match", b'"a,b"'), etag=b'"a"') == Whole()

    def test_an_empty_entity_tag_is_a_tag(self) -> None:
        # `etagc` is zero-or-more, so `""` is grammatically a tag rather than a
        # malformed field, and the closing quote is the character right after the
        # opening one.
        assert _decide((b"if-none-match", b'""'), etag=b'""') == NotModified()

    def test_a_stray_character_before_a_tag_makes_the_field_malformed(self) -> None:
        # Only whitespace and commas separate list elements; anything else means the
        # field is not an entity-tag list and none of it is trusted.
        assert _decide((b"if-none-match", b"X" + _STRONG)) == Whole()

    @pytest.mark.parametrize(
        "offered",
        [
            pytest.param(b"unquoted", id="a-bare-token-is-not-an-entity-tag"),
            pytest.param(b'"unterminated', id="a-missing-closing-quote"),
            pytest.param(b'W/unquoted"', id="a-weak-prefix-without-an-opening-quote"),
        ],
    )
    def test_a_malformed_if_none_match_serves_the_representation(self, offered: bytes) -> None:
        assert _decide((b"if-none-match", offered)) == Whole()

    def test_an_unmodified_since_date_is_not_modified(self) -> None:
        assert _decide((b"if-modified-since", _MODIFIED_HTTP)) == NotModified()

    def test_a_date_before_the_last_modification_is_served(self) -> None:
        assert _decide((b"if-modified-since", b"Fri, 13 Mar 2026 15:09:26 GMT")) == Whole()

    def test_a_date_after_the_last_modification_is_not_modified(self) -> None:
        assert _decide((b"if-modified-since", b"Sun, 15 Mar 2026 15:09:26 GMT")) == NotModified()

    def test_an_unparseable_date_is_served(self) -> None:
        assert _decide((b"if-modified-since", b"whenever")) == Whole()

    def test_a_date_condition_needs_a_published_last_modified(self) -> None:
        assert _decide((b"if-modified-since", _MODIFIED_HTTP), last_modified=None) == Whole()

    def test_a_non_matching_entity_tag_suppresses_the_date_condition(self) -> None:
        # §13.1.2: If-None-Match wins outright when present, so a date that would have
        # answered 304 on its own must not be consulted.
        decision = _decide(
            (b"if-none-match", b'"something-else"'),
            (b"if-modified-since", _MODIFIED_HTTP),
        )

        assert decision == Whole()

    @pytest.mark.parametrize(
        "offered",
        [
            pytest.param(b"unquoted", id="a-bare-token-is-not-an-entity-tag"),
            pytest.param(b'"unterminated', id="a-missing-closing-quote"),
            pytest.param(b'W/unquoted"', id="a-weak-prefix-without-an-opening-quote"),
            pytest.param(b"", id="an-empty-field-value"),
        ],
    )
    def test_a_malformed_entity_tag_suppresses_the_date_condition_too(self, offered: bytes) -> None:
        # §13.1.2 turns on If-None-Match being *present*, not on it parsing. Gating on
        # the parse lets a malformed tag fall through to a date the client never meant
        # to be decisive: with a content-hash validator, a rebuild that changes bytes but
        # preserves mtime would then answer 304 with content the tag was there to refuse.
        decision = _decide(
            (b"if-none-match", offered),
            (b"if-modified-since", _MODIFIED_HTTP),
        )

        assert decision == Whole()


class TestRanges:
    @pytest.mark.parametrize(
        ("spec", "expected"),
        [
            pytest.param(b"bytes=0-9", Span(0, 9), id="a-closed-range-from-the-start"),
            pytest.param(b"bytes=100-199", Span(100, 199), id="a-closed-range-in-the-middle"),
            pytest.param(b"bytes=0-0", Span(0, 0), id="a-single-byte"),
            pytest.param(b"bytes=999-999", Span(999, 999), id="the-final-byte"),
            pytest.param(b"bytes=500-", Span(500, 999), id="an-open-ended-range-runs-to-the-end"),
            pytest.param(b"bytes=0-", Span(0, 999), id="an-open-range-from-zero-is-the-whole-body"),
            pytest.param(b"bytes=-100", Span(900, 999), id="a-suffix-takes-the-last-n-bytes"),
            pytest.param(b"bytes=-1", Span(999, 999), id="a-one-byte-suffix"),
            pytest.param(b"bytes=0-4000", Span(0, 999), id="a-last-position-past-the-end-clamps"),
            pytest.param(b"bytes=-4000", Span(0, 999), id="a-suffix-longer-than-the-body-is-all-of-it"),
            pytest.param(b" bytes = 0-9 ", Span(0, 9), id="surrounding-whitespace-is-tolerated"),
            pytest.param(b"BYTES=0-9", Span(0, 9), id="the-range-unit-is-case-insensitive"),
        ],
    )
    def test_a_satisfiable_range_selects_its_span(self, spec: bytes, expected: Span) -> None:
        assert _decide((b"range", spec)) == expected

    @pytest.mark.parametrize(
        "spec",
        [
            pytest.param(b"bytes=1000-", id="a-first-position-at-the-size"),
            pytest.param(b"bytes=1000-1004", id="a-closed-range-past-the-end"),
            pytest.param(b"bytes=4000-5000", id="a-range-far-past-the-end"),
            pytest.param(b"bytes=-0", id="a-zero-length-suffix-names-no-bytes"),
        ],
    )
    def test_an_unreachable_range_is_unsatisfiable(self, spec: bytes) -> None:
        assert _decide((b"range", spec)) == Unsatisfiable()

    @pytest.mark.parametrize(
        "spec",
        [
            pytest.param(b"bytes=0-9", id="a-closed-range"),
            pytest.param(b"bytes=0-", id="an-open-range"),
            pytest.param(b"bytes=-10", id="a-suffix"),
        ],
    )
    def test_no_range_is_satisfiable_against_an_empty_representation(self, spec: bytes) -> None:
        assert _decide((b"range", spec), size=0) == Unsatisfiable()

    @pytest.mark.parametrize(
        "spec",
        [
            pytest.param(b"items=0-9", id="an-unknown-range-unit-is-ignored"),
            pytest.param(b"bytes", id="no-equals-sign"),
            pytest.param(b"bytes=", id="an-empty-range-set"),
            pytest.param(b"bytes=abc-def", id="non-numeric-positions"),
            pytest.param(b"bytes=0-abc", id="a-non-numeric-last-position"),
            pytest.param(b"bytes=--5", id="a-doubled-dash"),
            pytest.param(b"bytes=9-0", id="a-last-position-below-the-first-is-an-invalid-spec"),
            pytest.param(b"bytes=0", id="no-dash-at-all"),
            pytest.param(b"bytes=0x10-0x20", id="hexadecimal-positions"),
            pytest.param(b"bytes=" + b"9" * 40, id="a-position-too-long-to-be-meant"),
        ],
    )
    def test_a_range_that_cannot_be_read_is_ignored(self, spec: bytes) -> None:
        assert _decide((b"range", spec)) == Whole()

    @pytest.mark.parametrize(
        ("spec", "expected"),
        [
            pytest.param(b"bytes=0-18446744073709551615", Span(0, 999), id="a-64-bit-position-still-clamps"),
            pytest.param(b"bytes=0-184467440737095516150", Whole(), id="a-longer-position-is-not-read-at-all"),
        ],
    )
    def test_a_position_is_read_only_up_to_the_width_a_client_could_mean(
        self, spec: bytes, expected: Selection
    ) -> None:
        # Bounding the digit count bounds the `int()` before it is attempted, rather than
        # relying on CPython's own integer-parsing cap to notice.
        assert _decide((b"range", spec)) == expected

    def test_a_multi_range_request_answers_with_the_whole_representation(self) -> None:
        # RFC 9110 §14 permits ignoring a Range, and multipart/byteranges is the shape
        # behind CVE-2011-3192 and CVE-2025-62727.
        assert _decide((b"range", b"bytes=0-9,20-29")) == Whole()

    @pytest.mark.security(
        "a Range header naming many ranges is answered without per-range work",
        cve="CVE-2011-3192, CVE-2025-62727",
    )
    def test_a_range_header_of_many_ranges_costs_one_scan(self) -> None:
        spec = b"bytes=" + b",".join(b"%d-%d" % (n, n + 1) for n in range(0, 200_000, 2))

        assert _decide((b"range", spec)) == Whole()

    def test_a_conditional_hit_wins_over_a_range(self) -> None:
        decision = _decide((b"if-none-match", _STRONG), (b"range", b"bytes=0-9"))

        assert decision == NotModified()


class TestIfRange:
    def test_a_matching_strong_tag_honors_the_range(self) -> None:
        decision = _decide((b"range", b"bytes=10-19"), (b"if-range", _STRONG))

        assert decision == Span(10, 19)

    def test_a_stale_tag_serves_the_whole_representation_instead(self) -> None:
        decision = _decide((b"range", b"bytes=10-19"), (b"if-range", b'"stale"'))

        assert decision == Whole()

    def test_a_weak_candidate_never_matches(self) -> None:
        # §13.1.5: If-Range requires the strong comparison, because the client is about
        # to splice these bytes onto ones it already holds.
        decision = _decide((b"range", b"bytes=10-19"), (b"if-range", _WEAK), etag=_WEAK)

        assert decision == Whole()

    def test_a_weakly_published_validator_never_matches_a_strong_candidate(self) -> None:
        decision = _decide((b"range", b"bytes=10-19"), (b"if-range", _STRONG), etag=_WEAK)

        assert decision == Whole()

    def test_a_matching_date_honors_the_range(self) -> None:
        decision = _decide((b"range", b"bytes=10-19"), (b"if-range", _MODIFIED_HTTP))

        assert decision == Span(10, 19)

    def test_a_date_that_is_not_the_last_modification_serves_the_whole_representation(self) -> None:
        decision = _decide((b"range", b"bytes=10-19"), (b"if-range", b"Fri, 13 Mar 2026 15:09:26 GMT"))

        assert decision == Whole()

    def test_an_unreadable_condition_serves_the_whole_representation(self) -> None:
        decision = _decide((b"range", b"bytes=10-19"), (b"if-range", b"whenever"))

        assert decision == Whole()

    def test_a_stale_condition_suppresses_a_416_as_well(self) -> None:
        # The Range is ignored entirely, so an otherwise unsatisfiable one is not
        # reported as unsatisfiable.
        decision = _decide((b"range", b"bytes=4000-5000"), (b"if-range", b'"stale"'))

        assert decision == Whole()


class TestHttpDates:
    def test_a_formatted_date_round_trips(self) -> None:
        assert parse_http_date(http_date(_MODIFIED)) == _MODIFIED

    def test_the_preferred_form_is_imf_fixdate(self) -> None:
        assert http_date(_MODIFIED) == _MODIFIED_HTTP

    @pytest.mark.parametrize(
        "raw",
        [
            pytest.param(b"Sun, 06 Nov 1994 08:49:37 GMT", id="imf-fixdate"),
            pytest.param(b"Sunday, 06-Nov-94 08:49:37 GMT", id="obsolete-rfc-850"),
            pytest.param(b"Sun Nov  6 08:49:37 1994", id="obsolete-asctime"),
        ],
    )
    def test_every_form_a_recipient_must_accept_is_parsed(self, raw: bytes) -> None:
        # RFC 9110 §5.6.7 requires recipients to accept all three.
        assert parse_http_date(raw) == datetime(1994, 11, 6, 8, 49, 37, tzinfo=UTC)

    @pytest.mark.parametrize(
        "raw",
        [
            pytest.param(b"", id="empty"),
            pytest.param(b"whenever", id="not-a-date"),
            pytest.param(b"Sun, 99 Xxx 1994 08:49:37 GMT", id="an-impossible-month"),
        ],
    )
    def test_a_value_that_is_not_a_date_is_none(self, raw: bytes) -> None:
        assert parse_http_date(raw) is None


class TestSpan:
    @pytest.mark.parametrize(
        ("span", "expected"),
        [
            pytest.param(Span(0, 0), 1, id="an-inclusive-single-byte-span-is-one-byte"),
            pytest.param(Span(0, 9), 10, id="a-ten-byte-span"),
            pytest.param(Span(900, 999), 100, id="a-span-at-the-end"),
        ],
    )
    def test_length_counts_both_ends(self, span: Span, expected: int) -> None:
        assert span.length == expected
