from __future__ import annotations

from without_asgi import headers


def test_get_all_returns_every_value_for_a_name_in_order() -> None:
    raw = ((b"x-trace", b"first"), (b"content-type", b"text/plain"), (b"x-trace", b"second"))
    assert headers.get_all(raw, b"x-trace") == (b"first", b"second")


def test_get_all_is_case_insensitive_even_when_the_source_is_not_lowercased() -> None:
    raw = ((b"X-Trace", b"alpha"), (b"X-TRACE", b"beta"))
    assert headers.get_all(raw, b"x-trace") == (b"alpha", b"beta")


def test_get_all_returns_empty_tuple_for_an_absent_name() -> None:
    assert headers.get_all(((b"content-type", b"application/json"),), b"x-missing") == ()


def test_first_returns_the_first_value_for_a_name() -> None:
    raw = ((b"accept", b"text/html"), (b"accept", b"application/json"))
    assert headers.first(raw, b"Accept") == b"text/html"


def test_first_returns_none_for_an_absent_name() -> None:
    assert headers.first(((b"content-type", b"application/json"),), b"authorization") is None


def test_add_appends_a_lowercased_name_keeping_prior_values() -> None:
    raw = ((b"set-cookie", b"a=1"),)
    assert headers.add(raw, b"Set-Cookie", b"b=2") == ((b"set-cookie", b"a=1"), (b"set-cookie", b"b=2"))


def test_remove_drops_every_value_under_a_name_and_is_idempotent() -> None:
    raw = ((b"x-trace", b"a"), (b"keep", b"1"), (b"x-trace", b"b"))
    once = headers.remove(raw, b"X-Trace")
    assert once == ((b"keep", b"1"),)
    assert headers.remove(once, b"x-trace") == ((b"keep", b"1"),)


def test_replace_drops_existing_values_then_sets_the_single_value_at_the_end() -> None:
    raw = ((b"accept", b"text/html"), (b"x-keep", b"1"), (b"accept", b"application/json"))
    assert headers.replace(raw, b"Accept", b"text/plain") == ((b"x-keep", b"1"), (b"accept", b"text/plain"))


def test_subset_keeps_only_the_named_headers_preserving_order_and_duplicates() -> None:
    raw = ((b"x-trace", b"a"), (b"content-type", b"text/plain"), (b"x-trace", b"b"))
    assert headers.subset(raw, [b"X-Trace"]) == ((b"x-trace", b"a"), (b"x-trace", b"b"))


def test_merge_lets_the_over_headers_replace_matching_names() -> None:
    base = ((b"content-type", b"text/plain"), (b"x-keep", b"stay"))
    over = ((b"content-type", b"application/json"), (b"x-new", b"added"))
    assert headers.merge(base, over) == (
        (b"x-keep", b"stay"),
        (b"content-type", b"application/json"),
        (b"x-new", b"added"),
    )


def test_the_functions_are_pure_and_leave_the_input_untouched() -> None:
    raw = ((b"content-type", b"text/plain"),)
    headers.add(raw, b"x-trace", b"t")
    headers.replace(raw, b"content-type", b"application/json")
    headers.remove(raw, b"content-type")
    assert raw == ((b"content-type", b"text/plain"),)
