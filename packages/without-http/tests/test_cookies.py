from __future__ import annotations

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from without_http import CookieJar
from without_http.client import _Cookie
from without_http.client import _default_path
from without_http.client import _deletes
from without_http.client import _domain_matches
from without_http.client import _parse_expires
from without_http.client import _parse_set_cookie
from without_http.client import _path_matches

_BASE = datetime(2026, 7, 7, 12, 0, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("request_path", "expected"),
    [
        ("/items/page/3", "/items/page"),
        ("/single", "/"),
        ("/", "/"),
        ("relative-no-leading-slash", "/"),
    ],
)
def test_default_path_strips_to_the_last_slash(request_path: str, expected: str) -> None:
    assert _default_path(request_path) == expected


def _cookie(
    *,
    domain: str,
    path: str = "/",
    host_only: bool = False,
    secure: bool = False,
    expires: datetime | None = None,
) -> _Cookie:
    return _Cookie(
        name="sid", value="abc", domain=domain, path=path, secure=secure, host_only=host_only, expires=expires
    )


def test_domain_matches_a_subdomain_when_not_host_only() -> None:
    cookie = _cookie(domain="example.test", host_only=False)

    assert _domain_matches("api.example.test", cookie) is True


def test_domain_does_not_match_a_different_host_when_host_only() -> None:
    cookie = _cookie(domain="example.test", host_only=True)

    assert _domain_matches("api.example.test", cookie) is False


def test_path_matches_exactly() -> None:
    cookie = _cookie(domain="example.test", path="/dashboard")

    assert _path_matches("/dashboard", cookie) is True


def test_path_does_not_match_a_non_prefix() -> None:
    cookie = _cookie(domain="example.test", path="/dashboard")

    assert _path_matches("/settings", cookie) is False


def test_deletes_is_false_for_a_non_numeric_max_age() -> None:
    assert _deletes("not-a-number") is False


def test_deletes_is_false_when_max_age_absent() -> None:
    assert _deletes(None) is False


def test_deletes_is_true_for_a_non_positive_max_age() -> None:
    assert _deletes("0") is True
    assert _deletes("-5") is True


def test_parse_set_cookie_returns_none_without_a_name_value_pair() -> None:
    assert _parse_set_cookie("just-a-flag", "example.test", "/") is None


def test_parse_set_cookie_skips_empty_attribute_segments() -> None:
    parsed = _parse_set_cookie("sid=abc; ; Secure", "example.test", "/")

    assert parsed is not None
    cookie, deletes = parsed
    assert cookie.secure is True
    assert deletes is False


def test_parse_set_cookie_uses_the_default_path_when_no_path_attribute() -> None:
    parsed = _parse_set_cookie("sid=abc", "example.test", "/items/edit")

    assert parsed is not None
    cookie, _deletes_flag = parsed
    assert cookie.path == "/items"
    assert cookie.host_only is True


def test_cookie_jar_store_skips_an_unparseable_set_cookie() -> None:
    jar = CookieJar()

    jar.store("http://example.test/", ((b"set-cookie", b"no-equals-sign"),))

    assert jar.header_for("http://example.test/") is None


def test_parse_set_cookie_accepts_a_domain_the_host_is_a_subdomain_of() -> None:
    parsed = _parse_set_cookie("sid=abc; Domain=example.test", "api.example.test", "/")

    assert parsed is not None
    cookie, _deletes_flag = parsed
    assert cookie.domain == "example.test"
    assert cookie.host_only is False


def test_parse_set_cookie_accepts_a_domain_equal_to_the_host() -> None:
    parsed = _parse_set_cookie("sid=abc; Domain=example.test", "example.test", "/")

    assert parsed is not None
    cookie, _deletes_flag = parsed
    assert cookie.domain == "example.test"


@pytest.mark.security("a Set-Cookie can't scope a cookie to a domain the response host isn't within")
def test_parse_set_cookie_rejects_a_domain_the_host_is_not_within() -> None:
    assert _parse_set_cookie("sid=abc; Domain=victim.test", "evil.test", "/") is None


@pytest.mark.security("a bare-TLD Domain (a domain-wide supercookie) is rejected")
def test_parse_set_cookie_rejects_a_bare_tld_supercookie() -> None:
    assert _parse_set_cookie("sid=abc; Domain=test", "evil.test", "/") is None


@pytest.mark.security("the jar drops a cross-site Domain cookie at store time")
def test_cookie_jar_ignores_a_cross_site_domain_cookie() -> None:
    jar = CookieJar()

    jar.store("http://evil.test/", ((b"set-cookie", b"sid=abc; Domain=victim.test"),))

    assert jar.header_for("http://victim.test/") is None


def test_parse_expires_reads_a_gmt_date_as_aware_utc() -> None:
    assert _parse_expires("Wed, 09 Jun 2027 10:18:14 GMT") == datetime(2027, 6, 9, 10, 18, 14, tzinfo=UTC)


def test_parse_expires_assumes_utc_for_a_date_without_a_zone() -> None:
    parsed = _parse_expires("Wed, 09 Jun 2027 10:18:14 -0000")

    assert parsed == datetime(2027, 6, 9, 10, 18, 14, tzinfo=UTC)


def test_parse_expires_ignores_an_unparseable_date() -> None:
    assert _parse_expires("not-a-date") is None


def test_parse_set_cookie_prefers_max_age_over_expires() -> None:
    parsed = _parse_set_cookie("sid=abc; Max-Age=100; Expires=Wed, 09 Jun 2027 10:18:14 GMT", "example.test", "/")

    assert parsed is not None
    cookie, _deletes_flag = parsed
    assert cookie.expires is None


class _Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def test_cookie_jar_drops_a_cookie_whose_expires_is_already_past() -> None:
    jar = CookieJar(_now=_Clock(_BASE))

    jar.store("http://example.test/", ((b"set-cookie", b"sid=abc; Expires=Mon, 01 Jan 2001 00:00:00 GMT"),))

    assert jar.header_for("http://example.test/") is None


def test_cookie_jar_stops_sending_a_cookie_once_its_expiry_passes() -> None:
    clock = _Clock(_BASE)
    jar = CookieJar(_now=clock)
    expires = (_BASE + timedelta(hours=1)).strftime("%a, %d %b %Y %H:%M:%S GMT")

    jar.store("http://example.test/", ((b"set-cookie", f"sid=abc; Expires={expires}".encode()),))
    assert jar.header_for("http://example.test/") == b"sid=abc"

    clock.now = _BASE + timedelta(hours=2)
    assert jar.header_for("http://example.test/") is None


@pytest.mark.security("a Secure cookie offered over a cleartext response is not stored")
def test_cookie_jar_ignores_a_secure_cookie_from_a_cleartext_response() -> None:
    jar = CookieJar()

    jar.store("http://example.test/", ((b"set-cookie", b"sid=abc; Secure"),))

    assert jar.header_for("https://example.test/") is None


def test_cookie_jar_keeps_a_secure_cookie_from_a_secure_response() -> None:
    jar = CookieJar()

    jar.store("https://example.test/", ((b"set-cookie", b"sid=abc; Secure"),))

    assert jar.header_for("https://example.test/") == b"sid=abc"


def test_cookie_jar_add_injects_a_hand_written_cookie() -> None:
    jar = CookieJar()

    jar.add("session", "hand-written", domain="api.example.test")

    assert jar.header_for("http://api.example.test/path") == b"session=hand-written"


def test_cookie_jar_add_defaults_to_the_exact_host() -> None:
    jar = CookieJar()

    jar.add("session", "abc", domain="example.test")

    assert jar.header_for("http://example.test/") == b"session=abc"
    assert jar.header_for("http://api.example.test/") is None


def test_cookie_jar_add_with_subdomains_reaches_a_subdomain() -> None:
    jar = CookieJar()

    jar.add("session", "abc", domain="example.test", subdomains=True)

    assert jar.header_for("http://api.example.test/") == b"session=abc"


def test_cookie_jar_add_honors_secure_and_expiry() -> None:
    jar = CookieJar(_now=_Clock(_BASE))

    jar.add("session", "abc", domain="example.test", secure=True, expires=_BASE + timedelta(hours=1))

    assert jar.header_for("http://example.test/") is None  # secure cookie withheld over cleartext
    assert jar.header_for("https://example.test/") == b"session=abc"


def test_cookie_jar_add_skips_the_origin_guards_store_applies() -> None:
    jar = CookieJar()

    # A bare-TLD Domain that `store` would reject as a supercookie is trusted from the
    # caller's own hand.
    jar.add("session", "abc", domain="test", subdomains=True)

    assert jar.header_for("http://anything.test/") == b"session=abc"
