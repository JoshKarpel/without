from __future__ import annotations

import pytest
from without_http import CookieJar
from without_http.client import _Cookie
from without_http.client import _default_path
from without_http.client import _deletes
from without_http.client import _domain_matches
from without_http.client import _parse_set_cookie
from without_http.client import _path_matches


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


def _cookie(*, domain: str, path: str = "/", host_only: bool = False, secure: bool = False) -> _Cookie:
    return _Cookie(name="sid", value="abc", domain=domain, path=path, secure=secure, host_only=host_only)


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
