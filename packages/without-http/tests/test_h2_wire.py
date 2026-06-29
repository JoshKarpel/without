from __future__ import annotations

from without_http import early_hint_headers
from without_http import request_headers
from without_http import response_headers
from without_http import response_status_and_headers
from without_http import scope_from_h2_headers


def test_scope_from_h2_headers_reads_the_pseudo_headers() -> None:
    headers = [
        (b":method", b"POST"),
        (b":scheme", b"http"),
        (b":path", b"/items?page=2"),
        (b":authority", b"example.test"),
        (b"content-type", b"application/json"),
    ]

    scope = scope_from_h2_headers(headers, scheme="https", server=("example.test", 443), client=("198.51.100.7", 54321))

    assert scope.method == "POST"
    assert scope.path == "/items"
    assert scope.raw_path == b"/items"
    assert scope.query_string == b"page=2"
    assert scope.http_version == "2"
    assert scope.scheme == "https"
    assert scope.server == ("example.test", 443)
    assert scope.client == ("198.51.100.7", 54321)


def test_scope_from_h2_headers_synthesizes_a_host_from_the_authority() -> None:
    headers = [(b":method", b"GET"), (b":path", b"/"), (b":authority", b"api.example.test")]

    scope = scope_from_h2_headers(headers, scheme="https", server=None, client=None)

    assert scope.headers == ((b"host", b"api.example.test"),)


def test_scope_from_h2_headers_keeps_an_explicit_host_over_the_authority() -> None:
    headers = [(b":method", b"GET"), (b":path", b"/"), (b":authority", b"authority.test"), (b"host", b"explicit.test")]

    scope = scope_from_h2_headers(headers, scheme="http", server=None, client=None)

    assert scope.headers == ((b"host", b"explicit.test"),)


def test_scope_from_h2_headers_percent_decodes_the_path() -> None:
    headers = [(b":method", b"GET"), (b":path", b"/caf%C3%A9"), (b":authority", b"t")]

    scope = scope_from_h2_headers(headers, scheme="http", server=None, client=None)

    assert scope.path == "/café"
    assert scope.raw_path == b"/caf%C3%A9"


def test_response_headers_puts_status_first_and_lowercases_names() -> None:
    block = response_headers(201, ((b"Content-Type", b"text/plain"), (b"X-Custom", b"value")))

    assert block == [(b":status", b"201"), (b"content-type", b"text/plain"), (b"x-custom", b"value")]


def test_response_headers_drops_hop_by_hop_headers_illegal_over_h2() -> None:
    block = response_headers(200, ((b"connection", b"close"), (b"content-length", b"5")))

    assert block == [(b":status", b"200"), (b"content-length", b"5")]


def test_early_hint_headers_renders_a_103_with_links() -> None:
    block = early_hint_headers((b"</style.css>; rel=preload", b"</app.js>; rel=preload"))

    assert block == [
        (b":status", b"103"),
        (b"link", b"</style.css>; rel=preload"),
        (b"link", b"</app.js>; rel=preload"),
    ]


def test_request_headers_put_the_pseudo_headers_first() -> None:
    block = request_headers(
        b"POST", b"/items?page=2", "https", b"api.example.test", ((b"Accept", b"application/json"),)
    )

    assert block == [
        (b":method", b"POST"),
        (b":path", b"/items?page=2"),
        (b":scheme", b"https"),
        (b":authority", b"api.example.test"),
        (b"accept", b"application/json"),
    ]


def test_request_headers_fold_host_into_the_authority() -> None:
    block = request_headers(b"GET", b"/", "http", b"example.test", ((b"host", b"example.test"),))

    assert block == [(b":method", b"GET"), (b":path", b"/"), (b":scheme", b"http"), (b":authority", b"example.test")]


def test_request_headers_drop_hop_by_hop_headers_illegal_over_h2() -> None:
    block = request_headers(b"GET", b"/", "https", b"t", ((b"connection", b"keep-alive"), (b"x-keep", b"yes")))

    assert block == [
        (b":method", b"GET"),
        (b":path", b"/"),
        (b":scheme", b"https"),
        (b":authority", b"t"),
        (b"x-keep", b"yes"),
    ]


def test_response_status_and_headers_split_the_status_from_the_rest() -> None:
    status, headers = response_status_and_headers([(b":status", b"404"), (b"content-type", b"text/plain")])

    assert status == 404
    assert headers == ((b"content-type", b"text/plain"),)


def test_response_status_and_headers_drops_other_pseudo_headers() -> None:
    status, headers = response_status_and_headers(
        [(b":status", b"204"), (b":unexpected", b"ignored"), (b"x-trace", b"abc")]
    )

    assert status == 204
    assert headers == ((b"x-trace", b"abc"),)
