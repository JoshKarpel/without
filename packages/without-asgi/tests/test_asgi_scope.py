from __future__ import annotations

import pytest
from without_asgi import Asgi
from without_asgi import HttpScope
from without_asgi import LifespanScope
from without_asgi import RawMessage
from without_asgi import Scope
from without_asgi import Tls
from without_asgi import WebsocketScope
from without_asgi import encode_scope
from without_asgi import extension
from without_asgi import parse_http_scope
from without_asgi import parse_scope
from without_asgi import parse_tls
from without_asgi import parse_websocket_scope


def test_parse_http_scope_reads_the_connection_facts() -> None:
    scope: RawMessage = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "https",
        "path": "/flags",
        "raw_path": b"/flags",
        "query_string": b"name=dark_mode",
        "root_path": "/api",
        "headers": [[b"accept", b"application/json"]],
        "client": ["198.51.100.7", 54321],
        "server": ["example.test", 443],
        "extensions": {"http.response.zerocopysend": {}},
    }

    assert parse_http_scope(scope) == HttpScope(
        asgi=Asgi(version="3.0", spec_version="2.3"),
        http_version="1.1",
        method="POST",
        scheme="https",
        path="/flags",
        raw_path=b"/flags",
        query_string=b"name=dark_mode",
        root_path="/api",
        headers=((b"accept", b"application/json"),),
        client=("198.51.100.7", 54321),
        server=("example.test", 443),
        extensions={"http.response.zerocopysend": {}},
    )


def test_parse_http_scope_defaults_optional_fields() -> None:
    scope: RawMessage = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "path": "/flags",
        "query_string": b"",
        "headers": [],
    }

    assert parse_http_scope(scope) == HttpScope(
        asgi=Asgi(version="3.0", spec_version="2.0"),
        http_version="1.1",
        method="GET",
        scheme="http",
        path="/flags",
        raw_path=None,
        query_string=b"",
        root_path="",
        headers=(),
        client=None,
        server=None,
        extensions=None,
    )


def test_parse_websocket_scope_reads_the_handshake_facts() -> None:
    scope: RawMessage = {
        "type": "websocket",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "2",
        "scheme": "wss",
        "path": "/live",
        "raw_path": b"/live",
        "query_string": b"room=lobby",
        "root_path": "/api",
        "headers": [[b"origin", b"https://example.test"]],
        "client": ["198.51.100.7", 54321],
        "server": ["/run/app.sock", None],
        "subprotocols": ["graphql-ws", "json"],
        "extensions": {"websocket.http.response": {}},
    }

    assert parse_websocket_scope(scope) == WebsocketScope(
        asgi=Asgi(version="3.0", spec_version="2.3"),
        http_version="2",
        scheme="wss",
        path="/live",
        raw_path=b"/live",
        query_string=b"room=lobby",
        root_path="/api",
        headers=((b"origin", b"https://example.test"),),
        client=("198.51.100.7", 54321),
        server=("/run/app.sock", None),
        subprotocols=("graphql-ws", "json"),
        extensions={"websocket.http.response": {}},
    )


def test_parse_websocket_scope_defaults_optional_fields() -> None:
    scope: RawMessage = {"type": "websocket", "asgi": {"version": "3.0"}, "path": "/live", "headers": []}

    assert parse_websocket_scope(scope) == WebsocketScope(
        asgi=Asgi(version="3.0", spec_version="2.0"),
        http_version="1.1",
        scheme="ws",
        path="/live",
        raw_path=None,
        query_string=b"",
        root_path="",
        headers=(),
        client=None,
        server=None,
        subprotocols=(),
        extensions=None,
    )


@pytest.mark.parametrize(
    ("scope", "expected_type"),
    [
        (
            {
                "type": "http",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "method": "DELETE",
                "path": "/flags",
                "query_string": b"",
                "headers": [],
            },
            HttpScope,
        ),
        ({"type": "websocket", "asgi": {"version": "3.0"}, "path": "/live", "headers": []}, WebsocketScope),
        ({"type": "lifespan", "asgi": {"version": "3.0"}}, LifespanScope),
    ],
)
def test_parse_scope_dispatches_by_type(scope: RawMessage, expected_type: type) -> None:
    assert isinstance(parse_scope(scope), expected_type)


def test_parse_scope_rejects_an_unknown_type() -> None:
    with pytest.raises(ValueError, match="unexpected scope type"):
        parse_scope({"type": "tcp"})


def test_parse_tls_reads_the_connection_info() -> None:
    extensions: dict[str, dict[str, object]] = {
        "tls": {
            "server_cert": "-----BEGIN CERTIFICATE-----server",
            "client_cert_chain": ["-----BEGIN CERTIFICATE-----client", "-----BEGIN CERTIFICATE-----ca"],
            "client_cert_name": "CN=alice",
            "client_cert_error": None,
            "tls_version": 0x0304,
            "cipher_suite": 0x1301,
        }
    }

    assert parse_tls(extensions) == Tls(
        server_cert="-----BEGIN CERTIFICATE-----server",
        client_cert_chain=("-----BEGIN CERTIFICATE-----client", "-----BEGIN CERTIFICATE-----ca"),
        client_cert_name="CN=alice",
        client_cert_error=None,
        tls_version=0x0304,
        cipher_suite=0x1301,
    )


def test_parse_tls_defaults_optional_and_nullable_fields() -> None:
    extensions: dict[str, dict[str, object]] = {"tls": {"server_cert": None, "tls_version": None, "cipher_suite": None}}

    assert parse_tls(extensions) == Tls(
        server_cert=None,
        client_cert_chain=(),
        client_cert_name=None,
        client_cert_error=None,
        tls_version=None,
        cipher_suite=None,
    )


@pytest.mark.parametrize("extensions", [None, {"http.response.push": {}}])
def test_parse_tls_is_none_without_the_extension(extensions: object) -> None:
    assert parse_tls(extensions) is None  # type: ignore[arg-type]


def test_extension_returns_the_advertised_options() -> None:
    extensions: dict[str, dict[str, object]] = {"http.response.trailers": {"max_trailers": 16}}

    assert extension(extensions, "http.response.trailers") == {"max_trailers": 16}


def test_extension_is_none_for_an_unadvertised_extension() -> None:
    extensions: dict[str, dict[str, object]] = {"http.response.push": {}}

    assert extension(extensions, "http.response.trailers") is None


def test_extension_is_none_when_the_server_advertised_no_extensions() -> None:
    assert extension(None, "http.response.trailers") is None


def test_encode_http_scope_renders_the_raw_dict() -> None:
    scope = HttpScope(
        asgi=Asgi(version="3.0", spec_version="2.3"),
        http_version="1.1",
        method="POST",
        scheme="https",
        path="/flags",
        raw_path=b"/flags",
        query_string=b"name=dark_mode",
        root_path="/api",
        headers=((b"accept", b"application/json"),),
        client=("198.51.100.7", 54321),
        server=("example.test", 443),
        extensions={"http.response.zerocopysend": {}},
    )

    assert encode_scope(scope) == {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "https",
        "path": "/flags",
        "raw_path": b"/flags",
        "query_string": b"name=dark_mode",
        "root_path": "/api",
        "headers": [[b"accept", b"application/json"]],
        "client": ["198.51.100.7", 54321],
        "server": ["example.test", 443],
        "extensions": {"http.response.zerocopysend": {}},
    }


_HTTP_SCOPE = HttpScope(
    asgi=Asgi(version="3.0", spec_version="2.3"),
    http_version="1.1",
    method="PATCH",
    scheme="https",
    path="/flags",
    raw_path=b"/flags",
    query_string=b"name=dark_mode",
    root_path="/api",
    headers=((b"accept", b"application/json"),),
    client=("198.51.100.7", 54321),
    server=("/run/app.sock", None),
    extensions={"http.response.trailers": {}},
)
_WEBSOCKET_SCOPE = WebsocketScope(
    asgi=Asgi(version="3.0", spec_version="2.3"),
    http_version="2",
    scheme="wss",
    path="/live",
    raw_path=b"/live",
    query_string=b"room=lobby",
    root_path="/api",
    headers=((b"origin", b"https://example.test"),),
    client=("198.51.100.7", 54321),
    server=("example.test", 443),
    subprotocols=("graphql-ws", "json"),
    extensions={"websocket.http.response": {}},
)
_LIFESPAN_SCOPE = LifespanScope(asgi=Asgi(version="3.0", spec_version="2.0"))


@pytest.mark.parametrize("scope", [_HTTP_SCOPE, _WEBSOCKET_SCOPE, _LIFESPAN_SCOPE])
def test_encode_scope_round_trips_through_parse_scope(scope: Scope) -> None:
    assert parse_scope(encode_scope(scope)) == scope
