from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from typing import assert_never

from without_asgi.narrow import narrow
from without_asgi.types import RawHeaders
from without_asgi.types import RawScope
from without_asgi.types import narrow_headers


@dataclass(frozen=True, slots=True)
class Asgi:
    """The ASGI version metadata carried on every scope's `asgi` key."""

    version: str
    """Version of the ASGI spec."""

    spec_version: str
    """Version of the ASGI sub-spec this server implements."""


@dataclass(frozen=True, slots=True)
class HttpScope:
    """
    The per-request connection facts, known once when the request opens.

    Field descriptions are taken from the ASGI HTTP connection scope:
    https://asgi.readthedocs.io/en/latest/specs/www.html#http-connection-scope

    The ASGI `state` namespace is intentionally not surfaced: `without` threads
    lifespan-derived state to handlers explicitly through `make_asgi_app`, rather
    than reading it from the scope.
    """

    asgi: Asgi
    """Version metadata for the ASGI spec and HTTP sub-spec."""

    http_version: str
    """One of `"1.0"`, `"1.1"` or `"2"`."""

    method: str
    """The HTTP method name, uppercased."""

    scheme: str
    """URL scheme portion (likely `"http"` or `"https"`); defaults to `"http"`."""

    path: str
    """HTTP request target excluding any query string, with percent-encoded
    sequences and UTF-8 byte sequences decoded into characters."""

    raw_path: bytes | None
    """The original HTTP path component as the bytes the web server received,
    excluding any query string; `None` if the server cannot provide it."""

    query_string: bytes
    """URL portion after the `?`, percent-encoded."""

    root_path: str
    """The root path this application is mounted at (WSGI `SCRIPT_NAME`);
    defaults to `""`."""

    headers: RawHeaders
    """`[name, value]` byte-string header pairs, in the order received;
    duplicates are preserved."""

    client: tuple[str, int] | None
    """Remote `[host, port]`; `None` if not provided."""

    server: tuple[str, int | None] | None
    """Server `[host, port]`, or `[path, None]` for a unix socket; `None` if not
    provided."""

    extensions: Mapping[str, Mapping[str, object]] | None
    """Server-advertised optional extensions, keyed by name; `None` if the server
    advertised none. See `parse_tls` for reading the `tls` extension."""


@dataclass(frozen=True, slots=True)
class WebsocketScope:
    """
    The handshake facts of a websocket connection, known when it opens.

    Field descriptions are taken from the ASGI WebSocket connection scope:
    https://asgi.readthedocs.io/en/latest/specs/www.html#websocket-connection-scope

    The ASGI `state` namespace is intentionally not surfaced: `without` threads
    lifespan-derived state to handlers explicitly through `make_asgi_app`, rather
    than reading it from the scope.
    """

    asgi: Asgi
    """Version metadata for the ASGI spec and WebSocket sub-spec."""

    http_version: str
    """One of `"1.1"` or `"2"`; defaults to `"1.1"`."""

    scheme: str
    """URL scheme portion (likely `"ws"` or `"wss"`); defaults to `"ws"`."""

    path: str
    """HTTP request target excluding any query string, with percent-encoded
    sequences and UTF-8 byte sequences decoded into characters."""

    raw_path: bytes | None
    """The original HTTP path component as the bytes the web server received,
    excluding any query string; `None` if the server cannot provide it."""

    query_string: bytes
    """URL portion after the `?`; defaults to empty."""

    root_path: str
    """The root path this application is mounted at (WSGI `SCRIPT_NAME`);
    defaults to `""`."""

    headers: RawHeaders
    """`[name, value]` byte-string header pairs, in the order received;
    duplicates are preserved."""

    client: tuple[str, int] | None
    """Remote `[host, port]`; `None` if not provided."""

    server: tuple[str, int | None] | None
    """Server `[host, port]`, or `[path, None]` for a unix socket; `None` if not
    provided."""

    subprotocols: tuple[str, ...]
    """Subprotocols the client advertised; defaults to empty."""

    extensions: Mapping[str, Mapping[str, object]] | None
    """Server-advertised optional extensions, keyed by name; `None` if the server
    advertised none. See `parse_tls` for reading the `tls` extension."""


@dataclass(frozen=True, slots=True)
class LifespanScope:
    """
    The server lifecycle scope, shared across the whole event loop.

    Field descriptions are taken from the ASGI lifespan scope:
    https://asgi.readthedocs.io/en/latest/specs/lifespan.html#scope

    The ASGI `state` namespace is intentionally not surfaced: `without` threads
    lifespan-derived state to handlers explicitly through `make_asgi_app` (which
    holds it in a `_Cell` and passes it per request), rather than via the scope.
    """

    asgi: Asgi
    """Version metadata for the ASGI spec and lifespan sub-spec."""


@dataclass(frozen=True, slots=True)
class Tls:
    """
    The `tls` extension's connection info, present only on TLS connections.

    Field descriptions are taken from the ASGI TLS extension:
    https://asgi.readthedocs.io/en/latest/specs/tls.html
    """

    server_cert: str | None
    """PEM-encoded server certificate; `None` if the server cannot provide it."""

    client_cert_chain: tuple[str, ...]
    """PEM-encoded client certificate chain (client cert first); empty if none."""

    client_cert_name: str | None
    """RFC4514 Distinguished Name of the client certificate subject; `None` if
    no client certificate."""

    client_cert_error: str | None
    """Verification error message if a client certificate failed validation;
    `None` if it verified or none was provided."""

    tls_version: int | None
    """TLS version number (e.g. `0x0304` for TLS 1.3); `None` if not in use."""

    cipher_suite: int | None
    """16-bit cipher suite identifier in network byte order; `None` if not
    provided."""


# A connection scope is anything a request handler can be handed: the lifespan
# scope is the driver's own concern and never reaches an app.
type ConnectionScope = HttpScope | WebsocketScope
type Scope = ConnectionScope | LifespanScope


def _as_subprotocols(value: object) -> tuple[str, ...]:
    if not isinstance(value, Iterable):
        raise TypeError(f"expected an iterable of subprotocols, got {type(value).__name__}")
    return tuple(narrow(item, str) for item in value)


def _as_asgi(value: object, *, default_spec_version: str) -> Asgi:
    if not isinstance(value, Mapping):
        raise TypeError(f"expected an asgi mapping, got {type(value).__name__}")
    return Asgi(
        version=narrow(value["version"], str),
        spec_version=narrow(value.get("spec_version", default_spec_version), str),
    )


def _as_optional_bytes(value: object) -> bytes | None:
    return None if value is None else narrow(value, bytes)


def _as_client(value: object) -> tuple[str, int] | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)) and len(value) == 2:
        value1 = value[0]
        value2 = value[1]
        return narrow(value1, str), narrow(value2, int)
    raise TypeError(f"expected a [host, port] pair, got {value!r}")


def _as_server(value: object) -> tuple[str, int | None] | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)) and len(value) == 2:
        host, port = value
        return narrow(host, str), (None if port is None else narrow(port, int))
    raise TypeError(f"expected a [host, port] pair, got {value!r}")


def _as_optional_str(value: object) -> str | None:
    return None if value is None else narrow(value, str)


def _as_optional_int(value: object) -> int | None:
    return None if value is None else narrow(value, int)


def _as_cert_chain(value: object) -> tuple[str, ...]:
    if not isinstance(value, Iterable):
        raise TypeError(f"expected an iterable of certificates, got {type(value).__name__}")
    return tuple(narrow(cert, str) for cert in value)


def _as_extensions(value: object) -> Mapping[str, Mapping[str, object]] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError(f"expected an extensions mapping, got {type(value).__name__}")
    parsed: dict[str, Mapping[str, object]] = {}
    for name, options in value.items():
        if not isinstance(options, Mapping):
            raise TypeError(f"expected an options mapping for extension {name!r}, got {type(options).__name__}")
        parsed[narrow(name, str)] = options
    return parsed


def parse_http_scope(scope: RawScope) -> HttpScope:
    """Read an `http` scope into the typed connection facts, validating at the boundary."""
    return HttpScope(
        asgi=_as_asgi(scope["asgi"], default_spec_version="2.0"),
        http_version=narrow(scope["http_version"], str),
        method=narrow(scope["method"], str),
        scheme=narrow(scope.get("scheme", "http"), str),
        path=narrow(scope["path"], str),
        raw_path=_as_optional_bytes(scope.get("raw_path")),
        query_string=narrow(scope["query_string"], bytes),
        root_path=narrow(scope.get("root_path", ""), str),
        headers=narrow_headers(scope["headers"]),
        client=_as_client(scope.get("client")),
        server=_as_server(scope.get("server")),
        extensions=_as_extensions(scope.get("extensions")),
    )


def parse_websocket_scope(scope: RawScope) -> WebsocketScope:
    """Read a `websocket` scope into the typed handshake facts, validating at the boundary."""
    return WebsocketScope(
        asgi=_as_asgi(scope["asgi"], default_spec_version="2.0"),
        http_version=narrow(scope.get("http_version", "1.1"), str),
        scheme=narrow(scope.get("scheme", "ws"), str),
        path=narrow(scope["path"], str),
        raw_path=_as_optional_bytes(scope.get("raw_path")),
        query_string=narrow(scope.get("query_string", b""), bytes),
        root_path=narrow(scope.get("root_path", ""), str),
        headers=narrow_headers(scope["headers"]),
        client=_as_client(scope.get("client")),
        server=_as_server(scope.get("server")),
        subprotocols=_as_subprotocols(scope.get("subprotocols", ())),
        extensions=_as_extensions(scope.get("extensions")),
    )


def parse_scope(scope: RawScope) -> Scope:
    """Classify any scope by its `type` discriminator. An unknown type is a protocol fault, so it raises."""
    match narrow(scope["type"], str):
        case "http":
            return parse_http_scope(scope)
        case "websocket":
            return parse_websocket_scope(scope)
        case "lifespan":
            return LifespanScope(asgi=_as_asgi(scope["asgi"], default_spec_version="1.0"))
        case other:
            raise ValueError(f"unexpected scope type: {other!r}")


def extension(extensions: Mapping[str, Mapping[str, object]] | None, name: str) -> Mapping[str, object] | None:
    """
    The named extension's advertised options, or `None` when it is absent.

    Parse, don't validate: this returns the options mapping itself (often empty,
    as for `http.response.trailers`) rather than a bool, so a caller needing the
    options has them and one needing only presence checks `is not None`.
    Server-advertised extensions are optional per-connection capabilities; an app
    negotiates by looking one up before using it and falling back when it is
    `None` (and `parse_tls` reads the `tls` extension through this).
    """
    if extensions is None:
        return None
    return extensions.get(name)


def _encode_asgi(asgi: Asgi) -> dict[str, object]:
    return {"version": asgi.version, "spec_version": asgi.spec_version}


def _encode_pair(pair: tuple[str, int | None] | None) -> list[object] | None:
    return None if pair is None else [pair[0], pair[1]]


def _encode_headers(headers: RawHeaders) -> list[list[bytes]]:
    return [[name, value] for name, value in headers]


def encode_http_scope(scope: HttpScope) -> RawScope:
    """
    Render a typed `HttpScope` as the raw `http` scope dict an ASGI app expects.

    The server-direction dual of `parse_http_scope`: a transport that owns the
    wire (without-http) builds the typed scope from the request line and renders
    it back to the dict the ASGI contract hands an app.
    """
    return {
        "type": "http",
        "asgi": _encode_asgi(scope.asgi),
        "http_version": scope.http_version,
        "method": scope.method,
        "scheme": scope.scheme,
        "path": scope.path,
        "raw_path": scope.raw_path,
        "query_string": scope.query_string,
        "root_path": scope.root_path,
        "headers": _encode_headers(scope.headers),
        "client": _encode_pair(scope.client),
        "server": _encode_pair(scope.server),
        "extensions": scope.extensions,
    }


def encode_websocket_scope(scope: WebsocketScope) -> RawScope:
    """Render a typed `WebsocketScope` as the raw `websocket` scope dict an ASGI app expects."""
    return {
        "type": "websocket",
        "asgi": _encode_asgi(scope.asgi),
        "http_version": scope.http_version,
        "scheme": scope.scheme,
        "path": scope.path,
        "raw_path": scope.raw_path,
        "query_string": scope.query_string,
        "root_path": scope.root_path,
        "headers": _encode_headers(scope.headers),
        "client": _encode_pair(scope.client),
        "server": _encode_pair(scope.server),
        "subprotocols": list(scope.subprotocols),
        "extensions": scope.extensions,
    }


def encode_scope(scope: Scope) -> RawScope:
    """Render any typed scope as its raw ASGI dict, the dual of `parse_scope`."""
    match scope:
        case HttpScope():
            return encode_http_scope(scope)
        case WebsocketScope():
            return encode_websocket_scope(scope)
        case LifespanScope(asgi):
            return {"type": "lifespan", "asgi": _encode_asgi(asgi)}
        case _ as unreachable:
            assert_never(unreachable)


def parse_tls(extensions: Mapping[str, Mapping[str, object]] | None) -> Tls | None:
    """
    Read the `tls` extension's connection info from a scope's `extensions`.

    Returns `None` when the connection is not over TLS (the extension is absent),
    which is how an application distinguishes TLS from plaintext connections.
    """
    if (tls := extension(extensions, "tls")) is None:
        return None
    return Tls(
        server_cert=_as_optional_str(tls["server_cert"]),
        client_cert_chain=_as_cert_chain(tls.get("client_cert_chain", ())),
        client_cert_name=_as_optional_str(tls.get("client_cert_name")),
        client_cert_error=_as_optional_str(tls.get("client_cert_error")),
        tls_version=_as_optional_int(tls["tls_version"]),
        cipher_suite=_as_optional_int(tls["cipher_suite"]),
    )
