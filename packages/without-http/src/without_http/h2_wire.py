from __future__ import annotations

from collections.abc import Iterable
from urllib.parse import unquote

from without_asgi import HttpScope
from without_asgi import RawHeaders

from without_http.h11_wire import ASGI

# The HTTP/2 connection preface a cleartext client sends before any frames. A
# server detects "prior knowledge" h2c by sniffing it off the first bytes, since
# `h11` would otherwise mis-parse `PRI` as an HTTP/1 method. Matching the leading
# token is enough to tell it apart from every HTTP/1 request line.
H2_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

# Connection-specific (hop-by-hop) headers are forbidden in HTTP/2: hpack rejects
# them, so they are stripped from a response rendered for the h2 wire. HTTP/1.1
# carries them, which is why the h11 path keeps them.
_HOP_BY_HOP = frozenset({b"connection", b"keep-alive", b"proxy-connection", b"transfer-encoding", b"upgrade", b"te"})


def scope_from_h2_headers(
    headers: Iterable[tuple[bytes, bytes]],
    *,
    scheme: str,
    server: tuple[str, int | None] | None,
    client: tuple[str, int] | None,
) -> HttpScope:
    """
    Build the typed `HttpScope` an ASGI app expects from an h2 request's headers.

    Pure: it reads only the request pseudo-headers (`:method`/`:path`/`:authority`)
    and the connection facts the transport already knows (peer addresses, scheme).
    The `scheme` is taken from the transport, not the client-asserted `:scheme`. The
    `:authority` is folded into a synthesized `host` header when the request carries
    none, the same mapping uvicorn makes for HTTP/2.
    """
    method = b""
    target = b""
    authority = b""
    ordinary: list[tuple[bytes, bytes]] = []
    has_host = False
    for name, value in headers:
        if name == b":method":
            method = value
        elif name == b":path":
            target = value
        elif name == b":authority":
            authority = value
        elif name.startswith(b":"):
            continue
        else:
            if name == b"host":
                has_host = True
            ordinary.append((bytes(name), bytes(value)))
    if authority and not has_host:
        ordinary.insert(0, (b"host", bytes(authority)))
    raw_path, _, query_string = target.partition(b"?")
    return HttpScope(
        asgi=ASGI,
        http_version="2",
        method=method.decode("ascii"),
        scheme=scheme,
        path=unquote(raw_path.decode("ascii")),
        raw_path=raw_path,
        query_string=query_string,
        root_path="",
        headers=tuple(ordinary),
        client=client,
        server=server,
        extensions=None,
    )


def response_headers(status: int, headers: RawHeaders) -> list[tuple[bytes, bytes]]:
    """
    Render a response start as the h2 header block: `:status` first, then the rest.

    Header names are lowercased (HTTP/2 requires it) and the hop-by-hop headers that
    are illegal over h2 are dropped, so a response written for HTTP/1.1 round-trips
    over HTTP/2 without tripping hpack.
    """
    block = [(b":status", str(status).encode("ascii"))]
    block.extend((name.lower(), value) for name, value in headers if name.lower() not in _HOP_BY_HOP)
    return block


def early_hint_headers(links: Iterable[bytes]) -> list[tuple[bytes, bytes]]:
    """Render a 103 Early Hints informational response as an h2 header block."""
    return [(b":status", b"103"), *((b"link", link) for link in links)]


def request_headers(
    method: bytes,
    target: bytes,
    scheme: str,
    authority: bytes,
    headers: RawHeaders,
) -> list[tuple[bytes, bytes]]:
    """
    Render a client request as the h2 header block: the pseudo-headers, then the rest.

    The dual of `scope_from_h2_headers`: the request line and `Host` become the
    `:method`/`:path`/`:scheme`/`:authority` pseudo-headers (h2 carries the host as
    `:authority`, never an ordinary `host` header). Names are lowercased and the
    hop-by-hop headers illegal over h2 are dropped, so a request written for
    HTTP/1.1 round-trips over HTTP/2 without tripping hpack.
    """
    block = [
        (b":method", method),
        (b":path", target),
        (b":scheme", scheme.encode("ascii")),
        (b":authority", authority),
    ]
    block.extend(
        (name.lower(), value) for name, value in headers if name.lower() not in _HOP_BY_HOP and name.lower() != b"host"
    )
    return block


def response_status_and_headers(headers: Iterable[tuple[bytes, bytes]]) -> tuple[int, RawHeaders]:
    """
    Read an h2 response header block back into a status and ordinary headers.

    The dual of `response_headers`: the `:status` pseudo-header becomes the numeric
    status and every other pseudo-header is dropped, leaving the ordinary response
    headers the client surfaces.
    """
    status = 0
    ordinary: list[tuple[bytes, bytes]] = []
    for name, value in headers:
        if name == b":status":
            status = int(value)
        elif name.startswith(b":"):
            continue
        else:
            ordinary.append((bytes(name), bytes(value)))
    return status, tuple(ordinary)
