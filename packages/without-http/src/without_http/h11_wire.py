from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from urllib.parse import unquote

import h11
from without_asgi import Asgi
from without_asgi import Disconnect
from without_asgi import EarlyHint
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import PathSend
from without_asgi import RequestBody
from without_asgi import ResponseBody
from without_asgi import ResponseDebug
from without_asgi import ResponseStart
from without_asgi import ResponseTrailers
from without_asgi import ServerPush
from without_asgi import ZeroCopySend

# The ASGI metadata this server advertises on every scope.
ASGI = Asgi(version="3.0", spec_version="2.4")

_EARLY_HINT = "http.response.early_hint"

# What an HTTP scope from this server advertises: early hints, which both wire layers
# render as a 103, so a framework that checks the scope before sending `EarlyHint`, as
# the spec tells it to, finds the extension. The server-offload extensions (server push,
# zero-copy and path send, trailers, debug) raise `NotImplementedError` in the wire
# layers and stay unadvertised.
HTTP_EXTENSIONS: Mapping[str, Mapping[str, object]] = MappingProxyType({_EARLY_HINT: {}})

# The wire is ASCII: request/response tokens (method, path, version) decode with it.
# Naming the codec once keeps mutmut's codec-name mutations (an invalid `"XXasciiXX"`
# crashes; a case-swapped `"ASCII"` is an equivalent alias) on this single line rather
# than every call site. See docs/contributing/mutation-testing.md.
_ASCII = "ascii"


def scope_from_request(
    request: h11.Request,
    *,
    scheme: str,
    server: tuple[str, int | None] | None,
    client: tuple[str, int] | None,
    extensions: Mapping[str, Mapping[str, object]] = HTTP_EXTENSIONS,
) -> HttpScope:
    """
    Build the typed `HttpScope` an ASGI app expects from an `h11.Request`.

    Pure: it reads only the request event and the connection facts the transport
    already knows (peer addresses, scheme, and the `extensions` this connection
    offers, which is `HTTP_EXTENSIONS` plus `tls` when the connection is over TLS).
    The ASGI `path` is the percent-decoded target; `raw_path` keeps the bytes as
    received, the same split uvicorn makes.
    """
    raw_path, _, query_string = request.target.partition(b"?")
    headers = tuple((bytes(name), bytes(value)) for name, value in request.headers)
    http_version = request.http_version.decode(_ASCII)
    return HttpScope(
        asgi=ASGI,
        http_version=http_version,
        method=request.method.decode(_ASCII),
        scheme=scheme,
        path=unquote(raw_path.decode(_ASCII)),
        raw_path=raw_path,
        query_string=query_string,
        root_path="",
        headers=headers,
        client=client,
        server=server,
        extensions=_offered(extensions, http_version),
    )


def _offered(extensions: Mapping[str, Mapping[str, object]], http_version: str) -> Mapping[str, Mapping[str, object]]:
    """
    The subset of `extensions` this request can actually use.

    RFC 8297 §2 forbids sending a `103` to an HTTP/1.0 client, which has no notion of
    an interim response and would read it as the final one. h11 renders the event
    regardless, so early hints are withheld from the scope rather than left for an app
    that does the right thing (checks the scope, then sends) to mis-frame the exchange.
    """
    if http_version == "1.1" or _EARLY_HINT not in extensions:
        return extensions
    return MappingProxyType({name: value for name, value in extensions.items() if name != _EARLY_HINT})


def inbound_from_event(event: h11.Event) -> Inbound | None:
    """
    Classify one body-phase `h11` event as a typed `Inbound`, or `None` to skip.

    `h11.Data` is a body chunk (more to come); `h11.EndOfMessage` is the final,
    empty chunk that closes the request body; `h11.ConnectionClosed` is the client
    going away. Any other event is not part of the request body and is skipped.
    """
    match event:
        case h11.Data():
            return RequestBody(body=bytes(event.data), more_body=True)
        case h11.EndOfMessage():
            return RequestBody(body=b"", more_body=False)
        case h11.ConnectionClosed():
            return Disconnect()
        case _:
            return None


def h11_events_from_outbound(outbound: Outbound) -> list[h11.Event]:
    """
    Render one typed `Outbound` as the `h11` events that put it on the wire.

    HTTP/1.1 carries the response start, body, and 103 early hints. The
    server-offload and HTTP/2-only extensions (server push, zero-copy/path send,
    trailers, debug) have no HTTP/1.1 representation and the transport never
    advertised them, so reaching one is a programming error, not a wire case.
    """
    match outbound:
        case ResponseStart(status, headers, _trailers):
            return [h11.Response(status_code=status, headers=[(name, value) for name, value in headers])]
        case ResponseBody(body, more_body):
            events: list[h11.Event] = []
            if body:
                events.append(h11.Data(data=body))
            if not more_body:
                events.append(h11.EndOfMessage())
            return events
        case EarlyHint(links):
            return [h11.InformationalResponse(status_code=103, headers=[(b"link", link) for link in links])]
        case ServerPush() | ZeroCopySend() | PathSend() | ResponseTrailers() | ResponseDebug():
            raise NotImplementedError(f"{type(outbound).__name__} is not supported over HTTP/1.1")
