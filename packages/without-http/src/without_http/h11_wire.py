from __future__ import annotations

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

# The ASGI metadata this server advertises on every scope. `extensions` is left
# `None`: an HTTP/1.1 transport offers none of the server-offload extensions
# (zero-copy send, path send, server push), so a well-behaved app does not send
# the corresponding `Outbound` events.
ASGI = Asgi(version="3.0", spec_version="2.4")


def scope_from_request(
    request: h11.Request,
    *,
    scheme: str,
    server: tuple[str, int | None] | None,
    client: tuple[str, int] | None,
) -> HttpScope:
    """
    Build the typed `HttpScope` an ASGI app expects from an `h11.Request`.

    Pure: it reads only the request event and the connection facts the transport
    already knows (peer addresses, scheme). The ASGI `path` is the percent-decoded
    target; `raw_path` keeps the bytes as received, the same split uvicorn makes.
    """
    raw_path, _, query_string = request.target.partition(b"?")
    headers = tuple((bytes(name), bytes(value)) for name, value in request.headers)
    http_version = request.http_version.decode("ascii")  # pragma: no mutate - codec name case-insensitive
    method = request.method.decode("ascii")  # pragma: no mutate - codec name case-insensitive
    path = unquote(raw_path.decode("ascii"))  # pragma: no mutate - codec name case-insensitive
    return HttpScope(
        asgi=ASGI,
        http_version=http_version,
        method=method,
        scheme=scheme,
        path=path,
        raw_path=raw_path,
        query_string=query_string,
        root_path="",
        headers=headers,
        client=client,
        server=server,
        extensions=None,
    )


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
            return None  # pragma: no mutate - implicit fall-through return is identical


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
