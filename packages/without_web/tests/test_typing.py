from __future__ import annotations

from typing import TYPE_CHECKING
from typing import assert_type

# The extractor context lattice turns "wrong extractor on the wrong route" from a
# runtime guard into a *static* error, so its guarantees live in the type checker,
# not the runtime suite. This module is that suite's static half: mypy is the
# assertion engine. `assert_type` pins the positive invariants (a permissive token's
# context, `into` solving to the meet of its parts); each `# type: ignore[...]` pins
# a negative one, since `warn_unused_ignores` fails the build if the marked line ever
# stops erroring, exactly the regression the deleted runtime guards used to catch.
#
# Everything is under `if TYPE_CHECKING`: mypy checks the branch, the runtime never
# executes it (so there are no placeholder handlers to build or run). The assert_type
# targets are bare type expressions, not strings, so ruff sees the imported names as
# used and keeps them.

if TYPE_CHECKING:
    from without_asgi import HttpScope
    from without_asgi import Inbound
    from without_asgi import Response
    from without_asgi import WebsocketScope
    from without_streams import Stream
    from without_web import INT
    from without_web import BufferedRequest
    from without_web import Extractor
    from without_web import HttpRequestHead
    from without_web import RequestHead
    from without_web import body
    from without_web import handle
    from without_web import handle_stream
    from without_web import http_scope
    from without_web import into
    from without_web import path_param
    from without_web import websocket_scope
    from without_web import ws

    def _parse_bytes(raw: bytes) -> bytes:
        return raw

    _SCHEMA = {"type": "string"}

    # --- positive: a permissive token reads only `RequestHead`, so it serves any route.
    assert_type(path_param("id", INT), Extractor[RequestHead, int])

    # --- positive: the scope tokens carry the narrowed context their route provides.
    assert_type(http_scope(), Extractor[HttpRequestHead, HttpScope])

    # --- positive: `into` solves its shared context to the meet of its constituents',
    # so combining a permissive `path_param` with a `body` yields a buffered-only token.
    def _pair(identifier: int, payload: bytes) -> tuple[int, bytes]:
        return identifier, payload

    assert_type(
        into(_pair, path_param("id", INT), body(_parse_bytes, schema=_SCHEMA)),
        Extractor[BufferedRequest, tuple[int, bytes]],
    )

    # --- negative: a `body` token (`BufferedRequest`) on a streaming route, whose
    # context is `HttpRequestHead`, does not type-check: buffering is what it avoids.
    async def _stream_fn(state: object, value: bytes, inputs: Stream[Inbound]) -> Response: ...

    handle_stream(body(_parse_bytes, schema=_SCHEMA), fn=_stream_fn)  # type: ignore[arg-type]

    # --- negative: a `body` token on a websocket route (`WebsocketRequestHead`): a
    # handshake carries none.
    ws(t"/x", body(_parse_bytes, schema=_SCHEMA))  # type: ignore[arg-type]

    # --- negative: `http_scope` on a websocket route: its scope is a `WebsocketScope`.
    ws(t"/x", http_scope())  # type: ignore[arg-type]

    # --- negative: `websocket_scope` on a buffered HTTP route.
    async def _scope_fn(state: object, scope: WebsocketScope) -> Response: ...

    handle(websocket_scope(), fn=_scope_fn)  # type: ignore[arg-type]
