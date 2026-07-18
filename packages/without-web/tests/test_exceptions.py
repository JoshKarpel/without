from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from without import Stream
from without import stream_from_iterable
from without_asgi import Asgi
from without_asgi import WebsocketAccept
from without_asgi import WebsocketClose
from without_asgi import WebsocketInbound
from without_asgi import WebsocketOutbound
from without_asgi import WebsocketScope
from without_web import Match
from without_web import catching_websocket
from without_web import ws


class HandshakeDenied(Exception):
    pass


def _ws_scope() -> WebsocketScope:
    return WebsocketScope(
        asgi=Asgi(version="3.0", spec_version="2.0"),
        http_version="1.1",
        scheme="ws",
        path="/feed/3",
        raw_path=None,
        query_string=b"",
        root_path="",
        headers=(),
        client=None,
        server=None,
        subprotocols=(),
        extensions=None,
    )


class HandshakeThrottled(Exception):
    pass


async def _reject(exc: Exception) -> WebsocketClose | None:
    return WebsocketClose(code=4403, reason="denied")


async def _reject_by_type(exc: Exception) -> WebsocketClose | None:
    if isinstance(exc, HandshakeThrottled):
        return WebsocketClose(code=4429, reason="throttled")
    return WebsocketClose(code=4403, reason="denied")


async def test_catching_websocket_maps_an_exception_before_accept_to_a_close() -> None:
    deny = True

    async def handler(state: object, frames: Stream[WebsocketInbound]) -> AsyncIterator[WebsocketOutbound]:
        if deny:
            raise HandshakeDenied("denied")
        yield WebsocketAccept()  # pragma: no cover - the handler always denies before accepting

    built = ws("/feed")(handler).endpoint(object(), Match(_ws_scope(), {}))
    wrapped = catching_websocket(_reject)(built, object(), _ws_scope())
    events = [event async for event in wrapped(stream_from_iterable(()))]

    assert events == [WebsocketClose(code=4403, reason="denied")]


async def test_catching_websocket_hands_the_raised_exception_to_recover() -> None:
    async def handler(state: object, frames: Stream[WebsocketInbound]) -> AsyncIterator[WebsocketOutbound]:
        throttled = True
        if throttled:
            raise HandshakeThrottled("slow down")
        yield WebsocketAccept()  # pragma: no cover - the handler always raises before accepting

    built = ws("/feed")(handler).endpoint(object(), Match(_ws_scope(), {}))
    wrapped = catching_websocket(_reject_by_type)(built, object(), _ws_scope())
    events = [event async for event in wrapped(stream_from_iterable(()))]

    assert events == [WebsocketClose(code=4429, reason="throttled")]


async def test_catching_websocket_propagates_an_exception_raised_after_accept() -> None:
    async def handler(state: object, frames: Stream[WebsocketInbound]) -> AsyncIterator[WebsocketOutbound]:
        yield WebsocketAccept(subprotocol="chat")
        raise HandshakeDenied("too late")

    built = ws("/feed")(handler).endpoint(object(), Match(_ws_scope(), {}))
    wrapped = catching_websocket(_reject)(built, object(), _ws_scope())

    with pytest.raises(HandshakeDenied, match="too late"):
        [event async for event in wrapped(stream_from_iterable(()))]
