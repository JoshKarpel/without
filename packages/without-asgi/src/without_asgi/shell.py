from __future__ import annotations

from collections.abc import AsyncIterator

from without import Sink, from_sink

from without_asgi.inbound import (
    Disconnect,
    Inbound,
    LifespanEvent,
    RequestBody,
    Shutdown,
    WebsocketDisconnect,
    WebsocketInbound,
    parse_inbound,
    parse_lifespan_event,
    parse_websocket_inbound,
)
from without_asgi.outbound import (
    LifespanReply,
    Outbound,
    WebsocketOutbound,
    encode_lifespan_reply,
    encode_outbound,
    encode_websocket_outbound,
)
from without_asgi.types import Receive, Send


async def http_inbound(receive: Receive) -> AsyncIterator[Inbound]:
    """An `http` request's inbound events as a stream.

    The stream ends when the request is fully received (the last body chunk, or
    a disconnect), so a downstream processor's input runs dry exactly when the
    request does: the request's lifecycle *is* this stream's lifecycle.
    """
    while True:
        event = parse_inbound(await receive())
        yield event
        match event:
            case Disconnect():
                return
            case RequestBody(more_body=False):
                return
            case RequestBody(more_body=True):
                continue


def http_outbound(send: Send) -> Sink[Outbound]:
    """A sink that writes each outbound event to ASGI `send`, encoding at the boundary."""

    async def write(event: Outbound) -> None:
        await send(encode_outbound(event))

    return from_sink(write)


async def websocket_inbound(receive: Receive) -> AsyncIterator[WebsocketInbound]:
    """A websocket connection's inbound events as a stream.

    The stream ends on `WebsocketDisconnect`, so the connection's lifecycle *is*
    this stream's lifecycle, the same shape as `http_inbound`.
    """
    while True:
        event = parse_websocket_inbound(await receive())
        yield event
        if isinstance(event, WebsocketDisconnect):
            return


def websocket_outbound(send: Send) -> Sink[WebsocketOutbound]:
    """A sink that writes each outbound websocket event to ASGI `send`, encoding at the boundary."""

    async def write(event: WebsocketOutbound) -> None:
        await send(encode_websocket_outbound(event))

    return from_sink(write)


async def lifespan_inbound(receive: Receive) -> AsyncIterator[LifespanEvent]:
    """The `lifespan` protocol's events as a stream, ending after shutdown."""
    while True:
        event = parse_lifespan_event(await receive())
        yield event
        if isinstance(event, Shutdown):
            return


def lifespan_outbound(send: Send) -> Sink[LifespanReply]:
    """A sink that writes each lifespan reply to ASGI `send`, encoding at the boundary."""

    async def write(reply: LifespanReply) -> None:
        await send(encode_lifespan_reply(reply))

    return from_sink(write)
