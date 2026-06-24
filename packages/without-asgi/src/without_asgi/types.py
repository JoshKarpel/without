from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass

# The raw ASGI surface, hand-rolled to keep this package's only runtime
# dependency `without`. These mirror the shapes an ASGI server passes to an
# application callable; the typed values the other modules parse them into are
# built on top of these.
type RawScope = Mapping[str, object]
type RawMessage = Mapping[str, object]
type Receive = Callable[[], Awaitable[RawMessage]]
type Send = Callable[[RawMessage], Awaitable[None]]
type ASGIApp = Callable[[RawScope, Receive, Send], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class WebsocketText:
    text: str


@dataclass(frozen=True, slots=True)
class WebsocketBinary:
    data: bytes


# A single websocket data frame is exactly one of text or binary; modeling it as
# a union makes the spec's "exactly one of text/bytes" invariant unrepresentable.
# It is shared by the inbound `WebsocketReceive` and the outbound `WebsocketSend`.
type WebsocketData = WebsocketText | WebsocketBinary
