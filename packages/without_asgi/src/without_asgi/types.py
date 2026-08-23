from __future__ import annotations

from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from typing import assert_never

from without_asgi.narrow import narrow

# The raw ASGI surface, hand-rolled to keep this package's only runtime
# dependency `without`. These mirror the shapes an ASGI server passes to an
# application callable; the typed values the other modules parse them into are
# built on top of these.
type RawScope = Mapping[str, object]
type RawMessage = Mapping[str, object]
# `(name, value)` byte-string header pairs, in the order received; duplicates are preserved.
type RawHeaders = tuple[tuple[bytes, bytes], ...]
type Receive = Callable[[], Awaitable[RawMessage]]
type Send = Callable[[RawMessage], Awaitable[None]]
type ASGIApp = Callable[[RawScope, Receive, Send], Awaitable[None]]


def narrow_pair(item: object) -> tuple[bytes, bytes]:
    if isinstance(item, (list, tuple)) and len(item) == 2:
        return narrow(item[0], bytes), narrow(item[1], bytes)
    raise TypeError(f"expected a (bytes, bytes) pair, got {item!r}")


def narrow_headers(value: object) -> RawHeaders:
    if not isinstance(value, Iterable):
        raise TypeError(f"expected an iterable of bytes pairs, got {type(value).__name__}")
    return tuple(narrow_pair(item) for item in value)


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


def encode_websocket_data(data: WebsocketData) -> dict[str, object]:
    """Render a `WebsocketData` value as the `text`/`bytes` key an ASGI message carries."""
    match data:
        case WebsocketText(text):
            return {"text": text}
        case WebsocketBinary(binary):
            return {"bytes": binary}
        case _ as unreachable:
            assert_never(unreachable)


def decode_websocket_data(message: RawMessage) -> WebsocketData:
    """
    Read the `text`/`bytes` key off an ASGI websocket message into a `WebsocketData`.

    The spec requires exactly one of the two; both or neither is a protocol fault.
    """
    text = message.get("text")
    binary = message.get("bytes")
    if text is not None and binary is not None:
        raise ValueError("websocket message has both text and bytes")
    if text is not None:
        return WebsocketText(text=narrow(text, str))
    if binary is not None:
        return WebsocketBinary(data=narrow(binary, bytes))
    raise ValueError("websocket message has neither text nor bytes")
