from __future__ import annotations

import pytest
from without import collect
from without import stream_from_iterable
from without_asgi import ClientDisconnect
from without_asgi import Disconnect
from without_asgi import Inbound
from without_asgi import LifespanReply
from without_asgi import Outbound
from without_asgi import RawMessage
from without_asgi import Receive
from without_asgi import RequestBody
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_asgi import Send
from without_asgi import Shutdown
from without_asgi import ShutdownComplete
from without_asgi import Startup
from without_asgi import StartupComplete
from without_asgi import WebsocketBinary
from without_asgi import WebsocketConnect
from without_asgi import WebsocketDisconnect
from without_asgi import WebsocketOutbound
from without_asgi import WebsocketReceive
from without_asgi import WebsocketSend
from without_asgi import WebsocketText
from without_asgi import http_inbound
from without_asgi import http_outbound
from without_asgi import lifespan_inbound
from without_asgi import lifespan_outbound
from without_asgi import read_body
from without_asgi import websocket_inbound
from without_asgi import websocket_outbound


def _scripted(messages: list[RawMessage]) -> Receive:
    pending = iter(messages)

    async def receive() -> RawMessage:
        return next(pending)

    return receive


def _capturing() -> tuple[Send, list[RawMessage]]:
    sent: list[RawMessage] = []

    async def send(message: RawMessage) -> None:
        sent.append(message)

    return send, sent


async def test_http_inbound_ends_on_the_final_body_chunk() -> None:
    receive = _scripted(
        [
            {"type": "http.request", "body": b"ab", "more_body": True},
            {"type": "http.request", "body": b"c", "more_body": False},
        ]
    )

    events = await collect(http_inbound(receive))

    assert events == [
        RequestBody(body=b"ab", more_body=True),
        RequestBody(body=b"c", more_body=False),
    ]


async def test_http_inbound_ends_on_a_disconnect() -> None:
    receive = _scripted([{"type": "http.disconnect"}])

    events = await collect(http_inbound(receive))

    assert events == [Disconnect()]


async def test_read_body_joins_every_chunk() -> None:
    events = stream_from_iterable(
        [
            RequestBody(body=b"ab", more_body=True),
            RequestBody(body=b"c", more_body=False),
        ]
    )

    assert await read_body(events) == b"abc"


async def test_read_body_raises_on_a_mid_body_disconnect() -> None:
    inputs: list[Inbound] = [RequestBody(body=b"ab", more_body=True), Disconnect()]

    with pytest.raises(ClientDisconnect):
        await read_body(stream_from_iterable(inputs))


async def test_http_outbound_encodes_each_event_to_send() -> None:
    send, sent = _capturing()
    events: list[Outbound] = [
        ResponseStart(status=200, headers=((b"content-type", b"text/plain"),)),
        ResponseBody(body=b"ok", more_body=False),
    ]

    await http_outbound(send)(stream_from_iterable(events))

    assert sent == [
        {"type": "http.response.start", "status": 200, "headers": [[b"content-type", b"text/plain"]]},
        {"type": "http.response.body", "body": b"ok", "more_body": False},
    ]


async def test_websocket_inbound_ends_on_a_disconnect() -> None:
    receive = _scripted(
        [
            {"type": "websocket.connect"},
            {"type": "websocket.receive", "text": "ping"},
            {"type": "websocket.disconnect", "code": 1001},
        ]
    )

    events = await collect(websocket_inbound(receive))

    assert events == [
        WebsocketConnect(),
        WebsocketReceive(data=WebsocketText(text="ping")),
        WebsocketDisconnect(code=1001, reason=""),
    ]


async def test_websocket_outbound_encodes_each_event_to_send() -> None:
    send, sent = _capturing()
    events: list[WebsocketOutbound] = [
        WebsocketSend(data=WebsocketBinary(data=b"\x01")),
    ]

    await websocket_outbound(send)(stream_from_iterable(events))

    assert sent == [{"type": "websocket.send", "bytes": b"\x01"}]


async def test_lifespan_inbound_ends_after_shutdown() -> None:
    receive = _scripted([{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}])

    events = await collect(lifespan_inbound(receive))

    assert events == [Startup(), Shutdown()]


async def test_lifespan_outbound_encodes_replies_to_send() -> None:
    send, sent = _capturing()
    replies: list[LifespanReply] = [StartupComplete(), ShutdownComplete()]

    await lifespan_outbound(send)(stream_from_iterable(replies))

    assert sent == [{"type": "lifespan.startup.complete"}, {"type": "lifespan.shutdown.complete"}]
