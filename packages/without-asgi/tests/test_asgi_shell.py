from __future__ import annotations

from without.testing import collect, stream
from without_asgi import (
    Disconnect,
    LifespanReply,
    Outbound,
    RawMessage,
    Receive,
    RequestBody,
    ResponseBody,
    ResponseStart,
    Send,
    Shutdown,
    ShutdownComplete,
    Startup,
    StartupComplete,
    WebsocketBinary,
    WebsocketConnect,
    WebsocketDisconnect,
    WebsocketOutbound,
    WebsocketReceive,
    WebsocketSend,
    WebsocketText,
    http_inbound,
    http_outbound,
    lifespan_inbound,
    lifespan_outbound,
    websocket_inbound,
    websocket_outbound,
)


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


async def test_http_outbound_encodes_each_event_to_send() -> None:
    send, sent = _capturing()
    events: list[Outbound] = [
        ResponseStart(status=200, headers=((b"content-type", b"text/plain"),)),
        ResponseBody(body=b"ok", more_body=False),
    ]

    await http_outbound(send)(stream(events))

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

    await websocket_outbound(send)(stream(events))

    assert sent == [{"type": "websocket.send", "bytes": b"\x01"}]


async def test_lifespan_inbound_ends_after_shutdown() -> None:
    receive = _scripted([{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}])

    events = await collect(lifespan_inbound(receive))

    assert events == [Startup(), Shutdown()]


async def test_lifespan_outbound_encodes_replies_to_send() -> None:
    send, sent = _capturing()
    replies: list[LifespanReply] = [StartupComplete(), ShutdownComplete()]

    await lifespan_outbound(send)(stream(replies))

    assert sent == [{"type": "lifespan.startup.complete"}, {"type": "lifespan.shutdown.complete"}]
