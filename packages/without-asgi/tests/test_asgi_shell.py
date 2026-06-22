from __future__ import annotations

from without.testing import collect, stream
from without_asgi.core import (
    Disconnect,
    LifespanReply,
    Message,
    Outbound,
    Receive,
    RequestBody,
    ResponseBody,
    ResponseStart,
    Send,
    Shutdown,
    ShutdownComplete,
    Startup,
    StartupComplete,
)
from without_asgi.shell import http_inbound, http_outbound, lifespan_inbound, lifespan_outbound


def _scripted(messages: list[Message]) -> Receive:
    pending = iter(messages)

    async def receive() -> Message:
        return next(pending)

    return receive


def _capturing() -> tuple[Send, list[Message]]:
    sent: list[Message] = []

    async def send(message: Message) -> None:
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


async def test_lifespan_inbound_ends_after_shutdown() -> None:
    receive = _scripted([{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}])

    events = await collect(lifespan_inbound(receive))

    assert events == [Startup(), Shutdown()]


async def test_lifespan_outbound_encodes_replies_to_send() -> None:
    send, sent = _capturing()
    replies: list[LifespanReply] = [StartupComplete(), ShutdownComplete()]

    await lifespan_outbound(send)(stream(replies))

    assert sent == [{"type": "lifespan.startup.complete"}, {"type": "lifespan.shutdown.complete"}]
