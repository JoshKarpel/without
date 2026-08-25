from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import field
from types import AsyncGeneratorType

import pytest
from without import Processor
from without import Stream
from without_asgi import Asgi
from without_asgi import ASGIApp
from without_asgi import HttpRouter
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Lifespan
from without_asgi import Outbound
from without_asgi import RawMessage
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_asgi import WebsocketInbound
from without_asgi import WebsocketOutbound
from without_asgi import WebsocketReceive
from without_asgi import WebsocketScope
from without_asgi import WebsocketSend
from without_asgi import make_asgi_app


@dataclass(slots=True)
class Trace:
    log: list[str] = field(default_factory=list)


def _lifespan(
    trace: Trace,
    value: str,
    *,
    fail_enter: bool = False,
    fail_exit: bool = False,
) -> Lifespan[str]:
    @asynccontextmanager
    async def lifespan() -> AsyncIterator[str]:
        trace.log.append("enter")
        if fail_enter:
            raise RuntimeError("setup boom")
        try:
            yield value
        finally:
            trace.log.append("exit")
            if fail_exit:
                raise RuntimeError("teardown boom")

    return lifespan


def _recording_router(seen: list[str]) -> HttpRouter[str]:
    def router(state: str, scope: HttpScope) -> Processor[Inbound, Outbound]:
        seen.append(state)

        async def silent(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
            nothing: tuple[Outbound, ...] = ()  # reads no events, emits none: the test only checks threaded state
            for event in nothing:
                yield event  # pragma: no cover

        return silent

    return router


def _start_lifespan(
    app: ASGIApp,
) -> tuple[asyncio.Queue[RawMessage], asyncio.Queue[RawMessage], asyncio.Task[None]]:
    inbox: asyncio.Queue[RawMessage] = asyncio.Queue()
    outbox: asyncio.Queue[RawMessage] = asyncio.Queue()

    async def receive() -> RawMessage:
        return await inbox.get()

    async def send(message: RawMessage) -> None:
        await outbox.put(message)

    async def drive() -> None:
        await app({"type": "lifespan", "asgi": {"version": "3.0"}}, receive, send)

    task = asyncio.create_task(drive())
    return inbox, outbox, task


async def test_drives_the_handshake_and_enters_then_exits_the_lifespan() -> None:
    trace = Trace()

    def router(state: str, scope: HttpScope) -> Processor[Inbound, Outbound]:
        raise AssertionError("no request in this test")  # pragma: no cover

    wrapped = make_asgi_app(_lifespan(trace, "ready"), router)
    inbox, outbox, task = _start_lifespan(wrapped)

    await inbox.put({"type": "lifespan.startup"})
    assert await outbox.get() == {"type": "lifespan.startup.complete"}
    assert trace.log == ["enter"]

    await inbox.put({"type": "lifespan.shutdown"})
    assert await outbox.get() == {"type": "lifespan.shutdown.complete"}
    assert trace.log == ["enter", "exit"]

    await task


async def test_requests_are_handed_the_lifespan_state() -> None:
    seen: list[str] = []
    router = _recording_router(seen)
    wrapped = make_asgi_app(_lifespan(Trace(), "the-state"), router)
    inbox, outbox, task = _start_lifespan(wrapped)

    await inbox.put({"type": "lifespan.startup"})
    await outbox.get()

    async def receive() -> RawMessage:
        raise AssertionError("this handler reads no events")  # pragma: no cover

    async def send(message: RawMessage) -> None:
        raise AssertionError("this handler sends nothing")  # pragma: no cover

    await wrapped(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "path": "/",
            "query_string": b"",
            "headers": [],
        },
        receive,
        send,
    )
    assert seen == ["the-state"]

    await inbox.put({"type": "lifespan.shutdown"})
    await outbox.get()
    await task


async def test_the_http_router_is_handed_the_parsed_scope() -> None:
    captured: list[HttpScope] = []

    def router(state: str, scope: HttpScope) -> Processor[Inbound, Outbound]:
        captured.append(scope)

        async def silent(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
            nothing: tuple[Outbound, ...] = ()
            for event in nothing:
                yield event  # pragma: no cover

        return silent

    wrapped = make_asgi_app(_lifespan(Trace(), "state"), router)
    inbox, outbox, task = _start_lifespan(wrapped)
    await inbox.put({"type": "lifespan.startup"})
    await outbox.get()

    async def receive() -> RawMessage:
        raise AssertionError("this handler reads no events")  # pragma: no cover

    async def send(message: RawMessage) -> None:
        raise AssertionError("this handler sends nothing")  # pragma: no cover

    await wrapped(
        {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": "PATCH",
            "scheme": "https",
            "path": "/widgets/7",
            "raw_path": b"/widgets/7",
            "query_string": b"colour=teal",
            "root_path": "/api",
            "headers": [[b"x-trace", b"abc"]],
            "client": ["203.0.113.7", 54321],
            "server": ["example.test", 443],
        },
        receive,
        send,
    )

    assert captured == [
        HttpScope(
            asgi=Asgi(version="3.0", spec_version="2.3"),
            http_version="1.1",
            method="PATCH",
            scheme="https",
            path="/widgets/7",
            raw_path=b"/widgets/7",
            query_string=b"colour=teal",
            root_path="/api",
            headers=((b"x-trace", b"abc"),),
            client=("203.0.113.7", 54321),
            server=("example.test", 443),
            extensions=None,
        )
    ]

    await inbox.put({"type": "lifespan.shutdown"})
    await outbox.get()
    await task


async def test_the_websocket_router_is_handed_the_state_and_parsed_scope() -> None:
    captured_state: list[str] = []
    captured_scope: list[WebsocketScope] = []

    def router(state: str, scope: WebsocketScope) -> Processor[WebsocketInbound, WebsocketOutbound]:
        captured_state.append(state)
        captured_scope.append(scope)

        async def silent(inputs: Stream[WebsocketInbound]) -> AsyncIterator[WebsocketOutbound]:
            nothing: tuple[WebsocketOutbound, ...] = ()
            for event in nothing:
                yield event  # pragma: no cover

        return silent

    wrapped = make_asgi_app(_lifespan(Trace(), "socket-state"), websocket=router)
    inbox, outbox, task = _start_lifespan(wrapped)
    await inbox.put({"type": "lifespan.startup"})
    await outbox.get()

    async def receive() -> RawMessage:
        raise AssertionError("this handler reads no events")  # pragma: no cover

    async def send(message: RawMessage) -> None:
        raise AssertionError("this handler sends nothing")  # pragma: no cover

    await wrapped(
        {
            "type": "websocket",
            "asgi": {"version": "3.0", "spec_version": "2.4"},
            "http_version": "1.1",
            "scheme": "wss",
            "path": "/chat/lobby",
            "raw_path": b"/chat/lobby",
            "query_string": b"room=42",
            "root_path": "/ws",
            "headers": [[b"origin", b"https://example.test"]],
            "client": ["198.51.100.9", 12345],
            "server": ["example.test", 8443],
            "subprotocols": ["chat.v1"],
        },
        receive,
        send,
    )

    assert captured_state == ["socket-state"]
    assert captured_scope == [
        WebsocketScope(
            asgi=Asgi(version="3.0", spec_version="2.4"),
            http_version="1.1",
            scheme="wss",
            path="/chat/lobby",
            raw_path=b"/chat/lobby",
            query_string=b"room=42",
            root_path="/ws",
            headers=((b"origin", b"https://example.test"),),
            client=("198.51.100.9", 12345),
            server=("example.test", 8443),
            subprotocols=("chat.v1",),
            extensions=None,
        )
    ]

    await inbox.put({"type": "lifespan.shutdown"})
    await outbox.get()
    await task


async def test_the_websocket_handler_echoes_inbound_frames_through_the_wired_streams() -> None:
    def router(state: str, scope: WebsocketScope) -> Processor[WebsocketInbound, WebsocketOutbound]:
        async def echo(inputs: Stream[WebsocketInbound]) -> AsyncIterator[WebsocketOutbound]:
            async for event in inputs:
                if isinstance(event, WebsocketReceive):
                    yield WebsocketSend(data=event.data)

        return echo

    wrapped = make_asgi_app(_lifespan(Trace(), "state"), websocket=router)
    inbox, outbox, task = _start_lifespan(wrapped)
    await inbox.put({"type": "lifespan.startup"})
    await outbox.get()

    incoming: Iterator[RawMessage] = iter(
        [
            {"type": "websocket.connect"},
            {"type": "websocket.receive", "text": "ping-value"},
            {"type": "websocket.disconnect", "code": 1000, "reason": ""},
        ]
    )

    async def receive() -> RawMessage:
        return next(incoming)

    sent: list[RawMessage] = []

    async def send(message: RawMessage) -> None:
        sent.append(message)

    await wrapped({"type": "websocket", "asgi": {"version": "3.0"}, "path": "/ws", "headers": []}, receive, send)

    assert sent == [{"type": "websocket.send", "text": "ping-value"}]

    await inbox.put({"type": "lifespan.shutdown"})
    await outbox.get()
    await task


async def test_outbound_stream_is_closed_when_the_client_goes_away_mid_response() -> None:
    # The guarantee a long-lived response depends on: an event source's `finally` runs at
    # the `send` that fails, not at whenever the collector reaches the abandoned generator.
    released = asyncio.Event()

    def router(state: str, scope: HttpScope) -> Processor[Inbound, Outbound]:
        async def stream_forever(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
            try:
                await anext(aiter(inputs))
                yield ResponseStart(status=200)
                while True:
                    yield ResponseBody(body=b"tick", more_body=True)
            finally:
                released.set()

        return stream_forever

    wrapped = make_asgi_app(_lifespan(Trace(), "state"), router)
    inbox, outbox, task = _start_lifespan(wrapped)
    await inbox.put({"type": "lifespan.startup"})
    await outbox.get()

    async def receive() -> RawMessage:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: RawMessage) -> None:
        if message["type"] == "http.response.body":
            raise ConnectionResetError("the client hung up")

    with pytest.raises(ConnectionResetError):
        await wrapped(
            {
                "type": "http",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "method": "GET",
                "path": "/",
                "query_string": b"",
                "headers": [],
            },
            receive,
            send,
        )

    assert released.is_set()

    await inbox.put({"type": "lifespan.shutdown"})
    await outbox.get()
    await task


async def test_inbound_stream_is_closed_when_a_handler_abandons_the_body() -> None:
    captured: list[Stream[Inbound]] = []

    def router(state: str, scope: HttpScope) -> Processor[Inbound, Outbound]:
        async def abandon_after_first_chunk(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
            captured.append(inputs)
            await anext(aiter(inputs))  # read one chunk, then leave the rest of the body unread
            yield ResponseStart(status=204)
            yield ResponseBody(body=b"", more_body=False)

        return abandon_after_first_chunk

    wrapped = make_asgi_app(_lifespan(Trace(), "state"), router)
    inbox, outbox, task = _start_lifespan(wrapped)
    await inbox.put({"type": "lifespan.startup"})
    await outbox.get()

    async def receive() -> RawMessage:
        return {"type": "http.request", "body": b"first", "more_body": True}

    async def send(message: RawMessage) -> None:
        pass

    await wrapped(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "path": "/",
            "query_string": b"",
            "headers": [],
        },
        receive,
        send,
    )

    (inbound,) = captured
    assert isinstance(inbound, AsyncGeneratorType)
    assert inbound.ag_frame is None  # the abandoned generator was closed, not left for GC

    await inbox.put({"type": "lifespan.shutdown"})
    await outbox.get()
    await task


async def test_a_request_before_startup_fails_loud() -> None:
    seen: list[str] = []
    router = _recording_router(seen)
    wrapped = make_asgi_app(_lifespan(Trace(), "unused"), router)

    async def receive() -> RawMessage:
        raise AssertionError("unreached")  # pragma: no cover

    async def send(message: RawMessage) -> None:
        raise AssertionError("unreached")  # pragma: no cover

    with pytest.raises(RuntimeError, match="startup has not completed"):
        await wrapped(
            {
                "type": "http",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "method": "GET",
                "path": "/",
                "query_string": b"",
                "headers": [],
            },
            receive,
            send,
        )
    assert seen == []


async def test_an_unserved_http_scope_is_refused_with_501() -> None:
    wrapped = make_asgi_app(_lifespan(Trace(), "state"))  # no http router passed
    inbox, outbox, task = _start_lifespan(wrapped)
    await inbox.put({"type": "lifespan.startup"})
    await outbox.get()

    sent: list[RawMessage] = []

    async def receive() -> RawMessage:
        raise AssertionError("the refusal sends without reading the request")  # pragma: no cover

    async def send(message: RawMessage) -> None:
        sent.append(message)

    await wrapped(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "path": "/",
            "query_string": b"",
            "headers": [],
        },
        receive,
        send,
    )

    assert sent == [
        {
            "type": "http.response.start",
            "status": 501,
            "headers": ((b"content-type", b"text/plain; charset=utf-8"),),
        },
        {"type": "http.response.body", "body": b"this application does not serve http\n", "more_body": False},
    ]

    await inbox.put({"type": "lifespan.shutdown"})
    await outbox.get()
    await task


async def test_an_unserved_websocket_scope_is_refused_with_a_close() -> None:
    wrapped = make_asgi_app(_lifespan(Trace(), "state"))  # no websocket router passed
    inbox, outbox, task = _start_lifespan(wrapped)
    await inbox.put({"type": "lifespan.startup"})
    await outbox.get()

    sent: list[RawMessage] = []

    async def receive() -> RawMessage:
        raise AssertionError("the refusal closes without reading events")  # pragma: no cover

    async def send(message: RawMessage) -> None:
        sent.append(message)

    await wrapped({"type": "websocket", "asgi": {"version": "3.0"}, "path": "/ws", "headers": []}, receive, send)

    assert sent == [{"type": "websocket.close", "code": 1000, "reason": ""}]

    await inbox.put({"type": "lifespan.shutdown"})
    await outbox.get()
    await task


async def test_setup_failure_is_reported_as_startup_failed() -> None:
    trace = Trace()

    def router(state: str, scope: HttpScope) -> Processor[Inbound, Outbound]:
        raise AssertionError("startup failed, so no request should run")  # pragma: no cover

    wrapped = make_asgi_app(_lifespan(trace, "never", fail_enter=True), router)
    inbox, outbox, task = _start_lifespan(wrapped)

    await inbox.put({"type": "lifespan.startup"})
    assert await outbox.get() == {"type": "lifespan.startup.failed", "message": "setup boom"}
    assert trace.log == ["enter"]

    await task


async def test_teardown_failure_is_reported_as_shutdown_failed() -> None:
    trace = Trace()

    def router(state: str, scope: HttpScope) -> Processor[Inbound, Outbound]:
        raise AssertionError("no request in this test")  # pragma: no cover

    wrapped = make_asgi_app(_lifespan(trace, "ready", fail_exit=True), router)
    inbox, outbox, task = _start_lifespan(wrapped)

    await inbox.put({"type": "lifespan.startup"})
    assert await outbox.get() == {"type": "lifespan.startup.complete"}

    await inbox.put({"type": "lifespan.shutdown"})
    assert await outbox.get() == {"type": "lifespan.shutdown.failed", "message": "teardown boom"}
    assert trace.log == ["enter", "exit"]

    await task
