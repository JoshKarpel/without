from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import field

import pytest
from without import Processor
from without import Stream
from without_asgi import ASGIApp
from without_asgi import HttpRouter
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Lifespan
from without_asgi import Outbound
from without_asgi import RawMessage
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
                yield event

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
        raise AssertionError("no request in this test")

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
        raise AssertionError("this handler reads no events")

    async def send(message: RawMessage) -> None:
        raise AssertionError("this handler sends nothing")

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


async def test_a_request_before_startup_fails_loud() -> None:
    seen: list[str] = []
    router = _recording_router(seen)
    wrapped = make_asgi_app(_lifespan(Trace(), "unused"), router)

    async def receive() -> RawMessage:
        raise AssertionError("unreached")

    async def send(message: RawMessage) -> None:
        raise AssertionError("unreached")

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
        raise AssertionError("the refusal sends without reading the request")

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
            "headers": [[b"content-type", b"text/plain; charset=utf-8"]],
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
        raise AssertionError("the refusal closes without reading events")

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
        raise AssertionError("startup failed, so no request should run")

    wrapped = make_asgi_app(_lifespan(trace, "never", fail_enter=True), router)
    inbox, outbox, task = _start_lifespan(wrapped)

    await inbox.put({"type": "lifespan.startup"})
    assert await outbox.get() == {"type": "lifespan.startup.failed", "message": "setup boom"}
    assert trace.log == ["enter"]

    await task


async def test_teardown_failure_is_reported_as_shutdown_failed() -> None:
    trace = Trace()

    def router(state: str, scope: HttpScope) -> Processor[Inbound, Outbound]:
        raise AssertionError("no request in this test")

    wrapped = make_asgi_app(_lifespan(trace, "ready", fail_exit=True), router)
    inbox, outbox, task = _start_lifespan(wrapped)

    await inbox.put({"type": "lifespan.startup"})
    assert await outbox.get() == {"type": "lifespan.startup.complete"}

    await inbox.put({"type": "lifespan.shutdown"})
    assert await outbox.get() == {"type": "lifespan.shutdown.failed", "message": "teardown boom"}
    assert trace.log == ["enter", "exit"]

    await task
