from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import pytest
from without_asgi import ASGIApp, Lifespan, Message, Receive, Scope, ScopedApp, Send, with_lifespan


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


def _recording_app(seen: list[str]) -> ScopedApp[str]:
    async def app(state: str, scope: Scope, receive: Receive, send: Send) -> None:
        seen.append(state)

    return app


def _start_lifespan(
    app: ASGIApp,
) -> tuple[asyncio.Queue[Message], asyncio.Queue[Message], asyncio.Task[None]]:
    inbox: asyncio.Queue[Message] = asyncio.Queue()
    outbox: asyncio.Queue[Message] = asyncio.Queue()

    async def receive() -> Message:
        return await inbox.get()

    async def send(message: Message) -> None:
        await outbox.put(message)

    async def drive() -> None:
        await app({"type": "lifespan"}, receive, send)

    task = asyncio.create_task(drive())
    return inbox, outbox, task


async def test_drives_the_handshake_and_enters_then_exits_the_lifespan() -> None:
    trace = Trace()

    async def app(state: str, scope: Scope, receive: Receive, send: Send) -> None:
        raise AssertionError("no request in this test")

    wrapped = with_lifespan(_lifespan(trace, "ready"), app)
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
    app = _recording_app(seen)
    wrapped = with_lifespan(_lifespan(Trace(), "the-state"), app)
    inbox, outbox, task = _start_lifespan(wrapped)

    await inbox.put({"type": "lifespan.startup"})
    await outbox.get()

    async def receive() -> Message:
        raise AssertionError("this handler reads no events")

    async def send(message: Message) -> None:
        raise AssertionError("this handler sends nothing")

    await wrapped({"type": "http"}, receive, send)
    assert seen == ["the-state"]

    await inbox.put({"type": "lifespan.shutdown"})
    await outbox.get()
    await task


async def test_a_request_before_startup_fails_loud() -> None:
    seen: list[str] = []
    app = _recording_app(seen)
    wrapped = with_lifespan(_lifespan(Trace(), "unused"), app)

    async def receive() -> Message:
        raise AssertionError("unreached")

    async def send(message: Message) -> None:
        raise AssertionError("unreached")

    with pytest.raises(RuntimeError, match="startup has not completed"):
        await wrapped({"type": "http"}, receive, send)
    assert seen == []


async def test_setup_failure_is_reported_as_startup_failed() -> None:
    trace = Trace()

    async def app(state: str, scope: Scope, receive: Receive, send: Send) -> None:
        raise AssertionError("startup failed, so no request should run")

    wrapped = with_lifespan(_lifespan(trace, "never", fail_enter=True), app)
    inbox, outbox, task = _start_lifespan(wrapped)

    await inbox.put({"type": "lifespan.startup"})
    assert await outbox.get() == {"type": "lifespan.startup.failed", "message": "setup boom"}
    assert trace.log == ["enter"]

    await task


async def test_teardown_failure_is_reported_as_shutdown_failed() -> None:
    trace = Trace()

    async def app(state: str, scope: Scope, receive: Receive, send: Send) -> None:
        raise AssertionError("no request in this test")

    wrapped = with_lifespan(_lifespan(trace, "ready", fail_exit=True), app)
    inbox, outbox, task = _start_lifespan(wrapped)

    await inbox.put({"type": "lifespan.startup"})
    assert await outbox.get() == {"type": "lifespan.startup.complete"}

    await inbox.put({"type": "lifespan.shutdown"})
    assert await outbox.get() == {"type": "lifespan.shutdown.failed", "message": "teardown boom"}
    assert trace.log == ["enter", "exit"]

    await task
