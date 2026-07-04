from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from dataclasses import field

from without import Stream
from without import stream_from_iterable
from without_asgi import Asgi
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import Response
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_asgi.outbound import encode_response
from without_asgi.routing import limit_concurrent_requests


@dataclass(slots=True)
class _Gate:
    release: asyncio.Event = field(default_factory=asyncio.Event)
    started: int = 0


def _holding_handler(gate: _Gate) -> HttpHandler:
    """An HTTP handler that records it started, blocks until released, then replies 200."""

    async def handler(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
        gate.started += 1
        await gate.release.wait()
        for event in encode_response(Response(status=200, headers=((b"content-type", b"text/plain"),), body=b"ok")):
            yield event

    return handler


def _scope() -> HttpScope:
    return HttpScope(
        asgi=Asgi(version="3.0", spec_version="2.4"),
        http_version="2",
        method="GET",
        scheme="http",
        path="/items",
        raw_path=b"/items",
        query_string=b"",
        root_path="",
        headers=(),
        client=None,
        server=None,
        extensions=None,
    )


async def _collect(handler: HttpHandler) -> list[Outbound]:
    return [event async for event in handler(stream_from_iterable(()))]


def _status(events: list[Outbound]) -> int:
    start = events[0]
    assert isinstance(start, ResponseStart)
    return start.status


async def _started_reaching(gate: _Gate, count: int) -> None:
    async with asyncio.timeout(5):
        while gate.started < count:
            await asyncio.sleep(0.001)


async def test_admits_requests_up_to_the_limit() -> None:
    gate = _Gate()
    middleware = limit_concurrent_requests(3)
    handler = middleware(_holding_handler(gate), object(), _scope())

    in_flight = [asyncio.create_task(_collect(handler)) for _ in range(3)]
    try:
        await _started_reaching(gate, 3)
        assert gate.started == 3  # all three reached the inner handler
    finally:
        gate.release.set()
        await asyncio.gather(*in_flight)


async def test_sheds_an_extra_concurrent_request_with_503() -> None:
    gate = _Gate()
    middleware = limit_concurrent_requests(2)
    handler = middleware(_holding_handler(gate), object(), _scope())

    in_flight = [asyncio.create_task(_collect(handler)) for _ in range(2)]
    try:
        await _started_reaching(gate, 2)
        shed = await _collect(handler)
    finally:
        gate.release.set()
        await asyncio.gather(*in_flight)

    assert _status(shed) == 503
    start = shed[0]
    assert isinstance(start, ResponseStart)
    assert (b"retry-after", b"1") in start.headers
    assert gate.started == 2  # the shed request never reached the inner handler


async def test_sheds_with_a_caller_supplied_overload_response() -> None:
    gate = _Gate()
    overloaded = Response(
        status=429,
        headers=((b"content-type", b"application/json"), (b"retry-after", b"30")),
        body=b'{"error":"too busy"}',
    )
    middleware = limit_concurrent_requests(1, overloaded=overloaded)
    handler = middleware(_holding_handler(gate), object(), _scope())

    held = asyncio.create_task(_collect(handler))
    try:
        await _started_reaching(gate, 1)
        shed = await _collect(handler)
    finally:
        gate.release.set()
        await held

    assert _status(shed) == 429
    start = shed[0]
    assert isinstance(start, ResponseStart)
    assert (b"retry-after", b"30") in start.headers
    body = shed[1]
    assert isinstance(body, ResponseBody)
    assert body.body == b'{"error":"too busy"}'


async def test_releases_the_slot_when_a_request_finishes() -> None:
    gate = _Gate()
    middleware = limit_concurrent_requests(1)
    handler = middleware(_holding_handler(gate), object(), _scope())

    first = asyncio.create_task(_collect(handler))
    await _started_reaching(gate, 1)
    gate.release.set()
    assert _status(await first) == 200

    gate.release.clear()
    second = asyncio.create_task(_collect(handler))
    try:
        await _started_reaching(gate, 2)
        assert gate.started == 2  # the freed slot admitted the next request
    finally:
        gate.release.set()
        await second
