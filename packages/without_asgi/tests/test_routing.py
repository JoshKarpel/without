from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace

import pytest
from without import Stream
from without import stream_from_iterable
from without_asgi import Disconnect
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import RequestBody
from without_asgi import Response
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_asgi.outbound import encode_response
from without_asgi.routing import _BodyTooLarge
from without_asgi.routing import limit_concurrent_requests
from without_asgi.routing import limit_request_body

from .helpers import a_scope


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
    return a_scope(path="/items", http_version="2")


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


@dataclass(slots=True)
class _Started:
    hit: bool = False


def _draining_handler(started: _Started | None = None) -> HttpHandler:
    """Consume the whole input stream, then reply 200. Records that it ran if given a flag."""

    async def handler(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
        if started is not None:
            started.hit = True
        async for _event in inputs:
            pass
        for event in encode_response(Response(status=200, headers=((b"content-type", b"text/plain"),), body=b"ok")):
            yield event

    return handler


def _early_start_handler() -> HttpHandler:
    """Emit the response start *before* reading the body, so an overflow trips mid-response."""

    async def handler(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
        yield ResponseStart(status=200, headers=((b"content-type", b"text/plain"),))
        async for _event in inputs:
            pass
        yield ResponseBody(body=b"done", more_body=False)

    return handler


def _scope_with_headers(headers: tuple[tuple[bytes, bytes], ...]) -> HttpScope:
    return replace(_scope(), headers=headers)


async def _collect_over(handler: HttpHandler, events: tuple[Inbound, ...]) -> list[Outbound]:
    return [event async for event in handler(stream_from_iterable(events))]


@pytest.mark.security("an over-cap Content-Length is rejected with 413 before the body is read")
async def test_rejects_up_front_when_content_length_exceeds_the_cap() -> None:
    started = _Started()
    middleware = limit_request_body(10)
    handler = middleware(_draining_handler(started), object(), _scope_with_headers(((b"content-length", b"100"),)))

    events = await _collect_over(handler, (RequestBody(body=b"x" * 100, more_body=False),))

    assert _status(events) == 413
    assert not started.hit  # the inner handler was never invoked


async def test_admits_a_body_whose_content_length_is_within_the_cap() -> None:
    started = _Started()
    middleware = limit_request_body(10)
    scope = _scope_with_headers(((b"accept", b"*/*"), (b"content-length", b"5")))
    handler = middleware(_draining_handler(started), object(), scope)

    events = await _collect_over(handler, (RequestBody(body=b"hello", more_body=False),))

    assert _status(events) == 200
    assert started.hit  # the inner handler ran for an in-cap request


async def test_ignores_a_non_integer_content_length() -> None:
    middleware = limit_request_body(10)
    handler = middleware(_draining_handler(), object(), _scope_with_headers(((b"content-length", b"not-a-number"),)))

    events = await _collect_over(handler, (RequestBody(body=b"tiny", more_body=False),))

    assert _status(events) == 200


@pytest.mark.security("a chunked body that lies about or omits its length is capped at 413")
async def test_rejects_a_chunked_body_that_passes_the_cap() -> None:
    middleware = limit_request_body(10)
    handler = middleware(_draining_handler(), object(), _scope())

    events = await _collect_over(
        handler,
        (RequestBody(body=b"x" * 6, more_body=True), RequestBody(body=b"x" * 8, more_body=False)),
    )

    assert _status(events) == 413


async def test_passes_a_chunked_body_within_the_cap() -> None:
    middleware = limit_request_body(100)
    handler = middleware(_draining_handler(), object(), _scope())

    events = await _collect_over(handler, (RequestBody(body=b"abc", more_body=True), Disconnect()))

    assert _status(events) == 200


async def test_passes_the_request_stream_to_an_admitted_handler() -> None:
    started = _Started()
    middleware = limit_concurrent_requests(2)
    handler = middleware(_draining_handler(started), object(), _scope())

    events = await _collect_over(handler, (RequestBody(body=b"payload", more_body=False),))

    assert _status(events) == 200
    assert started.hit  # the handler consumed the real input stream, not None


async def test_admits_a_chunked_body_of_exactly_the_cap() -> None:
    middleware = limit_request_body(10)
    handler = middleware(_draining_handler(), object(), _scope())

    events = await _collect_over(handler, (RequestBody(body=b"x" * 10, more_body=False),))

    assert _status(events) == 200  # total == max_bytes is within the cap, not over it


async def test_admits_a_declared_length_equal_to_the_cap() -> None:
    started = _Started()
    middleware = limit_request_body(10)
    handler = middleware(_draining_handler(started), object(), _scope_with_headers(((b"content-length", b"10"),)))

    events = await _collect_over(handler, (RequestBody(body=b"y" * 10, more_body=False),))

    assert _status(events) == 200  # a declared length equal to the cap is not rejected up front
    assert started.hit


async def test_surfaces_an_overflow_after_the_handler_has_started() -> None:
    middleware = limit_request_body(10)
    handler = middleware(_early_start_handler(), object(), _scope())

    with pytest.raises(_BodyTooLarge):
        await _collect_over(
            handler,
            (RequestBody(body=b"x" * 6, more_body=True), RequestBody(body=b"x" * 8, more_body=False)),
        )
