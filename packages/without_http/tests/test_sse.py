from __future__ import annotations

from collections.abc import AsyncGenerator
from collections.abc import Iterator
from datetime import timedelta

import pytest
from pytest_mock import MockerFixture
from without_asgi import ReceivedEvent
from without_http import ClientRequest
from without_http import ClientResponse
from without_http import NotAnEventStream
from without_http import ReadTimeout
from without_http import ResponseBody
from without_http import ResponseHead
from without_http import ResponseTrailers
from without_http import subscribe

FEED = ClientRequest("GET", "https://api.test/feed")
EVENT_STREAM = ((b"content-type", b"text/event-stream"),)


def stream(*chunks: bytes, then: type[Exception] | None = None) -> ClientResponse:
    """A `200 text/event-stream` response over `chunks`, optionally cut short by `then`."""

    async def events() -> AsyncGenerator[bytes | ResponseTrailers]:
        for chunk in chunks:
            yield chunk
        if then is not None:
            raise then()

    return ClientResponse(ResponseHead(200, EVENT_STREAM), ResponseBody(events()))


class Recorder:
    """
    A `Client` answering from a script, recording the requests it was given.

    Running out of script raises, which is what ends a `subscribe` that would otherwise
    reconnect forever: the loop only swallows connection errors and timeouts.
    """

    def __init__(self, *responses: ClientResponse | Exception) -> None:
        self.requests: list[ClientRequest] = []
        self._responses: Iterator[ClientResponse | Exception] = iter(responses)

    async def __call__(self, request: ClientRequest) -> ClientResponse:
        self.requests.append(request)
        answer = next(self._responses, None)
        if answer is None:  # pragma: no cover - a healthy test never runs past its script
            raise AssertionError(f"unscripted request number {len(self.requests)} to {request.url}")
        if isinstance(answer, Exception):
            raise answer
        return answer

    def last_event_ids(self) -> list[bytes | None]:
        return [
            next((value for name, value in request.headers if name == b"last-event-id"), None)
            for request in self.requests
        ]


async def no_sleep(duration: timedelta) -> None:
    """Collapse the reconnection wait so a test never spends wall-clock on it."""


async def take(events: AsyncGenerator[ReceivedEvent], count: int) -> list[ReceivedEvent]:
    """The first `count` events, then close the subscription the way a caller would."""
    taken = [await anext(events) for _ in range(count)]
    await events.aclose()
    return taken


class TestOneConnection:
    async def test_it_yields_the_events_the_stream_carries(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n", b"event: tick\ndata: b\n\n"))
        assert await take(subscribe(recorder, FEED, sleep=no_sleep), 2) == [
            ReceivedEvent(type="message", data="a", id=""),
            ReceivedEvent(type="tick", data="b", id=""),
        ]

    async def test_a_caller_can_stop_consuming_and_the_body_is_closed(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n", b"data: b\n\n"))
        events = subscribe(recorder, FEED, sleep=no_sleep)
        assert await take(events, 1) == [ReceivedEvent(type="message", data="a", id="")]
        assert len(recorder.requests) == 1

    async def test_it_offers_the_event_stream_media_type(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n"))
        await take(subscribe(recorder, FEED, sleep=no_sleep), 1)
        assert (b"accept", b"text/event-stream") in recorder.requests[0].headers

    async def test_a_caller_supplied_accept_wins(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n"))
        request = ClientRequest("GET", "https://api.test/feed", headers=((b"accept", b"text/event-stream; q=0.9"),))
        await take(subscribe(recorder, request, sleep=no_sleep), 1)
        accepts = [value for name, value in recorder.requests[0].headers if name == b"accept"]
        assert accepts == [b"text/event-stream; q=0.9"]

    async def test_the_first_request_carries_no_last_event_id(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n"))
        await take(subscribe(recorder, FEED, sleep=no_sleep), 1)
        assert recorder.last_event_ids() == [None]


class TestTerminalFailures:
    @pytest.mark.parametrize("status", [204, 301, 404, 500])
    async def test_a_non_200_status_raises_and_never_reconnects(self, status: int) -> None:
        recorder = Recorder(ClientResponse(ResponseHead(status, EVENT_STREAM), ResponseBody(_empty())))
        with pytest.raises(NotAnEventStream):
            _ = [event async for event in subscribe(recorder, FEED, sleep=no_sleep)]
        assert len(recorder.requests) == 1

    @pytest.mark.parametrize("content_type", [b"text/html", b"application/json", b"text/plain"])
    async def test_a_wrong_content_type_raises_and_never_reconnects(self, content_type: bytes) -> None:
        head = ResponseHead(200, ((b"content-type", content_type),))
        recorder = Recorder(ClientResponse(head, ResponseBody(_empty())))
        with pytest.raises(NotAnEventStream):
            _ = [event async for event in subscribe(recorder, FEED, sleep=no_sleep)]
        assert len(recorder.requests) == 1

    async def test_a_missing_content_type_raises(self) -> None:
        recorder = Recorder(ClientResponse(ResponseHead(200, ()), ResponseBody(_empty())))
        with pytest.raises(NotAnEventStream):
            _ = [event async for event in subscribe(recorder, FEED, sleep=no_sleep)]

    async def test_media_type_parameters_are_tolerated(self) -> None:
        head = ResponseHead(200, ((b"Content-Type", b"text/event-stream; charset=utf-8"),))

        async def body() -> AsyncGenerator[bytes | ResponseTrailers]:
            yield b"data: a\n\n"

        recorder = Recorder(ClientResponse(head, ResponseBody(body())))
        assert await take(subscribe(recorder, FEED, sleep=no_sleep), 1) == [
            ReceivedEvent(type="message", data="a", id="")
        ]

    async def test_the_first_connection_error_propagates_rather_than_looping(self) -> None:
        recorder = Recorder(ConnectionRefusedError("nothing listening"))
        with pytest.raises(ConnectionRefusedError):
            _ = [event async for event in subscribe(recorder, FEED, sleep=no_sleep)]
        assert len(recorder.requests) == 1


class TestReconnection:
    async def test_a_stream_that_ends_is_reconnected(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n"), stream(b"data: b\n\n"))
        events = await take(subscribe(recorder, FEED, sleep=no_sleep), 2)
        assert [event.data for event in events] == ["a", "b"]
        assert len(recorder.requests) == 2

    async def test_the_last_id_seen_resumes_the_next_connection(self) -> None:
        recorder = Recorder(stream(b"id: 41\ndata: a\n\n"), stream(b"id: 42\ndata: b\n\n"), stream(b"data: c\n\n"))
        await take(subscribe(recorder, FEED, sleep=no_sleep), 3)
        assert recorder.last_event_ids() == [None, b"41", b"42"]

    async def test_a_checkpoint_moves_the_resumption_point_without_delivering_an_event(self) -> None:
        # An id-only frame dispatches nothing, so its id reaches the loop only as a
        # `Checkpoint`. Missing it resumes from before whatever the producer skipped.
        recorder = Recorder(stream(b"data: a\n\nid: 99\n\n"), stream(b"data: b\n\n"))
        events = await take(subscribe(recorder, FEED, sleep=no_sleep), 2)
        assert [event.data for event in events] == ["a", "b"]
        assert recorder.last_event_ids() == [None, b"99"]

    async def test_a_checkpoint_before_any_event_still_resumes(self) -> None:
        recorder = Recorder(stream(b"id: 7\n\n"), stream(b"data: a\n\n"))
        await take(subscribe(recorder, FEED, sleep=no_sleep), 1)
        assert recorder.last_event_ids() == [None, b"7"]

    async def test_a_later_event_wins_over_an_earlier_checkpoint(self) -> None:
        recorder = Recorder(stream(b"id: 7\n\nid: 8\ndata: a\n\n"), stream(b"data: b\n\n"))
        await take(subscribe(recorder, FEED, sleep=no_sleep), 2)
        assert recorder.last_event_ids() == [None, b"8"]

    async def test_a_connection_error_mid_stream_reconnects_rather_than_surfacing(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n", then=ConnectionResetError), stream(b"data: b\n\n"))
        assert [event.data for event in await take(subscribe(recorder, FEED, sleep=no_sleep), 2)] == ["a", "b"]

    async def test_a_read_timeout_mid_stream_reconnects(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n", then=ReadTimeout), stream(b"data: b\n\n"))
        assert [event.data for event in await take(subscribe(recorder, FEED, sleep=no_sleep), 2)] == ["a", "b"]

    async def test_a_failed_reconnect_is_retried_once_a_stream_was_established(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n"), ConnectionRefusedError("blip"), stream(b"data: b\n\n"))
        assert [event.data for event in await take(subscribe(recorder, FEED, sleep=no_sleep), 2)] == ["a", "b"]
        assert len(recorder.requests) == 3

    async def test_a_terminal_failure_after_a_reconnect_still_raises(self) -> None:
        # Establishing a stream once does not make a later `404` retryable.
        recorder = Recorder(stream(b"data: a\n\n"), ClientResponse(ResponseHead(404, ()), ResponseBody(_empty())))
        with pytest.raises(NotAnEventStream):
            _ = [event async for event in subscribe(recorder, FEED, sleep=no_sleep)]


class TestReconnectionTiming:
    async def test_the_default_wait_is_used_until_the_producer_names_one(self) -> None:
        waits = await _waits(Recorder(stream(b"data: a\n\n"), stream(b"data: b\n\n")), count=2)
        assert waits == [timedelta(seconds=3)]

    async def test_a_retry_directive_sets_the_wait(self) -> None:
        recorder = Recorder(stream(b"retry: 7000\ndata: a\n\n"), stream(b"data: b\n\n"))
        assert await _waits(recorder, count=2) == [timedelta(seconds=7)]

    async def test_a_retry_directive_with_no_event_still_sets_the_wait(self) -> None:
        recorder = Recorder(stream(b"retry: 7000\n\ndata: a\n\n"), stream(b"data: b\n\n"))
        assert await _waits(recorder, count=2) == [timedelta(seconds=7)]

    async def test_the_wait_persists_across_reconnections(self) -> None:
        recorder = Recorder(stream(b"retry: 7000\ndata: a\n\n"), stream(b"data: b\n\n"), stream(b"data: c\n\n"))
        assert await _waits(recorder, count=3) == [timedelta(seconds=7), timedelta(seconds=7)]

    @pytest.mark.parametrize("directive", [b"0", b"1", b"50"])
    async def test_a_tiny_retry_is_floored_so_it_cannot_spin(self, directive: bytes) -> None:
        # A hostile or broken `retry: 0` would otherwise be a hot reconnect loop.
        recorder = Recorder(stream(b"retry: " + directive + b"\ndata: a\n\n"), stream(b"data: b\n\n"))
        assert await _waits(recorder, count=2) == [timedelta(milliseconds=100)]

    async def test_a_custom_initial_wait_is_honoured(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n"), stream(b"data: b\n\n"))
        assert await _waits(recorder, count=2, reconnect=timedelta(seconds=30)) == [timedelta(seconds=30)]

    async def test_the_default_sleep_waits_in_seconds(self, mocker: MockerFixture) -> None:
        # The default is the one collaborator a caller cannot inject past, and the only
        # thing it does is convert the unit. Asserting that conversion rather than
        # elapsed wall-clock keeps the test deterministic: `asyncio.sleep` may return
        # marginally before `time.monotonic` shows the full interval.
        slept = mocker.patch("without_http.sse.asyncio.sleep")
        recorder = Recorder(stream(b"data: a\n\n"), stream(b"data: b\n\n"))
        assert len(await take(subscribe(recorder, FEED, reconnect=timedelta(milliseconds=1500)), 2)) == 2
        slept.assert_awaited_once_with(1.5)


class TestParserPassthrough:
    async def test_the_size_cap_reaches_the_parser(self) -> None:
        recorder = Recorder(stream(b"data: " + b"x" * 500))
        with pytest.raises(ValueError, match="exceeded max_event_size"):
            _ = [event async for event in subscribe(recorder, FEED, sleep=no_sleep, max_event_size=64)]

    async def test_events_split_across_chunks_are_reassembled(self) -> None:
        recorder = Recorder(stream(b"data: he", b"llo\n", b"\n"))
        assert await take(subscribe(recorder, FEED, sleep=no_sleep), 1) == [
            ReceivedEvent(type="message", data="hello", id="")
        ]

    async def test_comments_are_dropped_rather_than_surfaced(self) -> None:
        recorder = Recorder(stream(b": keepalive\n\ndata: a\n\n"))
        assert await take(subscribe(recorder, FEED, sleep=no_sleep), 1) == [
            ReceivedEvent(type="message", data="a", id="")
        ]


async def _empty() -> AsyncGenerator[bytes | ResponseTrailers]:
    # The body of a response `subscribe` rejects: constructed and closed, never started,
    # so neither line below ever runs.
    return  # pragma: no cover
    yield  # pragma: no cover


async def _waits(recorder: Recorder, *, count: int, **kwargs: object) -> list[timedelta]:
    """Drive `count` events, recording every reconnection wait the loop asked for."""
    recorded: list[timedelta] = []

    async def record(duration: timedelta) -> None:
        recorded.append(duration)

    await take(subscribe(recorder, FEED, sleep=record, **kwargs), count)  # type: ignore[arg-type]
    return recorded
