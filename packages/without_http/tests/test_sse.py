from __future__ import annotations

from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterator
from datetime import timedelta

import pytest
from pytest_mock import MockerFixture
from without import Stream
from without_asgi import RawHeaders
from without_asgi import ReceivedEvent
from without_asgi.headers import merge
from without_http import ClientRequest
from without_http import ClientResponse
from without_http import NotAnEventStream
from without_http import ReadTimeout
from without_http import ResponseBody
from without_http import ResponseHead
from without_http import ResponseTrailers
from without_http import subscribe
from without_http.testing import respond

FEED = "https://api.test/feed"
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


def feed(recorder: Recorder, headers: RawHeaders = ()) -> Callable[[RawHeaders], Awaitable[ClientResponse]]:
    """Open the feed through `recorder`, building a fresh `GET` for each attempt."""
    return lambda offered: recorder(ClientRequest("GET", FEED, merge(offered, headers)))


async def drained(recorder: Recorder, **kwargs: object) -> list[ReceivedEvent]:
    """Consume a subscription to its end, which is where the script runs out or it raises."""
    return [event async for event in subscribe(feed(recorder), sleep=no_sleep, **kwargs)]  # type: ignore[arg-type]


async def take(events: AsyncGenerator[ReceivedEvent], count: int) -> list[ReceivedEvent]:
    """The first `count` events, then close the subscription the way a caller would."""
    taken = [await anext(events) for _ in range(count)]
    await events.aclose()
    return taken


class TestOneConnection:
    async def test_it_yields_the_events_the_stream_carries(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n", b"event: tick\ndata: b\n\n"))
        assert await take(subscribe(feed(recorder), sleep=no_sleep), 2) == [
            ReceivedEvent(type="message", data="a", id=""),
            ReceivedEvent(type="tick", data="b", id=""),
        ]

    async def test_a_caller_can_stop_consuming_and_the_body_is_closed(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n", b"data: b\n\n"))
        events = subscribe(feed(recorder), sleep=no_sleep)
        assert await take(events, 1) == [ReceivedEvent(type="message", data="a", id="")]
        assert len(recorder.requests) == 1

    async def test_it_offers_the_event_stream_media_type(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n"))
        await take(subscribe(feed(recorder), sleep=no_sleep), 1)
        assert (b"accept", b"text/event-stream") in recorder.requests[0].headers

    async def test_a_caller_merging_over_the_offer_wins_on_accept(self) -> None:
        # Which side wins is the caller's to decide now that they build the request, so
        # this pins the documented composition rather than a policy the loop holds.
        recorder = Recorder(stream(b"data: a\n\n"))
        narrower = ((b"accept", b"text/event-stream; q=0.9"),)
        await take(subscribe(feed(recorder, narrower), sleep=no_sleep), 1)
        accepts = [value for name, value in recorder.requests[0].headers if name == b"accept"]
        assert accepts == [b"text/event-stream; q=0.9"]

    async def test_the_first_request_carries_no_last_event_id(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n"))
        await take(subscribe(feed(recorder), sleep=no_sleep), 1)
        assert recorder.last_event_ids() == [None]

    async def test_each_attempt_builds_its_own_request_so_a_one_shot_body_survives(self) -> None:
        # The reason `attempt` is a function: a `ClientRequest`'s body is iterable once,
        # so a single request value reused across attempts would put the payload on the
        # wire for the first connection and nothing at all for every one after it.
        recorder = Recorder(stream(b"data: a\n\n"), stream(b"data: b\n\n"))

        async def payload() -> AsyncGenerator[bytes]:
            yield b"subscribe me"

        await take(
            subscribe(lambda headers: recorder(ClientRequest("POST", FEED, headers, payload())), sleep=no_sleep), 2
        )
        assert [await _read(request.body) for request in recorder.requests] == [b"subscribe me", b"subscribe me"]


class TestTerminalFailures:
    @pytest.mark.parametrize("status", [204, 301, 404, 500])
    async def test_a_non_200_status_raises_and_never_reconnects(self, status: int) -> None:
        recorder = Recorder(respond(status, headers=EVENT_STREAM))
        with pytest.raises(NotAnEventStream):
            await drained(recorder)
        assert len(recorder.requests) == 1

    @pytest.mark.parametrize("content_type", [b"text/html", b"application/json", b"text/plain"])
    async def test_a_wrong_content_type_raises_and_never_reconnects(self, content_type: bytes) -> None:
        recorder = Recorder(respond(headers=((b"content-type", content_type),)))
        with pytest.raises(NotAnEventStream):
            await drained(recorder)
        assert len(recorder.requests) == 1

    async def test_a_missing_content_type_raises(self) -> None:
        recorder = Recorder(respond())
        with pytest.raises(NotAnEventStream):
            await drained(recorder)

    async def test_media_type_parameters_are_tolerated(self) -> None:
        head = ResponseHead(200, ((b"Content-Type", b"text/event-stream; charset=utf-8"),))

        async def body() -> AsyncGenerator[bytes | ResponseTrailers]:
            yield b"data: a\n\n"

        recorder = Recorder(ClientResponse(head, ResponseBody(body())))
        assert await take(subscribe(feed(recorder), sleep=no_sleep), 1) == [
            ReceivedEvent(type="message", data="a", id="")
        ]

    async def test_the_first_connection_error_propagates_rather_than_looping(self) -> None:
        recorder = Recorder(ConnectionRefusedError("nothing listening"))
        with pytest.raises(ConnectionRefusedError):
            await drained(recorder)
        assert len(recorder.requests) == 1


class TestReconnection:
    async def test_a_stream_that_ends_is_reconnected(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n"), stream(b"data: b\n\n"))
        events = await take(subscribe(feed(recorder), sleep=no_sleep), 2)
        assert [event.data for event in events] == ["a", "b"]
        assert len(recorder.requests) == 2

    async def test_the_last_id_seen_resumes_the_next_connection(self) -> None:
        recorder = Recorder(stream(b"id: 41\ndata: a\n\n"), stream(b"id: 42\ndata: b\n\n"), stream(b"data: c\n\n"))
        await take(subscribe(feed(recorder), sleep=no_sleep), 3)
        assert recorder.last_event_ids() == [None, b"41", b"42"]

    async def test_an_event_without_an_id_keeps_the_point_the_connection_resumed_from(self) -> None:
        # A producer that stamps ids periodically sends id-less events in between. Each
        # connection's parser is seeded with the id it resumed from, so those events
        # report it rather than erasing it and replaying the feed from the beginning.
        recorder = Recorder(stream(b"id: 41\ndata: a\n\n"), stream(b"data: b\n\n"), stream(b"data: c\n\n"))
        events = await take(subscribe(feed(recorder), sleep=no_sleep), 3)
        assert [event.id for event in events] == ["41", "41", "41"]
        assert recorder.last_event_ids() == [None, b"41", b"41"]

    async def test_a_checkpoint_moves_the_resumption_point_without_delivering_an_event(self) -> None:
        # An id-only frame dispatches nothing, so its id reaches the loop only as a
        # `Checkpoint`. Missing it resumes from before whatever the producer skipped.
        recorder = Recorder(stream(b"data: a\n\nid: 99\n\n"), stream(b"data: b\n\n"))
        events = await take(subscribe(feed(recorder), sleep=no_sleep), 2)
        assert [event.data for event in events] == ["a", "b"]
        assert recorder.last_event_ids() == [None, b"99"]

    async def test_a_checkpoint_before_any_event_still_resumes(self) -> None:
        recorder = Recorder(stream(b"id: 7\n\n"), stream(b"data: a\n\n"))
        await take(subscribe(feed(recorder), sleep=no_sleep), 1)
        assert recorder.last_event_ids() == [None, b"7"]

    async def test_an_id_carrying_a_control_character_is_not_reflected_into_the_header(self) -> None:
        # The parser drops it, so the reconnect carries the last id that *was* spellable
        # rather than a header value h11 would refuse, which would end the subscription.
        recorder = Recorder(stream(b"id: keep\ndata: a\n\nid: bad\x0bid\ndata: b\n\n"), stream(b"data: c\n\n"))
        await take(subscribe(feed(recorder), sleep=no_sleep), 3)
        assert recorder.last_event_ids() == [None, b"keep"]

    async def test_a_later_event_wins_over_an_earlier_checkpoint(self) -> None:
        recorder = Recorder(stream(b"id: 7\n\nid: 8\ndata: a\n\n"), stream(b"data: b\n\n"))
        await take(subscribe(feed(recorder), sleep=no_sleep), 2)
        assert recorder.last_event_ids() == [None, b"8"]

    async def test_a_connection_error_mid_stream_reconnects_rather_than_surfacing(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n", then=ConnectionResetError), stream(b"data: b\n\n"))
        assert [event.data for event in await take(subscribe(feed(recorder), sleep=no_sleep), 2)] == ["a", "b"]

    async def test_a_read_timeout_mid_stream_reconnects(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n", then=ReadTimeout), stream(b"data: b\n\n"))
        assert [event.data for event in await take(subscribe(feed(recorder), sleep=no_sleep), 2)] == ["a", "b"]

    async def test_a_failed_reconnect_is_retried_once_a_stream_was_established(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n"), ConnectionRefusedError("blip"), stream(b"data: b\n\n"))
        assert [event.data for event in await take(subscribe(feed(recorder), sleep=no_sleep), 2)] == ["a", "b"]
        assert len(recorder.requests) == 3

    async def test_a_terminal_failure_after_a_reconnect_still_raises(self) -> None:
        # Establishing a stream once does not make a later `404` retryable.
        recorder = Recorder(stream(b"data: a\n\n"), respond(404))
        with pytest.raises(NotAnEventStream):
            await drained(recorder)


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

    @pytest.mark.parametrize("directive", [b"300001", b"8640000000000", b"9" * 16])
    async def test_an_enormous_retry_is_capped_so_it_cannot_park(self, directive: bytes) -> None:
        # `retry: 8640000000000` is a plausible garble of `86400000`, and uncapped it
        # would leave the subscription silent for centuries with nothing raised.
        recorder = Recorder(stream(b"retry: " + directive + b"\ndata: a\n\n"), stream(b"data: b\n\n"))
        assert await _waits(recorder, count=2) == [timedelta(minutes=5)]

    async def test_a_custom_initial_wait_is_honoured(self) -> None:
        recorder = Recorder(stream(b"data: a\n\n"), stream(b"data: b\n\n"))
        assert await _waits(recorder, count=2, reconnect=timedelta(seconds=30)) == [timedelta(seconds=30)]

    async def test_a_custom_floor_is_honoured(self) -> None:
        recorder = Recorder(stream(b"retry: 50\ndata: a\n\n"), stream(b"data: b\n\n"))
        waits = await _waits(recorder, count=2, minimum_reconnect=timedelta(seconds=2))
        assert waits == [timedelta(seconds=2)]

    async def test_a_custom_ceiling_is_honoured(self) -> None:
        # Raised past the default five minutes, so a producer this caller trusts names a
        # longer backoff, and clamped at the value the caller chose rather than the default.
        recorder = Recorder(stream(b"retry: 7200000\ndata: a\n\n"), stream(b"data: b\n\n"))
        waits = await _waits(recorder, count=2, maximum_reconnect=timedelta(hours=1))
        assert waits == [timedelta(hours=1)]

    @pytest.mark.parametrize(
        ("call", "message"),
        [
            (lambda: subscribe(feed(Recorder()), minimum_reconnect=timedelta(minutes=10)), "reconnection window"),
            (lambda: subscribe(feed(Recorder()), maximum_reconnect=timedelta(milliseconds=10)), "reconnection window"),
            (lambda: subscribe(feed(Recorder()), minimum_reconnect=timedelta(seconds=-1)), "reconnection window"),
            (lambda: subscribe(feed(Recorder()), reconnect=timedelta(seconds=-1)), "wait cannot be negative"),
        ],
    )
    def test_a_contradictory_window_is_refused_where_the_caller_wrote_it(
        self, call: Callable[[], object], message: str
    ) -> None:
        # Never iterated, and the `Recorder` holds no script: the point is that the
        # mistake is reported at the call rather than deferred to the first `anext`,
        # which is after a request has already gone out.
        with pytest.raises(ValueError, match=message):
            call()

    async def test_a_ceiling_equal_to_the_floor_pins_every_wait(self) -> None:
        # The degenerate window is legal, since it is a caller saying they want one value
        # whatever the producer asks for.
        pinned = timedelta(seconds=9)
        recorder = Recorder(stream(b"retry: 50\ndata: a\n\n"), stream(b"data: b\n\n"))
        waits = await _waits(recorder, count=2, minimum_reconnect=pinned, maximum_reconnect=pinned)
        assert waits == [pinned]

    async def test_the_default_sleep_waits_in_seconds(self, mocker: MockerFixture) -> None:
        # The default is the one collaborator a caller cannot inject past, and the only
        # thing it does is convert the unit. Asserting that conversion rather than
        # elapsed wall-clock keeps the test deterministic: `asyncio.sleep` may return
        # marginally before `time.monotonic` shows the full interval.
        slept = mocker.patch("without_http.sse.asyncio.sleep")
        recorder = Recorder(stream(b"data: a\n\n"), stream(b"data: b\n\n"))
        assert len(await take(subscribe(feed(recorder), reconnect=timedelta(milliseconds=1500)), 2)) == 2
        slept.assert_awaited_once_with(1.5)


class TestParserPassthrough:
    async def test_the_size_cap_reaches_the_parser(self) -> None:
        recorder = Recorder(stream(b"data: " + b"x" * 500))
        with pytest.raises(ValueError, match="exceeded max_event_size"):
            await drained(recorder, max_event_size=64)

    async def test_events_split_across_chunks_are_reassembled(self) -> None:
        recorder = Recorder(stream(b"data: he", b"llo\n", b"\n"))
        assert await take(subscribe(feed(recorder), sleep=no_sleep), 1) == [
            ReceivedEvent(type="message", data="hello", id="")
        ]

    async def test_comments_are_dropped_rather_than_surfaced(self) -> None:
        recorder = Recorder(stream(b": keepalive\n\ndata: a\n\n"))
        assert await take(subscribe(feed(recorder), sleep=no_sleep), 1) == [
            ReceivedEvent(type="message", data="a", id="")
        ]


async def _read(body: Stream[bytes]) -> bytes:
    return b"".join([chunk async for chunk in body])


async def _waits(recorder: Recorder, *, count: int, **kwargs: object) -> list[timedelta]:
    """Drive `count` events, recording every reconnection wait the loop asked for."""
    recorded: list[timedelta] = []

    async def record(duration: timedelta) -> None:
        recorded.append(duration)

    await take(subscribe(feed(recorder), sleep=record, **kwargs), count)  # type: ignore[arg-type]
    return recorded
