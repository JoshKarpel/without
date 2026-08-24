from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Iterable
from datetime import timedelta
from itertools import pairwise

import pytest
from hypothesis import given
from hypothesis import strategies as st
from without import stream_from_iterable
from without_asgi import EVENT_STREAM_HEADERS
from without_asgi import Checkpoint
from without_asgi import Comment
from without_asgi import Event
from without_asgi import Outbound
from without_asgi import Received
from without_asgi import ReceivedEvent
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_asgi import Retry
from without_asgi import ServerSentEvent
from without_asgi import encode_event
from without_asgi import event_stream
from without_asgi import parse_events
from without_asgi import parse_events_with_directives
from without_asgi import with_heartbeat

# Far longer than any test's runtime, so a heartbeat only fires where one is the point.
NEVER = timedelta(hours=1)
# Short enough that a silent stream beats immediately, without being a synchronization
# device: every test that uses it asserts on what arrives, never on elapsed time.
AT_ONCE = timedelta(0)


async def chunked(*chunks: bytes) -> AsyncIterator[bytes]:
    for chunk in chunks:
        yield chunk


async def collected(wire: bytes, **kwargs: object) -> list[ReceivedEvent]:
    """Parse one whole byte string, the common shape for a conformance case."""
    return [event async for event in parse_events(chunked(wire), **kwargs)]  # type: ignore[arg-type]


async def collected_with_directives(wire: bytes) -> list[Received]:
    return [item async for item in parse_events_with_directives(chunked(wire))]


def encode_all(events: Iterable[ServerSentEvent]) -> bytes:
    return b"".join(encode_event(event) for event in events)


def message(data: str, *, event_id: str = "") -> ReceivedEvent:
    """The default-typed event most conformance cases expect."""
    return ReceivedEvent(data=data, type="message", id=event_id)


class TestEncoding:
    def test_an_event_renders_its_fields_then_a_blank_line(self) -> None:
        assert encode_event(Event(data="hello", type="greeting", id="7")) == b"event: greeting\nid: 7\ndata: hello\n\n"

    def test_a_bare_event_renders_only_a_data_line(self) -> None:
        assert encode_event(Event(data="hello")) == b"data: hello\n\n"

    @pytest.mark.parametrize("event_type", ["message", ""])
    def test_the_default_type_renders_no_event_line(self, event_type: str) -> None:
        # The format cannot tell an absent `event:` from `event: message`, so writing
        # one would spend bytes on every event to say nothing.
        assert encode_event(Event(data="hello", type=event_type)) == b"data: hello\n\n"

    def test_empty_data_still_renders_a_data_line_so_the_event_dispatches(self) -> None:
        assert encode_event(Event(data="")) == b"data: \n\n"

    def test_an_absent_id_renders_no_id_line_leaving_the_resumption_point_alone(self) -> None:
        assert encode_event(Event(data="hello", id=None)) == b"data: hello\n\n"

    def test_an_empty_id_renders_an_id_line_that_clears_the_resumption_point(self) -> None:
        # The distinction `None` versus `""` exists precisely because these differ.
        assert encode_event(Event(data="hello", id="")) == b"id: \ndata: hello\n\n"

    @pytest.mark.parametrize("newline", ["\n", "\r\n", "\r"])
    def test_a_newline_in_data_becomes_another_data_line(self, newline: str) -> None:
        assert encode_event(Event(data=f"one{newline}two")) == b"data: one\ndata: two\n\n"

    def test_data_forging_a_field_is_carried_as_data_rather_than_a_field(self) -> None:
        # The injection case: a hand-rolled `f"data: {payload}\n\n"` would hand the peer
        # a second event named `admin`.
        hostile = "harmless\n\nevent: admin\ndata: escalate"
        assert encode_event(Event(data=hostile)) == (
            b"data: harmless\ndata: \ndata: event: admin\ndata: data: escalate\n\n"
        )

    def test_a_forged_field_arrives_as_data_rather_than_as_an_event(self) -> None:
        wire = encode_event(Event(data="harmless\n\nevent: admin\ndata: escalate", type="safe"))
        assert b"\nevent: admin" not in wire

    def test_a_comment_renders_a_colon_line(self) -> None:
        assert encode_event(Comment("keepalive")) == b":keepalive\n\n"

    def test_a_newline_in_a_comment_becomes_another_comment_line(self) -> None:
        assert encode_event(Comment("one\ntwo")) == b":one\n:two\n\n"

    def test_an_empty_comment_is_the_minimal_heartbeat(self) -> None:
        assert encode_event(Comment()) == b":\n\n"

    def test_retry_renders_as_whole_milliseconds(self) -> None:
        assert encode_event(Retry(timedelta(milliseconds=1500))) == b"retry: 1500\n\n"

    def test_a_checkpoint_renders_an_id_with_no_data(self) -> None:
        assert encode_event(Checkpoint("42")) == b"id: 42\n\n"

    def test_a_non_ascii_value_is_encoded_as_utf_8(self) -> None:
        assert encode_event(Event(data="héllo €")) == "data: héllo €\n\n".encode()

    @pytest.mark.parametrize("bad", ["a\nb", "a\rb", "a\r\nb"])
    def test_a_newline_in_the_event_type_is_rejected_at_construction(self, bad: str) -> None:
        with pytest.raises(ValueError, match="event type cannot contain a newline"):
            Event(data="x", type=bad)

    @pytest.mark.parametrize("bad", ["a\nb", "a\rb"])
    def test_a_newline_in_an_event_id_is_rejected_at_construction(self, bad: str) -> None:
        with pytest.raises(ValueError, match="event id cannot contain a newline"):
            Event(data="x", id=bad)

    def test_a_newline_in_a_checkpoint_id_is_rejected_at_construction(self) -> None:
        with pytest.raises(ValueError, match="event id cannot contain a newline"):
            Checkpoint("a\nb")

    @pytest.mark.parametrize("build", [lambda bad: Event(data="x", id=bad), Checkpoint])
    def test_a_nul_in_an_id_is_rejected_because_a_peer_would_silently_drop_it(self, build: object) -> None:
        with pytest.raises(ValueError, match="event id cannot contain a NUL"):
            build("a\x00b")  # type: ignore[operator]

    def test_a_negative_reconnection_time_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="reconnection time cannot be negative"):
            Retry(timedelta(seconds=-1))


class TestParsing:
    async def test_a_bare_data_line_dispatches_an_event_typed_message(self) -> None:
        assert await collected(b"data: hello\n\n") == [message("hello")]

    async def test_all_the_fields_are_read(self) -> None:
        wire = b"event: greeting\nid: 7\ndata: hello\n\n"
        assert await collected(wire) == [ReceivedEvent(data="hello", type="greeting", id="7")]

    async def test_repeated_data_lines_are_joined_with_line_feeds(self) -> None:
        assert await collected(b"data: one\ndata: two\ndata: three\n\n") == [message("one\ntwo\nthree")]

    async def test_exactly_one_leading_space_is_stripped_from_a_value(self) -> None:
        assert await collected(b"data:  padded\n\n") == [message(" padded")]

    async def test_a_field_with_no_colon_is_a_name_with_an_empty_value(self) -> None:
        assert await collected(b"data\n\n") == [message("")]

    async def test_a_comment_line_is_ignored_and_dispatches_nothing(self) -> None:
        assert await collected(b": keepalive\n\n") == []

    async def test_a_comment_is_dropped_even_by_the_full_parser(self) -> None:
        # The spec says to ignore comments, so `Comment` is an outbound-only arm.
        assert await collected_with_directives(b": keepalive\n\n") == []

    async def test_an_unknown_field_is_ignored_rather_than_raising(self) -> None:
        # A producer adding a field must not break a consumer that predates it.
        assert await collected(b"data: hello\nfuture: whatever\n\n") == [message("hello")]

    async def test_an_empty_data_buffer_dispatches_nothing(self) -> None:
        assert await collected(b"event: ping\n\ndata: real\n\n") == [message("real")]

    async def test_an_event_left_incomplete_at_end_of_stream_is_discarded(self) -> None:
        assert await collected(b"data: first\n\ndata: truncated\n") == [message("first")]

    async def test_the_last_event_id_persists_across_later_events(self) -> None:
        wire = b"id: 1\ndata: a\n\ndata: b\n\nid: 2\ndata: c\n\n"
        assert await collected(wire) == [
            message("a", event_id="1"),
            message("b", event_id="1"),
            message("c", event_id="2"),
        ]

    async def test_the_event_type_does_not_persist_across_later_events(self) -> None:
        assert await collected(b"event: named\ndata: a\n\ndata: b\n\n") == [
            ReceivedEvent(data="a", type="named", id=""),
            message("b"),
        ]

    async def test_an_id_containing_a_nul_is_ignored_leaving_the_previous_one(self) -> None:
        wire = b"id: keep\ndata: a\n\nid: bad\x00id\ndata: b\n\n"
        assert await collected(wire) == [message("a", event_id="keep"), message("b", event_id="keep")]

    async def test_an_empty_event_field_falls_back_to_message(self) -> None:
        assert await collected(b"event\ndata: a\n\n") == [message("a")]

    @pytest.mark.parametrize("terminator", [b"\n", b"\r\n", b"\r"])
    async def test_each_line_terminator_is_recognized(self, terminator: bytes) -> None:
        assert await collected(terminator.join([b"data: hello", b"", b""])) == [message("hello")]

    async def test_one_leading_byte_order_mark_is_stripped(self) -> None:
        assert await collected(b"\xef\xbb\xbfdata: hello\n\n") == [message("hello")]

    async def test_only_the_first_byte_order_mark_is_stripped(self) -> None:
        # The second one survives into the line, so the field name is `\ufeffdata`,
        # which is not a name this format defines and is therefore ignored.
        assert await collected(b"\xef\xbb\xbf\xef\xbb\xbfdata: hello\n\n") == []

    async def test_a_byte_order_mark_later_in_the_stream_is_not_stripped(self) -> None:
        assert await collected(b"data: a\n\n\xef\xbb\xbfdata: b\n\n") == [message("a")]

    async def test_malformed_utf_8_becomes_replacement_characters_rather_than_raising(self) -> None:
        # Raising would let a hostile producer kill its consumers.
        assert await collected(b"data: \xff\xfe\n\n") == [message("\ufffd\ufffd")]

    async def test_a_line_separator_inside_data_does_not_split_the_line(self) -> None:
        # `str.splitlines` splits on U+2028; the format does not, and the difference
        # would let a value forge a field.
        assert await collected("data: a\u2028event: forged\n\n".encode()) == [message("a\u2028event: forged")]

    @pytest.mark.parametrize("payload", [b"\x0b", b"\x0c", b"\x1c", b"\xc2\x85"])
    async def test_other_characters_python_treats_as_line_breaks_stay_in_data(self, payload: bytes) -> None:
        events = await collected(b"data: a" + payload + b"b\n\n")
        assert len(events) == 1
        assert events[0].data.startswith("a")
        assert events[0].data.endswith("b")


class TestDirectives:
    async def test_a_retry_directive_is_surfaced_as_its_own_item(self) -> None:
        assert await collected_with_directives(b"retry: 4000\ndata: hello\n\n") == [
            Retry(after=timedelta(seconds=4)),
            message("hello"),
        ]

    async def test_a_retry_directive_alone_dispatches_no_event_but_is_still_surfaced(self) -> None:
        # The reason `Retry` is its own arm: there is no event here to hang it on.
        assert await collected_with_directives(b"retry: 4000\n\n") == [Retry(after=timedelta(seconds=4))]

    async def test_directives_are_dropped_by_the_common_parser(self) -> None:
        assert await collected(b"retry: 4000\ndata: hello\n\n") == [message("hello")]

    @pytest.mark.parametrize("value", [b"", b"soon", b"-1", b"1.5", b"4000ms", "\u0663".encode()])
    async def test_a_retry_value_that_is_not_ascii_digits_is_ignored(self, value: bytes) -> None:
        # U+0663 is an Arabic-Indic three: `str.isdigit` accepts it and the format does not.
        assert await collected_with_directives(b"retry: " + value + b"\ndata: x\n\n") == [message("x")]

    async def test_an_id_only_frame_is_surfaced_as_a_checkpoint(self) -> None:
        # The spec sets the last event ID string before returning early on empty data,
        # so this moves the resumption point without delivering anything.
        assert await collected_with_directives(b"id: 99\n\n") == [Checkpoint(id="99")]

    async def test_a_checkpoint_at_the_tail_survives_the_end_of_the_stream(self) -> None:
        assert await collected_with_directives(b"data: hello\n\nid: 99\n\n") == [
            message("hello"),
            Checkpoint(id="99"),
        ]

    async def test_an_id_carried_by_an_event_is_not_also_a_checkpoint(self) -> None:
        assert await collected_with_directives(b"id: 7\ndata: hello\n\n") == [message("hello", event_id="7")]

    async def test_a_checkpoint_is_reported_once_rather_than_per_frame(self) -> None:
        assert await collected_with_directives(b"id: 7\n\n: ping\n\n: ping\n\n") == [Checkpoint(id="7")]

    async def test_a_checkpoint_precedes_the_event_that_inherits_its_id(self) -> None:
        assert await collected_with_directives(b"id: 7\n\ndata: hello\n\n") == [
            Checkpoint(id="7"),
            message("hello", event_id="7"),
        ]

    async def test_checkpoints_are_dropped_by_the_common_parser(self) -> None:
        assert await collected(b"id: 99\n\n") == []


class TestChunkBoundaries:
    async def test_a_crlf_split_across_chunks_is_one_terminator(self) -> None:
        events = [event async for event in parse_events(chunked(b"data: hello\r", b"\n\r\n"))]
        assert events == [message("hello")]

    async def test_a_multi_byte_character_split_across_chunks_is_decoded_whole(self) -> None:
        events = [event async for event in parse_events(chunked(b"data: \xe2\x82", b"\xac\n\n"))]
        assert events == [message("€")]

    async def test_a_byte_order_mark_split_across_chunks_is_still_stripped(self) -> None:
        events = [event async for event in parse_events(chunked(b"\xef\xbb", b"\xbfdata: hi\n\n"))]
        assert events == [message("hi")]

    async def test_a_lone_trailing_cr_does_not_dispatch_until_the_next_chunk_settles_it(self) -> None:
        # Ending the line on the CR would dispatch on a blank line the stream never sent.
        events = [event async for event in parse_events(chunked(b"data: a\r", b"\ndata: b\n\n"))]
        assert events == [message("a\nb")]


class TestSizeCap:
    async def test_the_cap_is_absent_by_default(self) -> None:
        events = await collected(b"data: " + b"x" * 100_000 + b"\n\n")
        assert len(events[0].data) == 100_000

    async def test_data_that_never_dispatches_trips_the_cap(self) -> None:
        endless = chunked(*[b"data: filler\n"] * 50)
        with pytest.raises(ValueError, match="exceeded max_event_size"):
            _ = [event async for event in parse_events(endless, max_event_size=64)]

    async def test_a_single_line_that_never_ends_trips_the_cap(self) -> None:
        with pytest.raises(ValueError, match="exceeded max_event_size"):
            _ = [event async for event in parse_events(chunked(b"data: " + b"x" * 500), max_event_size=64)]

    async def test_dispatching_resets_the_cap_so_a_long_stream_of_small_events_passes(self) -> None:
        assert len(await collected(b"data: small\n\n" * 100, max_event_size=64)) == 100

    async def test_heartbeat_comments_retain_nothing_and_never_trip_the_cap(self) -> None:
        # A comment is discarded as its line ends, which is why the cap counts retained
        # state rather than bytes consumed.
        heartbeats = chunked(*[b": keepalive\n"] * 500)
        assert [event async for event in parse_events(heartbeats, max_event_size=16)] == []


class TestHeartbeat:
    async def test_a_silent_stream_is_kept_alive_by_beats(self) -> None:
        beats = with_heartbeat(_silent(), every=AT_ONCE)
        assert [await anext(beats) for _ in range(3)] == [Comment(), Comment(), Comment()]
        await beats.aclose()

    async def test_a_busy_stream_gets_no_beats_at_all(self) -> None:
        events: list[ServerSentEvent] = [Event(data="a"), Event(data="b"), Event(data="c")]
        assert [item async for item in with_heartbeat(stream_from_iterable(events), every=NEVER)] == events

    async def test_the_beat_is_configurable(self) -> None:
        beats = with_heartbeat(_silent(), every=AT_ONCE, beat=Checkpoint("42"))
        assert await anext(beats) == Checkpoint("42")
        await beats.aclose()

    async def test_the_stream_ends_when_the_source_does(self) -> None:
        assert [item async for item in with_heartbeat(stream_from_iterable([]), every=NEVER)] == []

    async def test_an_exception_from_the_source_propagates_rather_than_beating_forever(self) -> None:
        async def failing() -> AsyncIterator[ServerSentEvent]:
            yield Event(data="a")
            raise RuntimeError("upstream gave up")

        with pytest.raises(RuntimeError, match="upstream gave up"):
            _ = [item async for item in with_heartbeat(failing(), every=NEVER)]

    async def test_closing_the_wrapper_closes_the_source(self) -> None:
        closed = asyncio.Event()

        async def watched() -> AsyncIterator[ServerSentEvent]:
            try:
                yield Event(data="a")
            finally:
                closed.set()

        beats = with_heartbeat(watched(), every=NEVER)
        assert await anext(beats) == Event(data="a")
        await beats.aclose()
        assert closed.is_set()

    async def test_beats_interleave_with_the_events_that_do_arrive(self) -> None:
        handed: asyncio.Queue[ServerSentEvent] = asyncio.Queue()

        async def paced() -> AsyncIterator[ServerSentEvent]:
            while True:
                yield await handed.get()

        beats = with_heartbeat(paced(), every=AT_ONCE)
        assert await anext(beats) == Comment()
        await handed.put(Event(data="a"))
        # The pull was already in flight when the interval lapsed, so it is still
        # outstanding and delivers rather than being cancelled and lost.
        seen = [await anext(beats) for _ in range(6)]
        assert Event(data="a") in seen
        await beats.aclose()


class TestEventStreamResponse:
    async def test_it_yields_a_head_then_one_body_event_per_frame(self) -> None:
        frames: list[ServerSentEvent] = [Event(data="a"), Comment("ping"), Event(data="b")]
        assert [event async for event in event_stream(stream_from_iterable(frames))] == [
            ResponseStart(status=200, headers=EVENT_STREAM_HEADERS),
            ResponseBody(b"data: a\n\n", more_body=True),
            ResponseBody(b":ping\n\n", more_body=True),
            ResponseBody(b"data: b\n\n", more_body=True),
            ResponseBody(b"", more_body=False),
        ]

    async def test_every_frame_is_its_own_chunk_so_none_waits_on_the_next(self) -> None:
        events = stream_from_iterable([Event(data=str(n)) for n in range(5)])
        bodies = [event async for event in event_stream(events) if isinstance(event, ResponseBody)]
        assert [body.more_body for body in bodies] == [True] * 5 + [False]

    async def test_the_default_headers_name_the_media_type_and_decline_caching(self) -> None:
        start = await anext(event_stream(stream_from_iterable([])))
        assert isinstance(start, ResponseStart)
        assert start.headers == ((b"content-type", b"text/event-stream"), (b"cache-control", b"no-store"))

    async def test_caller_headers_win_over_the_defaults(self) -> None:
        outbound: list[Outbound] = [
            event
            async for event in event_stream(
                stream_from_iterable([]),
                headers=((b"cache-control", b"no-cache"), (b"x-accel-buffering", b"no")),
            )
        ]
        start = outbound[0]
        assert isinstance(start, ResponseStart)
        assert start.headers == (
            (b"content-type", b"text/event-stream"),
            (b"cache-control", b"no-cache"),
            (b"x-accel-buffering", b"no"),
        )

    async def test_no_content_length_is_declared(self) -> None:
        start = await anext(event_stream(stream_from_iterable([])))
        assert isinstance(start, ResponseStart)
        assert not any(name == b"content-length" for name, _ in start.headers)


async def _silent() -> AsyncIterator[ServerSentEvent]:
    """A source that never produces, so only heartbeats can come out of the wrapper."""
    await asyncio.Event().wait()
    yield Event(data="unreachable")  # pragma: no cover


# Text that exercises the corners the format cares about: the three terminators, the
# leading space a parser strips, colons, a BOM, and a line separator `str.splitlines`
# would wrongly split on.
_TEXT = st.text(alphabet=st.sampled_from("ab \r\n:\u2028\ufeff€\x00"), max_size=24)
_IDS = st.text(alphabet=st.sampled_from("ab :\u2028€"), max_size=8)


@st.composite
def _events(draw: st.DrawFn) -> Event:
    return Event(data=draw(_TEXT), type=draw(_IDS.filter(bool) | st.just("message")), id=draw(st.none() | _IDS))


def normalized(data: str) -> str:
    """`data` as the peer will see it: the wire has one spelling for a line break."""
    return data.replace("\r\n", "\n").replace("\r", "\n")


# Module level rather than in a class: a `@given` test inside one is called from a
# different executor per event loop the suite parametrizes over, which Hypothesis flags
# as a correctness risk.
@given(events=st.lists(_events(), max_size=6))
async def test_every_event_survives_the_round_trip(events: list[Event]) -> None:
    parsed = await collected(encode_all(events))
    assert [event.data for event in parsed] == [normalized(event.data) for event in events]
    assert [event.type for event in parsed] == [event.type for event in events]


@pytest.mark.parametrize("terminator", ["\r\n", "\r"])
async def test_a_carriage_return_in_data_arrives_as_a_line_feed(terminator: str) -> None:
    # Not lossy by accident: the format spells every line break the same way on the
    # wire, and the peer rejoins multi-line data with U+000A.
    assert await collected(encode_event(Event(data=f"one{terminator}two"))) == [message("one\ntwo")]


@given(
    events=st.lists(_events(), max_size=6),
    splits=st.lists(st.integers(0, 400), max_size=8),
    terminator=st.sampled_from([b"\n", b"\r\n", b"\r"]),
)
async def test_the_parse_is_the_same_however_the_bytes_are_chunked(
    events: list[Event], splits: list[int], terminator: bytes
) -> None:
    # The property that catches a CRLF pair or a multi-byte character straddling a
    # chunk boundary, which is where a hand-rolled parser fails. `terminator` re-spells
    # the wire's line endings, since the encoder only ever writes U+000A and a split
    # could otherwise never land between a CR and the LF completing it. Every U+000A in
    # an encoded stream *is* a terminator, because a value carrying one was split into
    # separate lines on the way out.
    wire = encode_all(events).replace(b"\n", terminator)
    points = sorted({0, len(wire)} | {point for point in splits if 0 < point < len(wire)})
    chunks = [wire[start:end] for start, end in pairwise(points)]
    whole = await collected(wire)
    piecewise = [event async for event in parse_events(chunked(*chunks))]
    assert piecewise == whole
    assert whole == await collected(encode_all(events))


@given(data=_TEXT)
async def test_no_data_value_can_forge_a_field(data: str) -> None:
    parsed = await collected(encode_event(Event(data=data, type="fixed", id="fixed")))
    assert parsed == [ReceivedEvent(data=normalized(data), type="fixed", id="fixed")]


@given(directives=st.lists(st.integers(0, 9999).map(lambda ms: Retry(timedelta(milliseconds=ms))), max_size=4))
async def test_retry_directives_survive_the_round_trip(directives: list[Retry]) -> None:
    assert await collected_with_directives(encode_all(directives)) == directives


@given(ids=st.lists(_IDS.filter(bool), min_size=1, max_size=4, unique=True))
async def test_checkpoints_survive_the_round_trip(ids: list[str]) -> None:
    checkpoints = [Checkpoint(event_id) for event_id in ids]
    assert await collected_with_directives(encode_all(checkpoints)) == checkpoints
