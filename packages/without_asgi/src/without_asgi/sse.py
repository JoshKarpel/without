from __future__ import annotations

import asyncio
import codecs
import re
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import assert_never

from without import Stream

from without_asgi.headers import merge
from without_asgi.outbound import Outbound
from without_asgi.outbound import ResponseBody
from without_asgi.outbound import ResponseStart
from without_asgi.types import RawHeaders

# Server-Sent Events as two pure transforms: `encode_event` renders one frame to bytes
# and `parse_events` is `Stream[bytes] -> Stream[ReceivedEvent]`. Neither touches a
# socket, which is what lets the format live at this layer rather than beside a
# transport: a handler streams events under any ASGI server, and a client parses them
# with nothing from `without-http` but the byte stream it already holds. The
# reconnecting loop that acts on `id` and `retry` is the one half that *does* need a
# transport, and it lives in `without-http` (`subscribe`).
#
# `with_heartbeat` is the one thing here that is not pure: it reads a clock, because
# what it exists to do is notice silence. It stays in this module rather than beside
# the transport because it needs no transport either, only a stream and an interval.
#
# The wire format is the WHATWG event stream format, implemented against its
# interpretation algorithm:
# https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation

__all__ = [
    "DEFAULT_EVENT_TYPE",
    "DEFAULT_HEARTBEAT_INTERVAL",
    "EVENT_STREAM_HEADERS",
    "EVENT_STREAM_MEDIA_TYPE",
    "HEARTBEAT",
    "Checkpoint",
    "Comment",
    "Event",
    "Received",
    "ReceivedEvent",
    "Retry",
    "ServerSentEvent",
    "encode_event",
    "event_stream",
    "parse_events",
    "parse_events_with_directives",
    "with_heartbeat",
]

EVENT_STREAM_MEDIA_TYPE = b"text/event-stream"

# No `charset` parameter: an event stream is defined as UTF-8, so unlike `html_content`
# there is no encoding left for the recipient to guess. `no-store` because a cache
# holding a prefix of a stream that never ends would serve it as a whole response.
EVENT_STREAM_HEADERS: RawHeaders = (
    (b"content-type", EVENT_STREAM_MEDIA_TYPE),
    (b"cache-control", b"no-store"),
)

# The three terminators the format allows, and *only* those. `str.splitlines` also
# splits on U+000B, U+000C, U+001C-U+001E, U+0085, U+2028 and U+2029, so a `data` value
# carrying any of them would become two lines here and one line at a conformant peer.
# That difference is an event injection vector, not just a conformance bug.
_NEWLINE = re.compile(r"\r\n|\r|\n")

# The type a peer assumes when a frame names none. The format cannot distinguish an
# absent `event:` line from `event: message` or from an empty `event: `, so all three
# are this, and the encoder writes no line for it.
DEFAULT_EVENT_TYPE = "message"


def _no_newline(field: str, value: str) -> None:
    r"""
    Reject a value that cannot be spelled in a single-line field.

    A newline would end the field and let the value forge another one, so it raises
    rather than being silently stripped, and it raises where the frame is built rather
    than mid-stream after the response head is already committed. This is the header
    injection problem, and it is what a hand-rolled `f"data: {payload}\n\n"` gets wrong.
    """
    if _NEWLINE.search(value):
        raise ValueError(f"an event {field} cannot contain a newline: {value!r}")


def _no_nul(value: str) -> None:
    # The spec has a peer *ignore* an id containing NUL, so sending one is an id
    # silently dropped on arrival rather than an error either side sees.
    if "\x00" in value:
        raise ValueError(f"an event id cannot contain a NUL: {value!r}")


@dataclass(frozen=True, slots=True)
class Event:
    r"""
    A frame that dispatches an event: the only kind that delivers anything.

    `data` is what makes it an event, so it is required here rather than optional the
    way it would be on a type covering every frame. `type` names the event, defaulting
    to the same `message` a peer assumes when no name is given. `id` makes the event a
    resumption point, echoed back in `Last-Event-ID` after a reconnect.

    `id` is the one field whose shape differs from `ReceivedEvent`'s, and the asymmetry
    is real rather than an oversight: `None` sends no `id:` line, which leaves the
    peer's resumption point where it was, while `""` sends an empty one, which *clears*
    it. There is no inbound counterpart to that choice, since a parser only ever
    reports the point the stream currently sits on. `type` needs no such option because
    the format cannot tell the two apart: an absent `event:` line, `event: message`,
    and `event: ` all arrive as `message`, so the encoder writes no line for the
    default and the distinction never reaches the wire.

    A newline in `data` is carried, by splitting the value across as many `data:` lines
    as it needs and letting the peer rejoin them. The break survives; *which* break
    does not, because the format spells all three terminators the same way and the peer
    rejoins with a line feed, so a `\r\n` or a lone `\r` inside `data` arrives as `\n`.
    Normalize before sending if the distinction matters, or send a format that can
    carry it (a JSON string in `data`).
    """

    data: str
    type: str = DEFAULT_EVENT_TYPE
    id: str | None = None

    def __post_init__(self) -> None:
        _no_newline("type", self.type)
        if self.id is not None:
            _no_newline("id", self.id)
            _no_nul(self.id)


@dataclass(frozen=True, slots=True)
class Comment:
    """
    A `:` line, which every parser ignores: the conventional heartbeat.

    It keeps intermediaries from reaping an idle connection without dispatching an
    event, and costs a consumer no memory, since a comment is discarded as its line
    ends. A newline in `text` is carried by splitting it across as many `:` lines as it
    needs, because a raw one would leave the remainder on a line the peer reads as a
    field.
    """

    text: str = ""


@dataclass(frozen=True, slots=True)
class Retry:
    """
    A `retry:` directive: how long a client should wait before reconnecting.

    Its own frame rather than a field on `Event`, because it is a property of the
    *stream*: a frame carrying only `retry:` dispatches no event, so a value hung on an
    event would have nowhere to live when a producer sent one on its own. Sent in whole
    milliseconds; `without-http`'s `subscribe` is what acts on it.
    """

    after: timedelta

    def __post_init__(self) -> None:
        if self.after < timedelta():
            raise ValueError(f"a reconnection time cannot be negative: {self.after!r}")


@dataclass(frozen=True, slots=True)
class Checkpoint:
    """
    An `id:` frame carrying no data: advance the resumption point, deliver nothing.

    The spec's dispatch sets the last event ID string *before* it returns early on an
    empty data buffer, so this moves where a reconnecting client resumes from without
    delivering an event. That is what a producer sends after skipping work a consumer
    asked not to see: a filtered batch, a compacted range, a heartbeat that is also a
    position. Without it the consumer would replay from before the skip.
    """

    id: str

    def __post_init__(self) -> None:
        _no_newline("id", self.id)
        _no_nul(self.id)


# What a handler sends. A union rather than one type with five optional fields,
# because only these four combinations mean anything: everything else such a type
# would permit is either a no-op on the wire (a frame with no data and no directive)
# or silently discarded on arrival (an `event:` naming a frame that dispatches
# nothing). Splitting them costs nothing, since a frame carrying several of these at
# once is equivalent to sending them one after another.
type ServerSentEvent = Event | Comment | Retry | Checkpoint


@dataclass(frozen=True, slots=True)
class ReceivedEvent:
    """
    One event as parsed off a stream: the inbound counterpart to `Event`.

    No defaults, because the parser always supplies every field, so a field it forgot
    fails loudly instead of arriving as a plausible blank. The fields line up with
    `Event`'s except that `id` is a plain `str` here: the spec's dispatch substitutes
    `message` for an unnamed type, and the last event id *persists across events*, so
    an event whose own frame carried no `id:` still reports whichever id the stream
    last set (`""` until it sets one). Only a sender gets to choose between leaving
    that point alone and clearing it, which is why only `Event.id` is optional.

    `id` never contains a carriage return or line feed, since those are what ended the
    field. That is what makes reflecting it into a `Last-Event-ID` request header safe.
    """

    data: str
    type: str
    id: str


# What a parser yields. `Comment` is absent, which looks asymmetric against
# `ServerSentEvent` and is not: a heartbeat is meaningful to send and the spec says to
# ignore it on receipt. `Retry` and `Checkpoint` are the same types in both directions,
# rather than split the way `Event` and `ReceivedEvent` are, because the inbound/
# outbound rule exists so a parsed type cannot silently default a field its parser
# forgot, and a type with one required field has no default to mask anything.
type Received = ReceivedEvent | Retry | Checkpoint


# The default beat `with_heartbeat` sends: a bare `:` line, the cheapest frame there is.
HEARTBEAT = Comment()

# How long `with_heartbeat` lets a stream sit silent, comfortably under the 30 to 60
# seconds an idle-connection reaper is usually set to.
DEFAULT_HEARTBEAT_INTERVAL = timedelta(seconds=15)


async def with_heartbeat(
    events: Stream[ServerSentEvent],
    *,
    every: timedelta = DEFAULT_HEARTBEAT_INTERVAL,
    beat: ServerSentEvent = HEARTBEAT,
) -> AsyncGenerator[ServerSentEvent]:
    """
    Re-emit `events`, inserting `beat` whenever the stream has been silent for `every`.

    An idle timer, not a metronome: the interval restarts on each frame that goes out,
    so a busy stream sends no heartbeats at all and a silent one sends exactly as many
    as it needs. Merging a fixed-rate tick instead would spend a frame every interval no
    matter how much real traffic there was.

    Reach for it whenever a stream can go quiet for longer than an intermediary will
    tolerate. A proxy, a load balancer, or a NAT table reaps an idle connection after
    30 to 60 seconds, and the client learns about it only as a drop; a comment costs
    three bytes, dispatches no event, and retains nothing in the peer's parser, so the
    connection stays alive without the consumer seeing anything.

    ```python
    return event_stream(with_heartbeat(progress(job)))
    ```

    `beat` is any frame, so a deployment that needs the traffic to carry meaning can
    send something else: a `Checkpoint` to double as a resumption point, or a typed
    `Event` if a browser client wants to observe liveness. It defaults to a bare
    comment because that is the only frame a conformant consumer is guaranteed to
    ignore.

    This is the one thing in this module that reads a clock; the encoder and parser
    stay pure. It pulls the source into a task so a lapsed interval leaves that pull
    running: bounding `anext` with a timeout instead would cancel the pull and lose
    whatever the source was about to produce. An `AsyncGenerator` rather than a bare
    `AsyncIterator`, because it owns that task: `aclose()` cancels the pull and closes
    the source there and then, rather than at whenever the collector gets to it.
    """
    source = aiter(events)
    pull = asyncio.ensure_future(anext(source))
    try:
        while True:
            done, _ = await asyncio.wait((pull,), timeout=every.total_seconds())
            if not done:
                yield beat
                continue
            try:
                event = pull.result()
            except StopAsyncIteration:
                return
            yield event
            pull = asyncio.ensure_future(anext(source))
    finally:
        pull.cancel()
        with suppress(BaseException):
            await pull
        if isinstance(source, AsyncGenerator):
            await source.aclose()


def _data_lines(data: str) -> list[str]:
    return [f"data: {piece}" for piece in _NEWLINE.split(data)]


def encode_event(event: ServerSentEvent) -> bytes:
    """
    Render one frame as its wire bytes, terminated by the blank line that ends it.

    Pure and total: every arm of `ServerSentEvent` that exists has already been checked
    for the values that cannot be spelled (see each `__post_init__`), so there is
    nothing left to reject here. Every frame is terminated, `Comment` included, so each
    one is self-contained and a `Checkpoint` takes effect on arrival rather than
    waiting for whatever a producer sends next.
    """
    match event:
        case Event(data, event_type, event_id):
            # No line for the default type: the format has no way to tell it from an
            # absent one, so writing it would spend bytes on every event to say nothing.
            named = [] if event_type in ("", DEFAULT_EVENT_TYPE) else [f"event: {event_type}"]
            identified = [] if event_id is None else [f"id: {event_id}"]
            lines = [*named, *identified, *_data_lines(data)]
        case Comment(text):
            # Each piece gets its own `:`, since a raw newline would leave the
            # remainder of the comment on a line the peer reads as a field.
            lines = [f":{piece}" for piece in _NEWLINE.split(text)]
        case Retry(after):
            lines = [f"retry: {after // timedelta(milliseconds=1)}"]
        case Checkpoint(event_id):
            lines = [f"id: {event_id}"]
        case _ as unreachable:
            assert_never(unreachable)
    return "\n".join([*lines, "", ""]).encode()


async def event_stream(
    events: Stream[ServerSentEvent],
    *,
    status: int = 200,
    headers: RawHeaders = (),
) -> AsyncIterator[Outbound]:
    """
    Serve a stream of frames as the `ResponseStart` + `ResponseBody` event stream a
    handler yields.

    The `file_response` analog for an event stream, and the shape `without-web`'s
    `Reply` already accepts. Each frame becomes one `ResponseBody` carrying
    `more_body=True`, so the transport writes it as its own chunk and the client sees
    it when it happens; the stream ends with the empty final body. One chunk per frame
    is the contract rather than an implementation detail: an event that sits in a
    buffer has not been delivered, so the chunk boundary is where the flush happens.

    ```python
    async def ticks(state, match) -> Reply:
        return event_stream(counter(), headers=((b"x-accel-buffering", b"no"),))
    ```

    `headers` is layered over the defaults, so a caller wins on any name they also
    set. Those defaults are `content-type: text/event-stream` and `cache-control:
    no-store`, and deliberately stop there:

    - **No `connection: keep-alive`**, which most SSE advice recommends. It is already
      the HTTP/1.1 default, and it is a forbidden header in HTTP/2 and HTTP/3, so
      sending it ranges from redundant to a protocol error.
    - **No `x-accel-buffering: no`**, the header that stops nginx buffering the
      response into 32 KB lumps and destroying the streaming this whole module exists
      for. It is real, it is the most common way an event stream breaks in
      production, and it is also one proxy vendor's deployment policy, which this
      layer does not hold (the same reason no access log ships). Pass it, as above,
      when nginx is in front, and read the deployment notes in the guide for the rest
      of the chain: a CDN, an ETag middleware, or anything else that wants to see a
      whole body will re-buffer a stream that this header freed.

    There is no `content-length`, and there cannot be. Compression is separately
    declined for this media type; see `is_compressible`.
    """
    yield ResponseStart(status=status, headers=merge(EVENT_STREAM_HEADERS, headers))
    async for event in events:
        yield ResponseBody(encode_event(event), more_body=True)
    yield ResponseBody(b"", more_body=False)


def _split_lines(text: str) -> tuple[list[str], str]:
    """Complete lines and the trailing partial one, splitting only on the three terminators."""
    # A trailing CR is held back rather than treated as a terminator: the next chunk may
    # open with the LF that completes a CRLF pair, and ending the line now would dispatch
    # an event on a blank line the stream never sent.
    if text.endswith("\r"):
        held, text = "\r", text[:-1]
    else:
        held = ""
    pieces = _NEWLINE.split(text)
    return pieces[:-1], pieces[-1] + held


def _field(line: str) -> tuple[str, str]:
    """Split one line into its field name and value, per the spec's line handling."""
    name, colon, value = line.partition(":")
    if not colon:
        # "Process the field using the whole line as the field name, and the empty
        # string as the field value."
        return name, ""
    # "If value starts with a U+0020 SPACE character, remove it from value."
    return name, value.removeprefix(" ")


async def parse_events_with_directives(
    chunks: Stream[bytes],
    *,
    max_event_size: int | None = None,
) -> AsyncIterator[Received]:
    """
    Parse a byte stream into events *and* the directives that carry no event.

    The full parse. Take it when you act on where the stream says to resume and how
    long to wait first, which in practice means you are writing a reconnecting loop;
    `subscribe` in `without-http` is that loop, and this is what it consumes. Reach for
    `parse_events` when you only want what was delivered.

    A `Retry` is surfaced where its line was read, and a `Checkpoint` where a frame
    moved the resumption point without dispatching an event. Neither can be a field on
    `ReceivedEvent`, because the frames that carry them deliver no event to hang them
    on, and a consumer that missed them would reconnect to the wrong place.

    Decoding follows the spec exactly, which matters more than it sounds:

    - **UTF-8, with replacement.** Malformed bytes become U+FFFD rather than raising,
      because raising would hand a hostile producer a way to kill its consumers. One
      leading byte order mark is stripped, and only one.
    - **Chunk boundaries are invisible.** A multi-byte character or a CRLF pair split
      across two chunks parses as if it had arrived whole.
    - **Unknown fields are ignored**, as are comments and malformed values, which is
      what lets a producer add a field without breaking a consumer that predates it.
      Nothing in the format is a parse error; the only thing raised here is the size
      cap below, which is this library's policy rather than the format's.

    `max_event_size` caps the characters retained toward the event being assembled
    (its data, its type, and any partial line), raising `ValueError` past the bound.
    It is `None`, unbounded, by default: the bound is worth setting exactly when the
    producer might be hostile or merely broken, since a stream of `data:` lines that
    never sends the blank line dispatching them makes a conformant parser buffer until
    it dies. A cap on retained state rather than on bytes consumed is what lets a
    heartbeat run forever without tripping it: a comment is discarded as soon as its
    line ends and retains nothing.
    """
    decoder = codecs.getincrementaldecoder("utf-8-sig")(errors="replace")
    pending = ""
    data: list[str] = []
    event_type = ""
    last_id = ""
    # What the consumer has already been told the resumption point is, so a frame that
    # only moves it produces exactly one `Checkpoint` and an event that carries it
    # produces none.
    reported_id = ""
    buffered = 0

    def handle(line: str) -> Received | None:
        nonlocal data, event_type, last_id, reported_id, buffered
        if not line:
            # Dispatch. The spec sets the last event ID string here, *before* the
            # empty-data early return, which is why an id-only frame still moves the
            # resumption point. An empty data buffer delivers nothing but still clears
            # the frame, which is why a `: ping` heartbeat costs a consumer no memory.
            dispatched: Received | None = None
            if data:
                dispatched = ReceivedEvent(data="\n".join(data), type=event_type or DEFAULT_EVENT_TYPE, id=last_id)
                reported_id = last_id
            elif last_id != reported_id:
                dispatched = Checkpoint(id=last_id)
                reported_id = last_id
            data, event_type, buffered = [], "", 0
            return dispatched
        if line.startswith(":"):
            return None
        name, value = _field(line)
        match name:
            case "data":
                data.append(value)
                buffered += len(value)
            case "event":
                event_type = value
            case "id":
                # "If the field value does not contain U+0000 NULL, then set the last
                # event ID buffer to the field value. Otherwise, ignore the field."
                if "\x00" not in value:
                    last_id = value
            case "retry":
                # ASCII digits only: `str.isdigit` alone also accepts U+0663 and the
                # other decimal digits the format does not take.
                if value.isascii() and value.isdigit():
                    return Retry(after=timedelta(milliseconds=int(value)))
            case _:
                return None
        return None

    async for chunk in chunks:
        lines, pending = _split_lines(pending + decoder.decode(chunk))
        for line in lines:
            if (item := handle(line)) is not None:
                yield item
        if max_event_size is not None and buffered + len(event_type) + len(pending) > max_event_size:
            raise ValueError(f"the event stream exceeded max_event_size ({max_event_size})")

    # A carriage return held back as a possible CRLF half can only have been a
    # terminator once the stream is over, so the line it ended is complete and is
    # processed here. Whatever follows it is not: "if the file ends in the middle of an
    # event, before the final empty line, the incomplete event is not dispatched", so a
    # stream cut mid-event delivers nothing rather than a truncated value under framing
    # that would not say so, and a reconnecting consumer resumes from the last id it
    # *did* see.
    if pending.endswith("\r") and (item := handle(pending[:-1])) is not None:
        yield item


async def parse_events(
    chunks: Stream[bytes],
    *,
    max_event_size: int | None = None,
) -> AsyncIterator[ReceivedEvent]:
    """
    Parse a byte stream into the events it delivers, dropping directives.

    The common path, and the one to reach for unless you are deciding when and where to
    reconnect: `async for event in parse_events(response.body)`. See
    `parse_events_with_directives` for the decoding rules and for `max_event_size`, and
    `without-http`'s `subscribe` for a loop that reconnects and resumes on its own.
    """
    async for item in parse_events_with_directives(chunks, max_event_size=max_event_size):
        if isinstance(item, ReceivedEvent):
            yield item
