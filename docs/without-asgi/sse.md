# Server-Sent Events

Server-Sent Events is not a protocol in the way WebSockets is. There is no
handshake, no upgrade, and nothing new on the socket: an event stream is an
ordinary HTTP response whose body never ends, carrying a
[wire format](https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation)
under the media type `text/event-stream`. `curl` on an SSE endpoint prints
events as they arrive, because there is nothing else to it.

That is why the format lives here rather than beside a transport. Both halves
are pure: `encode_event` renders one event to bytes, and `parse_events` is
`Stream[bytes] -> Stream[ReceivedEvent]`. Neither touches a socket, so an app
under uvicorn emits events without `without-http` present, a caller parses them
out of any byte stream, and the pair tests as assertions over byte strings.
(`with_heartbeat`, below, is the one piece here that is not pure, because
noticing silence means reading a clock. It still needs no transport.)

This page covers the format, the two directions, and what to watch for in
deployment. The one piece that genuinely needs a transport, the loop that
reconnects a dropped stream and resumes it, is
[`subscribe`](../without-http/index.md#server-sent-events) in `without-http`.

## Four kinds of frame

An event stream is a sequence of frames, each ended by a blank line, and only
four of them mean anything. `ServerSentEvent` is the union of exactly those:

```python
type ServerSentEvent = Event | Comment | Retry | Checkpoint
```

| Frame | Wire | Effect on the peer |
|---|---|---|
| `Event(data, type="message", id=None)` | `event:`, `id:`, `data:` | dispatches an event |
| `Comment(text)` | `:text` | nothing; keeps the connection warm |
| `Retry(after)` | `retry: 30000` | sets the reconnection time |
| `Checkpoint(id)` | `id: 42` | advances the resumption point, delivering nothing |

A union rather than one type with five optional fields, because the rest of what
such a type would permit is meaningless. A frame with an `event:` and no `data:`
dispatches nothing and its type is discarded on arrival; a frame with neither is
a no-op. Splitting them costs nothing on the wire, since a frame carrying several
of these at once is equivalent to sending them one after another.

Only `Event` delivers anything, which is why `data` is required on it and
optional nowhere. `Event(data="")` is a real event carrying the empty string,
not an empty frame.

## Sending frames

A handler returns the `Outbound` event stream `event_stream` builds, the same
shape [`file_response`](index.md#streaming-a-file) produces and the same
`Reply` `without-web` already accepts:

```python
from without_streams import Milliseconds
from without_asgi import Checkpoint, Comment, Event, Retry, ServerSentEvent, event_stream


async def progress(job: Job) -> AsyncIterator[ServerSentEvent]:
    yield Retry(Milliseconds(30_000))
    async for step in job.steps():
        if step.filtered:
            yield Checkpoint(step.cursor)  # skipped, but do not replay it
        else:
            yield Event(data=json.dumps(step.as_payload()), type="step", id=step.cursor)
    yield Event(data="done", type="complete")


def route(state: State, scope: HttpScope) -> HttpHandler:
    return lambda inputs: event_stream(progress(state.job))
```

Each frame becomes one `ResponseBody` carrying `more_body=True`. That is the
contract, not an implementation detail: an event sitting in a buffer has not
been delivered, so the chunk boundary is where the flush happens.

`Event` is the outbound half of a pair, and `ReceivedEvent` is the parsed
counterpart: the same [inbound/outbound split](../without-http/index.md) as
`ResponseStart` against `ResponseHead`. Their fields line up except for one, and
that difference is worth stating because it is the only asymmetry in the pair:

- **`id` is `str | None` outbound and `str` inbound.** `None` sends no `id:`
  line, leaving the peer's resumption point where it was; `""` sends an empty
  one, which *clears* it. Only a sender gets that choice, since a parser only
  ever reports the point the stream currently sits on.
- **`type` is `str` on both**, defaulting to `"message"`. The format cannot tell
  an absent `event:` line from `event: message` or `event: `, so the encoder
  writes no line for the default and the distinction never reaches the wire.
  There is nothing for an optional to express.

`Retry` and `Checkpoint` are not split by direction at all: with one required
field each there is no default that could mask a parser bug, so one type serves
both.

### Heartbeats

A proxy, a load balancer, or a NAT table reaps an idle connection after 30 to 60
seconds, and the client learns about it only as a drop. `with_heartbeat` keeps
the connection warm by inserting a frame whenever the stream has been silent too
long:

```python
from without_asgi import event_stream, with_heartbeat

return event_stream(with_heartbeat(progress(job)))
```

It is an **idle timer, not a metronome**: the interval restarts on each frame
that goes out, so a busy stream sends no heartbeats at all and a silent one sends
exactly as many as it needs. Merging a fixed-rate `ticks` instead would spend a
frame every interval no matter how much real traffic there was.

The default beat is a bare `Comment()`, three bytes on the wire, because a
comment is the only frame a conformant consumer is guaranteed to ignore: it
dispatches no event and retains nothing in the peer's parser. Pass `beat=` to
send something else, such as a `Checkpoint` that doubles as a resumption point:

```python
with_heartbeat(events, every=timedelta(seconds=20), beat=Checkpoint(cursor))
```

This is the one piece of the module that reads a clock; the encoder and parser
stay pure. It pulls the source into a task so a lapsed interval leaves that pull
running, since bounding `anext` with a timeout would cancel the pull and lose
whatever the source was about to produce. Closing the returned generator cancels
the pull and closes the source, and `event_stream` closes what it iterates, so
the guarantee holds through the composition above: closing the response closes
the heartbeat wrapper, which closes `progress(job)`, there and then rather than
at whenever the collector gets to it.

## Receiving events

`parse_events` takes the byte stream a response body already is:

```python
from without_asgi import parse_events
from without_http import request

async with request(client, "GET", url) as (head, body):
    async for event in parse_events(body):
        print(event.type, event.data)
```

Decoding follows the spec, which matters more than it sounds. Bytes decode as
UTF-8 with replacement rather than raising, because raising would hand a hostile
producer a way to kill its consumers. Exactly one leading byte order mark is
stripped. Chunk boundaries are invisible, so a multi-byte character or a CRLF
pair split across two chunks parses as if it had arrived whole. Unknown fields,
comments, and malformed values are ignored, which is what lets a producer add a
field without breaking a consumer that predates it. Nothing in the format is a
parse error, so "malformed" reaches further than the spec's own list: a `retry:`
too large to name a duration is dropped rather than raised, on the same grounds
as the replacement decoding above.

`parse_events` yields only `ReceivedEvent`, because that is all most consumers
want. `parse_events_with_directives` yields the fuller `Received` union, adding
the two frames that change a client's state without delivering anything:

```python
type Received = ReceivedEvent | Retry | Checkpoint
```

`Comment` is absent, which looks asymmetric against `ServerSentEvent` and is
not: a heartbeat is meaningful to send, and the spec says to ignore it on
receipt. The split between the two functions is the same one
[`ResponseBody`'s trailers](../without-http/index.md#trailers) already has, so
the common path pays nothing for a feature it does not use.

Three details of the format surprise people, and all three are load-bearing:

- **The last event id persists across events, and across connections.** An event
  whose own frame carried no `id:` still reports whichever id the stream last
  set, and on a reconnect that starts as the id the connection resumed from: both
  parsers take a `last_event_id` to seed it, which
  [`subscribe`](../without-http/index.md#server-sent-events) passes for you. A
  parser starting from empty would report `""` for the first id-less event after
  a drop, and a consumer storing that as its resumption point would replay the
  feed from the beginning.
- **`retry:` is a property of the stream, not of an event.** A frame carrying
  only `retry:` and a blank line dispatches nothing, so a reconnection time hung
  on the next event would be dropped whenever a producer sent one on its own.
- **An `id:` with no data still moves the resumption point.** The spec's dispatch
  sets the last event ID string *before* it returns early on an empty data
  buffer. That is what a producer sends after skipping work you asked not to see,
  and a consumer that ignores it replays from before the skip. Neither this nor
  `retry:` can be a field on `ReceivedEvent`, because the frames carrying them
  deliver no event to hang them on.

## Values that cannot be spelled

A newline in `data` is carried, by splitting the value across as many `data:`
lines as it needs and letting the peer rejoin them. This is what makes event
injection structurally impossible, and it is what a hand-rolled
`f"data: {payload}\n\n"` gets wrong: a payload containing
`\n\nevent: admin\ndata: escalate` forges a whole second event there, and
arrives here as the literal text it is.

The break survives; *which* break does not. The format spells all three
terminators the same way and the peer rejoins with a line feed, so a `\r\n` or a
lone `\r` inside `data` arrives as `\n`. Normalize before sending if the
distinction matters, or put a format that can carry it inside `data`.

A newline in `type` or `id` has no spelling at all: it would end the field and
let the value forge another one. So it raises in the constructor of the frame
that carries it (`Event`, or `Checkpoint` for an id) rather than at encode time,
which puts the failure where the frame is built instead of mid-stream after the
response head is already committed.

An `id` is held to a stricter rule than the rest of the format, for a quieter
reason: it comes back on reconnect as a `Last-Event-ID` header, so what it has to
survive is a *field value*, not just a line.

- **A control character** is not a legal field value at all, so an id carrying
  one is a resumption point no reconnect can name.
- **A leading or trailing space** is legal but not preserved: a field parser
  strips the whitespace around a value, so the peer would answer from `abc` where
  the consumer meant `abc `. Interior spaces survive intact and are left alone.

The parser is opinionated to match, ignoring an `id:` that breaks either rule
where the spec ignores only a `NUL`. Being strict on both sides is the point,
since only one of them is ever this library: an id we refuse to send cannot break
someone else's consumer, and an id we refuse to accept cannot break a reconnect
of ours. Ignoring rather than raising on the way in costs a replay from an older
id, where reconnecting on a changed one resumes from a point neither side chose
and reconnecting on an unspellable one ends the subscription outright.

`Retry` is bounded on both sides for the same reason. The parser drops a `retry:`
value too long to name a duration, so `Retry` refuses to construct one this
library's own parser would drop: a frame that encodes to bytes we would not read
is not one worth writing.

The line carries whole milliseconds, so `after` is a count of
[`Milliseconds`](../without/index.md#durations-that-cross-an-integer-boundary-withoutdurations)
rather than a `timedelta`, and a finer duration is not something a `Retry` can be
built from at all. Truncating one is at its worst at the bottom of the range:
half a millisecond renders `retry: 0`, which does not mean "almost no wait" but
"reconnect immediately". `subscribe` clamps that to `MINIMUM_RECONNECT`, so this
library's own consumer cannot be made to hot loop, but a browser `EventSource`
takes it at face value. `Retry(Milliseconds(0))` stays legal, because zero is a
wait a sender chose rather than one that fell out of a conversion.

## What to watch in deployment

**Buffering is the way this breaks, and it breaks silently.** nginx buffers a
proxied response into 32 KB lumps by default, which turns a real-time stream
into one delivery at the end. `event_stream` does not set
`x-accel-buffering: no` for you, because that is one proxy vendor's deployment
policy and this layer does not hold deployment policy (the same reason no access
log ships). Pass it where nginx is in front:

```python
event_stream(events, headers=((b"x-accel-buffering", b"no"),))
```

The header is necessary and not sufficient. Anything in the chain that wants to
see a whole body will re-buffer the stream: a CDN, an ETag middleware that has
to hash the response, a `proxy_http_version 1.0` left at the default. Verify
against the real chain rather than trusting the header.

**`connection: keep-alive` is not set, deliberately**, despite most SSE advice
recommending it. It is already the HTTP/1.1 default, and it is a forbidden
header in HTTP/2 and HTTP/3.

**Compression is declined for this media type.** An event stream is the one
response held open for as long as the client stays, so every event on it would
be encoded against a window holding every event before it, and an attacker who
can inject one event reads the length of the next. See
[what gets compressed](index.md#what-gets-compressed).

**A long-lived stream occupies a slot for its whole life.** Each one holds a
connection, a task, and a generator until the client leaves, so
`limit_concurrent_requests` counts streams that never finish: enough of them
permanently fill the cap and starve ordinary requests.
Give an SSE route its own bound rather than sharing the global one. Related, a
browser allows six connections per origin under HTTP/1.1, which HTTP/2 fixes.

**`Last-Event-ID` is attacker-controlled request input.** It arrives as a header
and feeds whatever resumes the stream, an offset, a cursor, a query. Parse it at
the boundary like any other input.

**An SSE endpoint is a cookie-bearing `GET`.** `EventSource` with
`withCredentials` attaches cookies, and while CORS stops an attacker *reading*
the response cross-origin, it does not stop the request happening. Keep the
endpoint free of side effects on connect, or require a credential a browser will
not attach on its own.

**The parser buffers without bound by default.** A producer that sends `data:`
lines and never the blank line that dispatches them makes a conformant parser
buffer until it dies. `max_event_size` caps the characters retained toward the
event being assembled, raising `ValueError` past the bound. It is off by
default, because whether it is worth setting is a question about the producer:
set it when the far side might be hostile or merely broken, and leave it off for
one you operate. It counts retained state rather than bytes consumed, which is
why a heartbeat can run forever without tripping it.
