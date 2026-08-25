from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import timedelta
from typing import assert_never

from without_asgi import RawHeaders
from without_asgi.headers import first
from without_asgi.sse import EVENT_STREAM_MEDIA_TYPE
from without_asgi.sse import Checkpoint
from without_asgi.sse import ReceivedEvent
from without_asgi.sse import Retry
from without_asgi.sse import parse_events_with_directives

from without_http.client import ClientResponse
from without_http.client import ResponseHead
from without_http.timeouts import HTTPTimeout

# The half of Server-Sent Events that needs a transport. The format itself is two pure
# stream transforms in `without-asgi` (`without_asgi.sse`), which is why a handler can
# emit an event stream under uvicorn and a caller can parse one out of any
# `Stream[bytes]`. What lives here is the loop those two cannot express on their own:
# reconnecting a dropped stream and resuming it from the last id, which needs a client
# exchange to reconnect *with* — the `ClientResponse` it opens each attempt against, and
# the timeouts it treats as a stream that dropped rather than a fault.

__all__ = [
    "DEFAULT_RECONNECT",
    "MAXIMUM_RECONNECT",
    "MINIMUM_RECONNECT",
    "NotAnEventStream",
    "subscribe",
]

# How long to wait before reconnecting until a producer says otherwise with `retry:`.
# The spec leaves the initial value to the implementation, "probably in the region of a
# few seconds"; browsers land between two and five.
DEFAULT_RECONNECT = timedelta(seconds=3)

# The floor a producer's `retry:` is clamped to, so a hostile or broken `retry: 0`
# cannot spin a consumer into a hot reconnect loop.
MINIMUM_RECONNECT = timedelta(milliseconds=100)

# The ceiling, for the mirrored reason: a `retry: 8640000000000` that was meant to be
# `retry: 86400000` parks a consumer for centuries, and a subscription that is silent
# forever with nothing raised is harder to notice than one that reconnects too often.
# Well past any real backoff, which browsers spell in seconds.
MAXIMUM_RECONNECT = timedelta(minutes=5)


class NotAnEventStream(Exception):
    """
    The endpoint did not answer with a `200 text/event-stream` response.

    Terminal, never retried: the spec fails the connection on exactly these two
    conditions and does not reconnect afterwards, because an endpoint answering `404`
    or `text/html` is not a stream that dropped, it is one that was never there.
    """


async def _sleep(duration: timedelta) -> None:
    await asyncio.sleep(duration.total_seconds())


def _is_event_stream(head: ResponseHead) -> bool:
    content_type = first(head.headers, b"content-type")
    if content_type is None:
        return False
    return content_type.split(b";")[0].strip().lower() == EVENT_STREAM_MEDIA_TYPE


def subscribe(
    attempt: Callable[[RawHeaders], Awaitable[ClientResponse]],
    *,
    reconnect: timedelta = DEFAULT_RECONNECT,
    minimum_reconnect: timedelta = MINIMUM_RECONNECT,
    maximum_reconnect: timedelta = MAXIMUM_RECONNECT,
    max_event_size: int | None = None,
    sleep: Callable[[timedelta], Awaitable[None]] = _sleep,
) -> AsyncGenerator[ReceivedEvent]:
    """
    Consume an event stream, reconnecting and resuming when it drops.

    The composition the two halves of Server-Sent Events exist to be assembled into: it
    opens a connection, parses the response body with
    `without_asgi.sse.parse_events_with_directives`, and when the stream ends it waits
    and opens another one carrying `Last-Event-ID`, so the producer can resume where the
    consumer stopped. What a caller sees is one uninterrupted stream of events across
    however many connections it took.

    ```python
    async for event in subscribe(lambda headers: client(ClientRequest("GET", url, headers))):
        print(event.type, event.data)
    ```

    `attempt` opens one connection: given the headers this loop wants on the request, it
    answers with the response. A *function* rather than a `ClientRequest`, because a
    request is not replayable. Its body is a `Stream[bytes]`, which the interface allows
    to be iterated exactly once, and the bodies this package builds are one-shot async
    generators, so re-sending one request value would put a full body on the wire for
    the first attempt and an empty one on every attempt after it. Building the request
    inside `attempt` makes that unrepresentable rather than documented, and it is what
    lets an event stream ride a `POST` (the shape MCP's Streamable HTTP uses) instead of
    only the bodyless `GET` a reused request survives.

    The headers handed to `attempt` are `accept: text/event-stream` and, once the stream
    has a resumption point, `last-event-id`. Pass them through as above, or `merge` them
    with your own to decide which side wins on a name you also set.

    Descending a layer is the whole point of the split, and costs one line. A caller
    that wants exactly one connection, or its own reconnection policy, skips this and
    parses the body directly:

    ```python
    head, body = await client(request)
    async for event in parse_events(body):
        ...
    ```

    ## What it retries, and what it does not

    This is the only retry loop `without-http` ships, and the register's position
    against a `retry()` middleware is why it can be: that position rejects *policy*
    the library would have to invent (how many attempts, which statuses, what backoff),
    and here there is none to invent. The backoff arrives on the wire as `retry:`, the
    resumption token arrives as `id:`, and the terminal condition is written into the
    protocol. What the settings below decide is how far to trust the peer that supplies
    them.

    - A **non-`200` status or a content type other than `text/event-stream`** raises
      `NotAnEventStream` and never reconnects, per the spec's terminal failure.
    - The **first connection's errors propagate**. A caller that cannot reach the
      endpoint at all learns so immediately instead of watching a silent loop.
    - Once a stream has been established, a **connection error or timeout, on the
      stream or on any later attempt, reconnects** after the current wait. A stream
      that a proxy reaps every 60 seconds is the ordinary case, not the exceptional
      one, which is why the protocol has a resumption token at all.

    `reconnect` is the wait until the producer names one with `retry:`, after which its
    value is used, clamped to between `minimum_reconnect` (100ms) and
    `maximum_reconnect` (five minutes). Both ends guard the same thing, a `retry:` that
    is hostile or merely wrong: at zero it would spin a consumer into a hot reconnect
    loop, and a few orders of magnitude too large it would park one on a subscription
    that goes silent forever with nothing raised to notice. Widen either end for a
    producer you trust to name its own backoff, or narrow them to hold a peer to a
    window you chose. `max_event_size` is passed through to the parser. `sleep` is the
    delay, injected so a test drives the loop without waiting (and so a caller can add
    jitter).

    Reflecting a producer's value into a request header is safe here because the parser
    only ever hands on an id a header can carry unchanged: a carriage return or line
    feed is what ended the field, and an `id:` a field value could not spell, or would
    silently alter, is ignored rather than resumed from.

    An `AsyncGenerator` rather than a bare `AsyncIterator`, because this holds a live
    connection and a caller that stops early should be able to say so: `aclose()`
    releases it there and then, rather than at whenever the collector gets to it.
    """
    # An ordinary function wrapping the generator, so a contradictory window is refused
    # where the caller wrote it. The same guards inside the generator body would not run
    # until the first `anext`, which is after the first request is on the wire and may be
    # arbitrarily long after the mistake was made.
    if not timedelta() <= minimum_reconnect <= maximum_reconnect:
        raise ValueError(
            f"the reconnection window runs from zero upwards, not from {minimum_reconnect!r} to {maximum_reconnect!r}"
        )
    if reconnect < timedelta():
        raise ValueError(f"an initial reconnection wait cannot be negative: {reconnect!r}")
    return _subscribed(
        attempt,
        reconnect=reconnect,
        minimum_reconnect=minimum_reconnect,
        maximum_reconnect=maximum_reconnect,
        max_event_size=max_event_size,
        sleep=sleep,
    )


async def _subscribed(
    attempt: Callable[[RawHeaders], Awaitable[ClientResponse]],
    *,
    reconnect: timedelta,
    minimum_reconnect: timedelta,
    maximum_reconnect: timedelta,
    max_event_size: int | None,
    sleep: Callable[[timedelta], Awaitable[None]],
) -> AsyncGenerator[ReceivedEvent]:
    last_id = ""
    wait = reconnect
    established = False
    # What every attempt offers. `last-event-id` joins it once the stream has a
    # resumption point, and the caller decides how these meet the request's own headers.
    offered: RawHeaders = ((b"accept", EVENT_STREAM_MEDIA_TYPE),)

    while True:
        headers = (*offered, (b"last-event-id", last_id.encode())) if last_id else offered
        try:
            head, body = await attempt(headers)
        except OSError, HTTPTimeout:
            if not established:
                raise
            await sleep(wait)
            continue

        if head.status != 200 or not _is_event_stream(head):
            await body.aclose()
            raise NotAnEventStream(
                f"expected a 200 text/event-stream response, got {head.status} "
                f"{(first(head.headers, b'content-type') or b'with no content-type').decode('latin-1')}"
            )
        established = True

        try:
            async for item in parse_events_with_directives(body, max_event_size=max_event_size):
                match item:
                    case ReceivedEvent():
                        last_id = item.id
                        yield item
                    case Retry(after):
                        wait = min(max(after.duration, minimum_reconnect), maximum_reconnect)
                    case Checkpoint(moved_to):
                        # A frame that moved the resumption point without delivering an
                        # event. Missing these resumes from before whatever the producer
                        # skipped, which is the whole reason it sent one.
                        last_id = moved_to
                    case _ as unreachable:
                        assert_never(unreachable)
        except OSError, HTTPTimeout:
            # A dropped stream is the ordinary end of a long-lived connection, not a
            # fault to surface: that is what the resumption token is for.
            pass
        finally:
            await body.aclose()

        await sleep(wait)
