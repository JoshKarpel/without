from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from typing import assert_never

from without_asgi.headers import first
from without_asgi.headers import merge
from without_asgi.headers import replace as replace_header
from without_asgi.sse import EVENT_STREAM_MEDIA_TYPE
from without_asgi.sse import Checkpoint
from without_asgi.sse import ReceivedEvent
from without_asgi.sse import Retry
from without_asgi.sse import parse_events_with_directives

from without_http.client import Client
from without_http.client import ClientRequest
from without_http.client import ResponseHead
from without_http.timeouts import HTTPTimeout

# The half of Server-Sent Events that needs a transport. The format itself is two pure
# stream transforms in `without-asgi` (`without_asgi.sse`), which is why a handler can
# emit an event stream under uvicorn and a caller can parse one out of any
# `Stream[bytes]`. What lives here is the loop those two cannot express on their own:
# reconnecting a dropped stream and resuming it from the last id, which needs a `Client`
# to reconnect *with*.

__all__ = [
    "DEFAULT_RECONNECT",
    "NotAnEventStream",
    "subscribe",
]

# How long to wait before reconnecting until a producer says otherwise with `retry:`.
# The spec leaves the initial value to the implementation, "probably in the region of a
# few seconds"; browsers land between two and five.
DEFAULT_RECONNECT = timedelta(seconds=3)

# The floor a producer's `retry:` is clamped to, so a hostile or broken `retry: 0`
# cannot spin a consumer into a hot reconnect loop.
_MINIMUM_RECONNECT = timedelta(milliseconds=100)


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


async def subscribe(
    client: Client,
    request: ClientRequest,
    *,
    reconnect: timedelta = DEFAULT_RECONNECT,
    max_event_size: int | None = None,
    sleep: Callable[[timedelta], Awaitable[None]] = _sleep,
) -> AsyncGenerator[ReceivedEvent]:
    """
    Consume an event stream, reconnecting and resuming when it drops.

    The composition the two halves of Server-Sent Events exist to be assembled into: it
    sends `request` through `client`, parses the response body with
    `without_asgi.sse.events_with_reconnects`, and when the stream ends it waits and
    sends the request again carrying `Last-Event-ID`, so the producer can resume where
    the consumer stopped. What a caller sees is one uninterrupted stream of events
    across however many connections it took.

    ```python
    async for event in subscribe(client, ClientRequest("GET", "https://api.test/feed")):
        print(event.type, event.data)
    ```

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
    protocol. Nothing is left for a flag to configure, so nothing grows one.

    - A **non-`200` status or a content type other than `text/event-stream`** raises
      `NotAnEventStream` and never reconnects, per the spec's terminal failure.
    - The **first connection's errors propagate**. A caller that cannot reach the
      endpoint at all learns so immediately instead of watching a silent loop.
    - Once a stream has been established, a **connection error or timeout, on the
      stream or on any later attempt, reconnects** after the current wait. A stream
      that a proxy reaps every 60 seconds is the ordinary case, not the exceptional
      one, which is why the protocol has a resumption token at all.

    `reconnect` is the wait until the producer names one with `retry:`, after which its
    value is used, floored at 100ms so a hostile or broken `retry: 0` cannot spin a
    consumer into a hot reconnect loop. `max_event_size` is passed through to the
    parser. `sleep` is the delay, injected so a test drives the loop without waiting
    (and so a caller can add jitter).

    The last id seen is reflected into the `Last-Event-ID` header of every later
    request, and `accept: text/event-stream` is set unless `request` already carries
    one. Reflecting a producer's value into a request header is safe here because a
    parsed `id` cannot contain a carriage return or line feed: those are what ended
    the field.

    An `AsyncGenerator` rather than a bare `AsyncIterator`, because this holds a live
    connection and a caller that stops early should be able to say so: `aclose()`
    releases it there and then, rather than at whenever the collector gets to it.
    """
    last_id = ""
    wait = reconnect
    established = False

    while True:
        # The caller wins on `accept`, so a producer that wants a narrower type stated
        # gets it; `last-event-id` is ours to set, and replaces whatever a previous
        # attempt left behind.
        headers = merge(((b"accept", EVENT_STREAM_MEDIA_TYPE),), request.headers)
        if last_id:
            headers = replace_header(headers, b"last-event-id", last_id.encode())
        attempt = replace(request, headers=headers)
        try:
            head, body = await client(attempt)
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
                        wait = max(after, _MINIMUM_RECONNECT)
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
