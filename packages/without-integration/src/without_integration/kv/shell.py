# The imperative shell: a generic line-protocol TCP server, plus the wiring that
# runs the pure `core` over it. This is the demonstration that without is a
# principled way to write a shell. The transport (`Send`, `Connected`, `serve`)
# knows nothing of any protocol: it reads lines, hands each to a caller-supplied
# codec, funnels every connection into one inbox, and runs one consumer over the
# merged stream. Each event carries a `send` callable bound to its connection, so
# the consumer replies by calling `send` (contained I/O) rather than the server
# routing output: no connection ids, no reply registry. This is the shape ASGI
# uses (scope/receive/send). `make_responder` is the wiring: it lifts the core's
# pure `apply` into a fold over `Connected[Request, Reply]`, instantiating the
# generic `In`/`Out` at the KV protocol's `Request`/`Reply`.
#
# Finding worth recording: `without.merge` is a *static* N-to-1 fan-in (a fixed
# set of sources known up front). A server's set of connections is *dynamic*, so
# the fan-in here is a shared inbox queue (`stream_from_queue`) any newly accepted
# connection writes to, not `merge`. A dynamic-merge connector is a candidate
# addition to the core.

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass

from pydantic_settings import BaseSettings, SettingsConfigDict
from without import Context, Fold, from_fold, stream_from_queue

from without_integration.kv.core import EMPTY_STORE, Reply, Request, Store, apply

type Send[Out] = Callable[[Out], Awaitable[None]]


class ServeConfig(BaseSettings):
    """How the line server binds and shuts down, parsed from the environment.

    A `without` `Context` carries this into `serve`: the transport reads its
    knobs from `config.current()` rather than taking a fistful of keyword
    arguments, so the same server can be driven by env-backed config in
    production (`EnvContext.load(ServeConfig)`) or a fixed value in a test
    (`EnvContext(settings=ServeConfig(...))`) without changing its signature.
    """

    model_config = SettingsConfigDict(env_prefix="KV_")

    host: str = "127.0.0.1"
    port: int = 0
    max_pending: int = 100
    drain_timeout: float = 5.0


@dataclass(frozen=True, slots=True)
class Connected[In, Out]:
    """An inbound payload paired with the channel to reply on.

    `send` is bound to the connection the payload arrived on, so the reply
    target rides *in the value* rather than through a side registry keyed by a
    surrogate id. The consumer sends by calling it; nothing has to route outputs
    back. This is the per-event analogue of ASGI's `send`.
    """

    send: Send[Out]
    payload: In


@asynccontextmanager
async def serve[In, Out](
    consumer: Fold[Connected[In, Out], object],
    *,
    decode: Callable[[str], In],
    encode: Callable[[Out], str],
    config: Context[ServeConfig],
) -> AsyncIterator[asyncio.Server]:
    """Run a TCP line server, feeding every connection's requests to `consumer`.

    The transport, with no knowledge of any protocol. `decode` parses a received
    line into an input and `encode` renders an output into a line: both are the
    boundary, so `consumer` works in domain values and never touches bytes or
    sockets. Each connection gets a `send` closure (encode, then write) carried
    on its events, so `consumer` replies by calling `send` rather than the server
    routing output. `consumer` is a leaf (a fold or a sink); its result is
    ignored. Binding and shutdown knobs come from `config` (a `Context`); `port`
    0 lets the OS choose and the chosen address is on the yielded server's
    `sockets`.

    The shared `inbox` is bounded, so a slow consumer backpressures every
    connection's reader rather than buffering an unbounded backlog. `send` does
    not `drain`, so one slow client cannot stall the shared consumer (the
    transport buffers instead, the same trade the design accepts on the inbound
    side).

    A connection's writer is not closed when its reader hits EOF: replies are
    produced asynchronously by the shared consumer, so closing on EOF would race
    the reply for an already-enqueued request and silently drop it (the classic
    send-then-half-close client). Each connection instead tracks its in-flight
    requests and closes only once every reply it enqueued has been written.

    On exit the server drains gracefully, in order: it stops accepting new
    connections, then waits (bounded by `drain_timeout`) for the connections it
    already accepted to finish reading their buffered requests into the `inbox`
    and receive their replies. Stragglers still parked past the budget (an idle
    client that never disconnects) are then cut off, the `inbox` is shut down so
    the consumer finishes the queue and its stream ends, and a consumer that
    overruns the budget is cancelled. Every wait is bounded, so shutdown always
    terminates.
    """
    settings = config.current()
    inbox: asyncio.Queue[Connected[In, Out]] = asyncio.Queue(maxsize=settings.max_pending)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        inflight = 0  # requests enqueued from this connection whose reply has not yet been written
        quiesced = asyncio.Event()
        quiesced.set()

        async def send(value: Out) -> None:
            nonlocal inflight
            try:
                if not writer.is_closing():  # a reply to a departed client must not kill the shared consumer
                    writer.write(f"{encode(value)}\n".encode())
                    # deliberately no `await writer.drain()`: draining here would let one slow client stall the
                    # single shared consumer (head-of-line blocking across all clients); the transport buffers
                    # instead. `writer.close()` below still flushes the buffered bytes before closing.
            finally:
                inflight -= 1
                if inflight == 0:
                    quiesced.set()

        try:
            async for raw in reader:
                inflight += 1
                quiesced.clear()
                try:
                    await inbox.put(Connected(send=send, payload=decode(raw.decode().rstrip("\n"))))
                except asyncio.QueueShutDown:  # shutting down: stop reading, do not strand this request as in-flight
                    inflight -= 1
                    if inflight == 0:
                        quiesced.set()
                    break
        finally:
            # hold the writer open until the consumer has replied to everything this connection enqueued, but
            # never longer than the drain budget (a cancelled consumer leaves replies unsent, so do not hang).
            with suppress(TimeoutError):
                await asyncio.wait_for(quiesced.wait(), settings.drain_timeout)
            writer.close()

    connections: set[asyncio.Task[None]] = set()

    def spawn(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connection = asyncio.create_task(handle(reader, writer))
        connections.add(connection)
        connection.add_done_callback(connections.discard)

    async def drain_connections() -> None:
        if connections:
            await asyncio.wait(connections, timeout=settings.drain_timeout)

    async def consume_inbox() -> None:
        await consumer(stream_from_queue(inbox))

    consumer_task = asyncio.create_task(consume_inbox())
    server = await asyncio.start_server(spawn, host=settings.host, port=settings.port)
    try:
        yield server
    finally:
        server.close()  # stop accepting new connections; in-flight ones keep going
        await drain_connections()  # let accepted readers finish enqueuing requests and receiving replies
        server.close_clients()  # cut off stragglers (an idle reader parked with nothing left to send)
        inbox.shutdown()  # readers are done producing; let the consumer finish the queue, then its stream ends
        try:
            await asyncio.wait_for(consumer_task, settings.drain_timeout)
        except TimeoutError:
            pass  # wait_for cancelled the overrunning consumer; collect it below
        with suppress(asyncio.CancelledError):
            await consumer_task
        await drain_connections()  # let the just-cut-off handlers finish closing their writers
        await server.wait_closed()


def make_responder(initial: Store = EMPTY_STORE) -> Fold[Connected[Request, Reply], Store]:
    """Wire the pure core into the shell: fold each request into the keyspace, reply on its channel.

    The KV instantiation of the generic transport: it lifts `core.apply` into a
    fold over `Connected[Request, Reply]` (so `In`/`Out` become `Request`/`Reply`),
    threading the keyspace as `make_store` does but with a per-event effect of
    sending the reply on the event's own channel. The final `Store` is returned at
    end-of-stream and ignored by the server (whose source never ends); the point
    is the send, and the keyspace stays a threaded value.
    """

    async def respond(event: Connected[Request, Reply], store: Store) -> Store:
        transition = await apply(event.payload, store)
        await event.send(transition.output)
        return transition.state

    return from_fold(initial, respond)
