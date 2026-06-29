# The imperative shell: a generic line-protocol TCP server, plus the wiring that
# runs the pure `core` over it. This is the demonstration that without is a
# principled way to write a shell, and it turns on one move: each connection is
# its own `Processor` over its own line stream, so a connection's lifecycle *is* a
# stream's lifecycle. The line stream ends when the client hangs up (EOF), the
# per-connection processor's `async for` runs dry and returns, and the writer
# closes. No in-flight counter, no "is this connection done" bookkeeping: the
# stream's end is the signal.
#
# Two kinds of state live here, and where each lives is the whole lesson:
#   - Shared state (the keyspace) lives in ONE serial `from_fold` (`make_keyspace`)
#     that every connection funnels into through a bounded `inbox`. A `from_fold`
#     pulls its next event only after the current step's coroutine completes, so
#     the store's read-modify-write never interleaves even across `await`s: the
#     sequential `async for` *is* the mutual exclusion, and the store stays a
#     threaded value, never a shared place behind a lock.
#   - Connection-scoped state (a per-connection request counter) is threaded in
#     that connection's own `from_scan` (`make_session`). It is safe to keep local
#     precisely because it is unshared, so connections run fully concurrently.
# The rule: thread state *down* only when it is scoped to that level; funnel *up*
# to a singular fold for anything shared. A connection reaches the shared core
# through `ask` (put a request on the `inbox`, await the reply on this
# connection's own channel), which is contained I/O, the per-event analogue of
# ASGI's receive/send.
#
# Finding worth recording: a server's set of connections is *dynamic* (it grows
# as clients arrive), so the fan-in here is a shared inbox queue
# (`stream_from_queue`) any newly accepted connection writes to. A static,
# fixed-set N-to-1 fan-in connector would not cover the dynamic case; a
# dynamic-merge connector is a candidate addition to the core.

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass

from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from without import Context
from without import Fold
from without import Processor
from without import Transition
from without import from_fold
from without import from_scan
from without import stream_from_queue

from integration.kv.core import EMPTY_STORE
from integration.kv.core import Reply
from integration.kv.core import Request
from integration.kv.core import Store
from integration.kv.core import apply
from integration.kv.core import encode_reply
from integration.kv.core import parse_request

type Send[Out] = Callable[[Out], Awaitable[None]]
type Ask[In, Out] = Callable[[In], Awaitable[Out]]
type MakeSession[In, Out] = Callable[[Ask[In, Out]], Processor[str, str]]


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
    idle_timeout: float | None = None  # reap a connection silent this long; None disables it


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
    make_session: MakeSession[In, Out],
    *,
    config: Context[ServeConfig],
) -> AsyncIterator[asyncio.Server]:
    """Run a TCP line server: a shared serial `consumer` plus a `make_session` per connection.

    The two arguments are a value and a maker, and that asymmetry is the point:
    `consumer` is the single serial owner of any shared state (the keyspace fold),
    built once, into which every connection funnels its requests on one bounded
    `inbox`; `make_session` is called *per connection* and, given that
    connection's `ask` (the round trip to the consumer), builds the
    `Processor[str, str]` mapping its inbound lines to outbound lines, owning the
    protocol codec plus any connection-scoped state. The transport itself touches
    only bytes and sockets. Binding and shutdown knobs come from `config` (a
    `Context`); `port` 0 lets the OS choose and the chosen address is on the
    yielded server's `sockets`.

    Each connection runs its session over its own line stream, so the connection
    ends naturally when the client hangs up: EOF ends the stream, the session's
    processor returns, the writer closes. A connection silent longer than
    `idle_timeout` is reaped the same way (its read times out, ending the stream),
    so an idle or slow-loris client cannot hold a session open forever; `None`
    disables it. Requests on one connection are sequential (await the reply before
    the next line), which is correct for an ordered line protocol and means a slow
    client stalls only its own session, never the shared consumer, so
    `await writer.drain()` is free of head-of-line risk. The `inbox` is bounded,
    so a backed-up consumer backpressures the connections rather than buffering
    without limit.

    On exit the server drains within a single `drain_timeout` budget. It stops
    accepting new connections, then, inside one `asyncio.timeout`, waits for
    accepted sessions to finish on their own (clients hanging up, the consumer
    answering their asks) and for the consumer to drain the `inbox`. If that whole
    graceful phase overruns the one budget, the server hard-stops: force-close
    every remaining client, cancel the consumer, and cancel any session still
    parked (e.g. awaiting a reply a stopped consumer will never send). Either way
    it then waits for the transports to close. The budget is global (one timeout
    around the graceful phase, not one per step), so shutdown always terminates in
    roughly `drain_timeout` without leaking a task.
    """
    settings = config.current()
    inbox: asyncio.Queue[Connected[In, Out]] = asyncio.Queue(maxsize=settings.max_pending)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        replies: asyncio.Queue[Out] = asyncio.Queue(maxsize=1)  # one reply in flight: this connection is sequential

        async def ask(request: In) -> Out:
            await inbox.put(Connected(send=replies.put, payload=request))
            return await replies.get()

        async def input_lines() -> AsyncIterator[str]:
            while True:
                try:
                    raw = await asyncio.wait_for(reader.readline(), settings.idle_timeout)
                except TimeoutError:
                    return  # idle past idle_timeout: end the stream, which returns the session and closes the writer
                if not raw:
                    return  # EOF: the client hung up
                yield raw.decode().rstrip("\n")

        try:
            async for output_line in make_session(ask)(input_lines()):
                writer.write(f"{output_line}\n".encode())
                await writer.drain()
        except ConnectionError:  # client reset the connection (read or write); this session is simply over
            pass
        finally:
            writer.close()

    connections: set[asyncio.Task[None]] = set()

    def spawn(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        connection = asyncio.create_task(handle(reader, writer))
        connections.add(connection)
        connection.add_done_callback(connections.discard)

    async def consume_inbox() -> None:
        await consumer(stream_from_queue(inbox))

    consumer_task = asyncio.create_task(consume_inbox())
    server = await asyncio.start_server(spawn, host=settings.host, port=settings.port)
    try:
        yield server
    finally:
        server.close()  # stop accepting new connections; in-flight ones keep going
        try:
            async with asyncio.timeout(settings.drain_timeout):  # ONE global budget for the whole graceful drain
                if connections:
                    await asyncio.wait(connections)  # each session finishes: its client hangs up, the consumer answers
                inbox.shutdown()  # sessions all done, so no more asks; the consumer drains the rest and returns
                await asyncio.wait([consumer_task])
        except TimeoutError:
            pass  # budget spent; hard-stop whatever is still running below
        server.close_clients()  # force-close any client still connected
        consumer_task.cancel()  # no-op if the consumer already returned
        stragglers = list(connections)  # any session still parked (e.g. awaiting a reply the consumer will not send)
        for connection in stragglers:
            connection.cancel()
        await asyncio.gather(consumer_task, *stragglers, return_exceptions=True)  # let the cancellations settle
        await server.wait_closed()  # blocks until the listening sockets and every client connection (writer) close


def make_keyspace(initial: Store = EMPTY_STORE) -> Fold[Connected[Request, Reply], Store]:
    """The shared serial state owner: fold each request into the keyspace, reply on its channel.

    Every connection funnels here. A `from_fold` consumes the merged request
    stream one event at a time, threading the keyspace as a value and answering
    each request on the channel it arrived with. Because the fold pulls its next
    event only after the current step completes, the store's read-modify-write is
    serialized without a lock even though `apply` may `await`. The final `Store`
    at end-of-stream is ignored; the point is the threaded value and the reply.
    """

    async def respond(event: Connected[Request, Reply], store: Store) -> Store:
        transition = await apply(event.payload, store)
        await event.send(transition.output)
        return transition.state

    return from_fold(initial, respond)


def make_session(ask: Ask[Request, Reply]) -> Processor[str, str]:
    """Build one connection's processor: thread a request counter, funnel to the shared keyspace.

    The connection-scoped half, dual to `make_keyspace`'s shared half. Called per
    connection with that connection's `ask`, it owns the protocol codec (parse
    at the inbound boundary, encode at the outbound one) and threads a per-connection
    request counter with `from_scan`. The counter is safe to keep local precisely
    because it is unshared: each connection numbers its own replies from 1 while the
    keyspace stays common to all. Each reply line is thus a function of both kinds of
    state, the connection-local number and the shared-store answer, reached via
    `ask` (contained I/O to the serial keyspace fold).
    """

    async def step(line: str, count: int) -> Transition[int, str]:
        number = count + 1
        reply = await ask(parse_request(line))
        return Transition(state=number, output=f"{number} {encode_reply(reply)}")

    return from_scan(0, step)
