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
from contextlib import asynccontextmanager
from dataclasses import dataclass

from without import Fold, from_fold, stream_from_queue
from without.tasks import background_task

from without_integration.kv.core import EMPTY_STORE, Reply, Request, Store, apply

type Send[Out] = Callable[[Out], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class Connected[In, Out]:
    """An inbound payload paired with the channel to reply on.

    ``send`` is bound to the connection the payload arrived on, so the reply
    target rides *in the value* rather than through a side registry keyed by a
    surrogate id. The consumer sends by calling it; nothing has to route outputs
    back. This is the per-event analogue of ASGI's ``send``.
    """

    send: Send[Out]
    payload: In


@asynccontextmanager
async def serve[In, Out](
    consumer: Fold[Connected[In, Out], object],
    *,
    decode: Callable[[str], In],
    encode: Callable[[Out], str],
    host: str = "127.0.0.1",
    port: int = 0,
    max_pending: int = 100,
) -> AsyncIterator[asyncio.Server]:
    """Run a TCP line server, feeding every connection's requests to ``consumer``.

    The transport, with no knowledge of any protocol. ``decode`` parses a
    received line into an input and ``encode`` renders an output into a line:
    both are the boundary, so ``consumer`` works in domain values and never
    touches bytes or sockets. Each connection gets a ``send`` closure (encode,
    then write) carried on its events, so ``consumer`` replies by calling
    ``send`` rather than the server routing output. ``consumer`` is a leaf (a
    fold or a sink); its result is ignored. ``port`` 0 lets the OS choose; the
    address is on the yielded server's ``sockets``.

    The shared ``inbox`` is bounded, so a slow consumer backpressures every
    connection's reader rather than buffering an unbounded backlog. ``send`` does
    not ``drain``, so one slow client cannot stall the shared consumer (the
    transport buffers instead, the same trade the design accepts on the inbound
    side). Graceful draining of in-flight connections on shutdown is out of scope
    for this toy.
    """
    inbox: asyncio.Queue[Connected[In, Out]] = asyncio.Queue(maxsize=max_pending)

    async def run() -> None:
        await consumer(stream_from_queue(inbox))

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        async def send(value: Out) -> None:
            if not writer.is_closing():  # a reply to a departed client must not kill the shared consumer
                writer.write(f"{encode(value)}\n".encode())
                # deliberately no `await writer.drain()`: draining here would let one slow client stall the
                # single shared consumer (head-of-line blocking across all clients); the transport buffers instead.

        try:
            async for raw in reader:
                await inbox.put(Connected(send=send, payload=decode(raw.decode().rstrip("\n"))))
        finally:
            writer.close()

    async with background_task(run()):
        server = await asyncio.start_server(handle, host=host, port=port)
        async with server:
            yield server


def make_responder(initial: Store = EMPTY_STORE) -> Fold[Connected[Request, Reply], Store]:
    """Wire the pure core into the shell: fold each request into the keyspace, reply on its channel.

    The KV instantiation of the generic transport: it lifts ``core.apply`` into a
    fold over ``Connected[Request, Reply]`` (so ``In``/``Out`` become
    ``Request``/``Reply``), threading the keyspace as ``make_store`` does but with
    a per-event effect of sending the reply on the event's own channel. The final
    ``Store`` is returned at end-of-stream and ignored by the server (whose source
    never ends); the point is the send, and the keyspace stays a threaded value.
    """

    async def respond(event: Connected[Request, Reply], store: Store) -> Store:
        transition = await apply(event.payload, store)
        await event.send(transition.output)
        return transition.state

    return from_fold(initial, respond)
