# A toy line-protocol key-value server (Redis-ish) built on without, to validate
# that the contract supports long-lived processor state and request/response.
#
# Two halves, kept apart:
#   - The functional core: parse a line into a request, fold a request into the
#     keyspace, render a reply to a line. No sockets, no asyncio, no I/O. The
#     keyspace is threaded as an immutable value (never a shared mutable place),
#     so it is long-lived processor state in the without sense.
#   - The imperative shell: an asyncio TCP server. Every connection funnels its
#     decoded requests into one inbox, so a single fold threads one keyspace
#     across every client. Each event carries a `send` callable bound to the
#     connection it arrived on, and the fold replies by calling `send`
#     (contained I/O); outputs need no routing back, so there are no connection
#     ids and no reply registry. This is the shape ASGI uses (scope/receive/send):
#     the output channel is passed *down into* the consumer rather than returned
#     to a dispatcher.
#
# Finding worth recording: `without.merge` is a *static* N-to-1 fan-in (a fixed
# set of sources known up front). A server's set of connections is *dynamic*, so
# the fan-in here is a shared inbox queue (`stream_from_queue`) any newly accepted
# connection can write to, not `merge`. A dynamic-merge connector is a candidate
# addition to the core.

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass

from without import Fold, Processor, Transition, from_fold, from_scan, stream_from_queue
from without.tasks import background_task


@dataclass(frozen=True, slots=True)
class Get:
    key: str


@dataclass(frozen=True, slots=True)
class Set:
    key: str
    value: str


@dataclass(frozen=True, slots=True)
class Delete:
    key: str


@dataclass(frozen=True, slots=True)
class Malformed:
    """A line the boundary could not read as a command.

    Garbage from a client is a real protocol event, not an illegal internal
    state, so the parser names it as a closed variant rather than raising. The
    step turns it into an error reply; the server keeps serving.
    """

    line: str
    reason: str


type Command = Get | Set | Delete
type Request = Command | Malformed


def parse_request(line: str) -> Request:
    """Classify one protocol line into the closed set of things a client can send.

    A total function at the boundary (parse, don't validate): every line maps to
    a `Request`, and an unreadable one becomes `Malformed` rather than raising,
    so downstream code only ever handles known variants.
    """
    parts = line.split()
    if not parts:
        return Malformed(line=line, reason="empty command")

    name = parts[0].upper()
    arguments = parts[1:]
    if name == "GET":
        if len(arguments) != 1:
            return Malformed(line=line, reason="GET takes exactly one key")
        return Get(key=arguments[0])
    if name == "SET":
        if len(arguments) < 2:
            return Malformed(line=line, reason="SET takes a key and a value")
        return Set(key=arguments[0], value=" ".join(arguments[1:]))
    if name == "DEL":
        if len(arguments) != 1:
            return Malformed(line=line, reason="DEL takes exactly one key")
        return Delete(key=arguments[0])
    return Malformed(line=line, reason=f"unknown command {parts[0]!r}")


@dataclass(frozen=True, slots=True)
class Value:
    value: str


@dataclass(frozen=True, slots=True)
class Nil:
    """The reply to a `Get` for a key that is not present."""


@dataclass(frozen=True, slots=True)
class Stored:
    """The reply to a successful `Set`."""


@dataclass(frozen=True, slots=True)
class Deleted:
    existed: bool


@dataclass(frozen=True, slots=True)
class Error:
    message: str


type Reply = Value | Nil | Stored | Deleted | Error


def encode_reply(reply: Reply) -> str:
    """Render a reply as one protocol line (the dual of `parse_request`)."""
    match reply:
        case Value(value):
            return value
        case Nil():
            return "(nil)"
        case Stored():
            return "OK"
        case Deleted(existed):
            return "1" if existed else "0"
        case Error(message):
            return f"ERR {message}"


@dataclass(frozen=True, slots=True)
class Store:
    """An immutable snapshot of the keyspace.

    Mutating operations return a *new* `Store`: the keyspace is a value the
    step threads from one request to the next, not a place callers share and
    write through. That is what makes it safe as long-lived processor state.
    """

    entries: Mapping[str, str]

    def with_entry(self, key: str, value: str) -> Store:
        return Store(entries={**self.entries, key: value})

    def without_entry(self, key: str) -> Store:
        return Store(entries={existing: value for existing, value in self.entries.items() if existing != key})


EMPTY_STORE = Store(entries={})


async def apply(request: Request, store: Store) -> Transition[Store, Reply]:
    """Fold one request into the keyspace, emitting its reply.

    The step kernel. It does no I/O, so it is an `async def` that never
    awaits: a step that happens to be pure is just the degenerate case of one
    that may await contained I/O.
    """
    match request:
        case Get(key):
            held = store.entries.get(key)
            return Transition(state=store, output=Nil() if held is None else Value(held))
        case Set(key, value):
            return Transition(state=store.with_entry(key, value), output=Stored())
        case Delete(key):
            return Transition(state=store.without_entry(key), output=Deleted(existed=key in store.entries))
        case Malformed(_, reason):
            return Transition(state=store, output=Error(reason))


def make_store(initial: Store = EMPTY_STORE) -> Processor[Request, Reply]:
    """The keyspace as a processor: a stream of requests in, a stream of replies out."""
    return from_scan(initial, apply)


type Send[Out] = Callable[[Out], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class Connected[In, Out]:
    """An inbound payload paired with the channel to reply on.

    ``send`` is bound to the connection the payload arrived on, so the reply
    target rides *in the value* rather than through a side registry keyed by a
    surrogate id. The fold sends by calling it; nothing has to route outputs
    back. This is the per-event analogue of ASGI's ``send``.
    """

    send: Send[Out]
    payload: In


def make_responder(initial: Store = EMPTY_STORE) -> Fold[Connected[Request, Reply], Store]:
    """The keyspace as a connection-aware leaf: fold each request into the store, reply on its channel.

    A thin shell over the pure ``apply``: it threads the keyspace exactly as
    ``make_store`` does, but as a fold whose per-event effect is sending the
    reply on the event's own channel. The final ``Store`` is returned at
    end-of-stream and ignored by the server (whose source never ends); the point
    is the send, and the keyspace stays a threaded value rather than a place.
    """

    async def respond(event: Connected[Request, Reply], store: Store) -> Store:
        transition = await apply(event.payload, store)
        await event.send(transition.output)
        return transition.state

    return from_fold(initial, respond)


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

    The transport. ``decode`` parses a received line into an input and ``encode``
    renders an output into a line: both are the boundary, so ``consumer`` works
    in domain values and never touches bytes or sockets. Each connection gets a
    ``send`` closure (encode, then write) carried on its events, so ``consumer``
    replies by calling ``send`` rather than the server routing output.
    ``consumer`` is a leaf (a fold or a sink); its result is ignored. ``port`` 0
    lets the OS choose; the address is on the yielded server's ``sockets``.

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

        try:
            async for raw in reader:
                await inbox.put(Connected(send=send, payload=decode(raw.decode().rstrip("\n"))))
        finally:
            writer.close()

    async with background_task(run()):
        server = await asyncio.start_server(handle, host=host, port=port)
        async with server:
            yield server
