# The functional core of the KV server: parse a line into a request, fold a
# request into the immutable `Store`, render a reply to a line. Pure functions,
# no sockets, no asyncio, no I/O. The keyspace is threaded as a value (never a
# shared mutable place), so it is long-lived processor state in the without
# sense. The imperative shell that runs this core over a socket lives in `shell`.

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from without import Processor
from without import Transition
from without import from_scan


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
