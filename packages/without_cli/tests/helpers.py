from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import field

from without_cli import INT
from without_cli import STR
from without_cli import Arm
from without_cli import Streams
from without_cli import argument
from without_cli import command
from without_cli import count
from without_cli import default
from without_cli import flag
from without_cli import group
from without_cli import many
from without_cli import once
from without_cli import option
from without_cli import optional


@dataclass(frozen=True, slots=True)
class Session(Streams):
    """
    State built from the root's options, carrying the streams on by *being* a
    `Streams`, which is one of the two ways to thread them down a path.
    """

    endpoint: str
    verbosity: int
    events: list[str] = field(default_factory=list)

    @classmethod
    def opened(cls, streams: Streams, endpoint: str, verbosity: int) -> Session:
        """Assemble from the parent state, naming every field that crosses."""
        return cls(
            stdin=streams.stdin,
            stdout=streams.stdout,
            stderr=streams.stderr,
            endpoint=endpoint,
            verbosity=verbosity,
        )


@dataclass(frozen=True, slots=True)
class Database:
    """The other way: hold the parent rather than extend it, and reach through it."""

    session: Session
    dsn: str


@asynccontextmanager
async def session(streams: Streams, verbosity: int, endpoint: str) -> AsyncIterator[Session]:
    built = Session.opened(streams, endpoint=endpoint, verbosity=verbosity)
    built.events.append("opened")
    try:
        yield built
    finally:
        built.events.append("closed")
        built.stderr.write("session closed\n")


@asynccontextmanager
async def database(parent: Session, dsn: str) -> AsyncIterator[Database]:
    parent.events.append(f"db opened {dsn}")
    try:
        yield Database(session=parent, dsn=dsn)
    finally:
        parent.events.append("db closed")


async def unreached(state: object, *values: object) -> int:
    """
    A well-formed handler for a command a test builds but never runs.

    A test asserting on a rejection or on a description still needs a command to
    assert *about*, and the whole point of those tests is that the handler is not
    reached. Raising rather than returning a code means a test that accidentally
    does run it fails loudly instead of passing for the wrong reason.
    """
    raise AssertionError("a command that should not have run was invoked")  # pragma: no cover - see above


@command("show", summary="Show the endpoint.")
async def show(state: Session) -> int:
    state.stdout.write(f"{state.endpoint} v{state.verbosity} {state.events}\n")
    return 0


@command(
    "add",
    argument("text", once(STR), summary="What to do."),
    option(("-t", "--tag"), many(STR), summary="Repeatable."),
    flag("--loud"),
    summary="Add a todo.",
)
async def add(state: Session, text: str, tags: tuple[str, ...], loud: bool) -> int:
    rendered = f"{text}|{','.join(tags)}"
    state.stdout.write((rendered.upper() if loud else rendered) + "\n")
    return 0


@command("done", argument("id", once(INT)), summary="Complete a todo.")
async def done(state: Session, todo_id: int) -> int:
    state.stdout.write(f"done {todo_id}\n")
    return todo_id % 2


@command("check", argument("paths", many(STR)), summary="Check paths.")
async def check(state: Session, paths: tuple[str, ...]) -> int:
    state.stdout.write(f"{len(paths)}:{'|'.join(paths)}\n")
    return 0


@command("consume", summary="Echo stdin.")
async def consume(state: Session) -> int:
    for chunk in state.stdin:
        state.stdout.write(f"[{chunk}]")
    return 0


@command("migrate", option("--to", optional(INT)), summary="Apply migrations.")
async def migrate(state: Database, target: int | None) -> int:
    state.session.stdout.write(f"migrate {state.dsn} -> {target}\n")
    state.session.events.append("migrated")
    return 0


db = group(
    "db",
    option("--dsn", once(STR), summary="Database URL."),
    state=database,
    commands=(migrate,),
    summary="Database maintenance.",
)


def build() -> Arm[Streams]:
    """
    The whole example program, rebuilt per call so no test can disturb another's.

    The root is an ordinary `group`: its parent is the shell, which supplies the
    `Streams` its `state` factory derives a `Session` from.
    """
    return group(
        "todos",
        count(("-v", "--verbose"), summary="Raise log level."),
        option("--endpoint", default("http://localhost", STR), summary="Service URL."),
        state=session,
        commands=(show, add, done, check, consume, db),
        summary="A todo list.",
    )
