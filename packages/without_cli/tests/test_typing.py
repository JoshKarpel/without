from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import assert_type

from without_cli import INT
from without_cli import STR
from without_cli import Arm
from without_cli import Extractor
from without_cli import Streams
from without_cli import argument
from without_cli import command
from without_cli import count
from without_cli import flag
from without_cli import group
from without_cli import many
from without_cli import once
from without_cli import option
from without_cli import optional
from without_cli import rest


@dataclass(frozen=True, slots=True)
class Session:
    endpoint: str


@dataclass(frozen=True, slots=True)
class Unrelated:
    dsn: str


@asynccontextmanager
async def session(parent: Streams, endpoint: str) -> AsyncIterator[Session]:  # pragma: no cover - type-level only
    """The root's factory: its parent is the shell, so it receives the `Streams`."""
    yield Session(endpoint)


@asynccontextmanager
async def nested(parent: Session, dsn: str) -> AsyncIterator[Unrelated]:  # pragma: no cover - type-level only
    """A group's factory takes its parent's state, which is how state threads down a path."""
    yield Unrelated(dsn)


async def wants_session(state: Session) -> int:  # pragma: no cover - type-level only
    return 0


async def wants_unrelated(state: Unrelated) -> int:  # pragma: no cover - type-level only
    return 0


async def wants_int(state: Streams, value: int) -> int:  # pragma: no cover - type-level only
    return 0


async def wants_str(state: Streams, value: str) -> int:  # pragma: no cover - type-level only
    return 0


# The guarantees below have no runtime failure to catch, because turning them
# into static errors is the whole point. They are asserted against the type
# checker instead: `assert_type` pins what a token solves to, and each pinned
# `# type: ignore[code]` fails the build (under `warn_unused_ignores`) if the
# line ever stops erroring.
if TYPE_CHECKING:
    # A token's converter fixes the value's type all the way to the handler.
    assert_type(argument("id", INT), Extractor[int])
    assert_type(rest("paths", STR), Extractor[tuple[str, ...]])
    assert_type(flag("--loud"), Extractor[bool])
    assert_type(count("-v"), Extractor[int])
    assert_type(option("--port", once(INT)), Extractor[int])
    assert_type(option("--port", optional(INT)), Extractor[int | None])
    assert_type(option("--tag", many(STR)), Extractor[tuple[str, ...]])

    # A command's state type is solved from its handler, and a program ties that
    # to what its `state` factory actually builds.
    assert_type(command("show")(wants_session), Arm[Session])
    assert_type(
        group("app", option("--endpoint", once(STR)), state=session, commands=(command("show")(wants_session),)),
        Arm[Streams],
    )

    # An `int` token paired with a handler wanting a `str` is refused, with no
    # runtime introspection anywhere.
    command("show", argument("id", INT))(wants_str)  # type: ignore[arg-type]
    command("show", argument("id", STR))(wants_int)  # type: ignore[arg-type]

    # A command wanting one state cannot be assembled under a program building
    # another. `without-web` catches the equivalent only when its router is
    # annotated, because an unannotated `Router(routes=(...))` joins to
    # `Router[Any]`; here `state` and `commands` both constrain the same
    # variable, so the bare call is checked too.
    group(
        "app",
        option("--endpoint", once(STR)),
        state=session,  # type: ignore[arg-type]
        commands=(command("show")(wants_unrelated),),
    )

    # Two commands wanting different states cannot be siblings: there is no `U`
    # that satisfies both, so the call has no solution rather than a silent `Any`.
    group(  # type: ignore[misc]
        "app",
        option("--endpoint", once(STR)),
        state=session,
        commands=(command("a")(wants_session), command("b")(wants_unrelated)),
    )

    # A group threads its parent's state into its own factory, and ties what that
    # factory builds to what its children want.
    group("db", option("--dsn", once(STR)), state=nested, commands=(command("show")(wants_unrelated),))
    group(
        "db",
        option("--dsn", once(STR)),
        state=nested,  # type: ignore[arg-type]
        commands=(command("show")(wants_session),),
    )
