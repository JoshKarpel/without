from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING
from typing import assert_type

from without_cli import INT
from without_cli import STR
from without_cli import Arm
from without_cli import Converter
from without_cli import Extractor
from without_cli import Streams
from without_cli import argument
from without_cli import choice
from without_cli import command
from without_cli import count
from without_cli import default
from without_cli import flag
from without_cli import group
from without_cli import many
from without_cli import once
from without_cli import option
from without_cli import optional


class Profile(StrEnum):
    DEV = "dev"
    PROD = "prod"


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


async def wants_twenty(  # pragma: no cover - type-level only
    state: Streams,
    v0: int,
    v1: int,
    v2: int,
    v3: int,
    v4: int,
    v5: int,
    v6: int,
    v7: int,
    v8: int,
    v9: int,
    v10: int,
    v11: int,
    v12: int,
    v13: int,
    v14: int,
    v15: int,
    v16: int,
    v17: int,
    v18: int,
    v19: int,
) -> int:
    return 0


# The guarantees below have no runtime failure to catch, because turning them
# into static errors is the whole point. They are asserted against the type
# checker instead: `assert_type` pins what a token solves to, and each pinned
# `# type: ignore[code]` fails the build (under `warn_unused_ignores`) if the
# line ever stops erroring.
if TYPE_CHECKING:
    # A token's converter fixes the value's type all the way to the handler.
    # Positionals take the same cardinality vocabulary options do, so what each
    # one yields is read off the cardinality rather than off a separate function.
    assert_type(argument("id", once(INT)), Extractor[int])
    assert_type(argument("id", optional(INT)), Extractor[int | None])
    assert_type(argument("id", default(0, INT)), Extractor[int])
    assert_type(argument("paths", many(STR)), Extractor[tuple[str, ...]])
    assert_type(flag("--loud"), Extractor[bool])
    assert_type(count("-v"), Extractor[int])
    assert_type(option("--port", once(INT)), Extractor[int])
    assert_type(option("--port", optional(INT)), Extractor[int | None])
    assert_type(option("--tag", many(STR)), Extractor[tuple[str, ...]])

    # `choice` keeps the enum's own type, so a handler receives a member rather
    # than the string it was spelled as.
    assert_type(choice(Profile), Converter[Profile])
    assert_type(argument("profile", once(choice(Profile))), Extractor[Profile])

    # A command's state type is solved from its handler, and a program ties that
    # to what its `state` factory actually builds.
    assert_type(command("show")(wants_session), Arm[Session])
    assert_type(
        group("app", option("--endpoint", once(STR)), state=session, commands=(command("show")(wants_session),)),
        Arm[Streams],
    )

    # An `int` token paired with a handler wanting a `str` is refused, with no
    # runtime introspection anywhere.
    command("show", argument("id", once(INT)))(wants_str)  # type: ignore[arg-type]
    command("show", argument("id", once(STR)))(wants_int)  # type: ignore[arg-type]

    # The top of the overload ladder. Twenty tokens is the last arity that ties
    # each token to its handler parameter; past it the call matches no overload
    # and `into` is the way to combine several tokens into one.
    assert_type(
        command(
            "wide",
            argument("v0", once(INT)),
            argument("v1", once(INT)),
            argument("v2", once(INT)),
            argument("v3", once(INT)),
            argument("v4", once(INT)),
            argument("v5", once(INT)),
            argument("v6", once(INT)),
            argument("v7", once(INT)),
            argument("v8", once(INT)),
            argument("v9", once(INT)),
            argument("v10", once(INT)),
            argument("v11", once(INT)),
            argument("v12", once(INT)),
            argument("v13", once(INT)),
            argument("v14", once(INT)),
            argument("v15", once(INT)),
            argument("v16", once(INT)),
            argument("v17", once(INT)),
            argument("v18", once(INT)),
            argument("v19", once(INT)),
        )(wants_twenty),
        Arm[Streams],
    )

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
