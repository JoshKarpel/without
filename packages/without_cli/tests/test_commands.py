from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from without_cli import INT
from without_cli import STR
from without_cli import Bound
from without_cli import DeclarationError
from without_cli import FromEnv
from without_cli import FromFile
from without_cli import Option
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
from without_cli import parse_argv
from without_cli import source_paths

from .helpers import Database
from .helpers import Session
from .helpers import build
from .helpers import database
from .helpers import session
from .helpers import unreached


class TestDeclaration:
    def test_two_variadic_positionals_are_refused(self) -> None:
        with pytest.raises(DeclarationError, match="more than one `many` argument"):
            command("bad", argument("a", many(STR)), argument("b", many(STR)))

    def test_a_variadic_positional_must_come_last(self) -> None:
        with pytest.raises(DeclarationError, match="not its last positional"):
            command("bad", argument("a", many(STR)), argument("b", once(STR)))

    def test_a_required_positional_cannot_follow_an_optional_one(self) -> None:
        # With one bare token to hand out, the greedy assignment would give it to
        # `a` and leave `b` empty, so the layout can never be satisfied.
        with pytest.raises(DeclarationError, match="required argument after an optional one"):
            command("bad", argument("a", optional(STR)), argument("b", once(STR)))

    def test_a_defaulted_positional_also_counts_as_optional(self) -> None:
        with pytest.raises(DeclarationError, match="required argument after an optional one"):
            command("bad", argument("a", default("x", STR)), argument("b", once(STR)))

    def test_two_positionals_cannot_share_a_name(self) -> None:
        with pytest.raises(DeclarationError, match="same name"):
            command("bad", argument("a", once(STR)), argument("a", once(INT)))

    def test_two_options_cannot_share_a_spelling(self) -> None:
        # The binder resolves a spelling to one option, so the later declaration
        # would silently win: `--x` would take no value and `prog --x hi` would
        # fail on a stray `hi` rather than on the two tokens that disagree.
        with pytest.raises(DeclarationError, match="declares the option '--x' twice"):
            command("bad", option("--x", once(STR)), flag("--x"))

    def test_an_alias_shared_with_another_option_is_refused(self) -> None:
        # Aliases count the same as the canonical name: `-v` reaching two options
        # is as ambiguous as `--verbose` being declared twice.
        with pytest.raises(DeclarationError, match="declares the option '-v' twice"):
            command("bad", flag(("-v", "--verbose")), count(("-v", "--volume")))

    def test_a_group_refuses_two_options_with_one_spelling(self) -> None:
        leaf = command("leaf")(unreached)
        with pytest.raises(DeclarationError, match="declares the option '--x' twice"):
            group("app", option("--x", once(STR)), flag("--x"), state=_never_pair, commands=(leaf,))

    def test_a_level_with_no_commands_is_refused(self) -> None:
        with pytest.raises(DeclarationError, match="declares no commands"):
            group("empty", commands=())

    def test_two_commands_cannot_share_a_name(self) -> None:
        first = command("same")(unreached)
        second = command("same")(unreached)
        with pytest.raises(DeclarationError, match="same name"):
            group("app", commands=(first, second))

    def test_a_level_with_subcommands_cannot_take_positionals(self) -> None:
        leaf = command("leaf")(unreached)
        with pytest.raises(DeclarationError, match="cannot also declare positional"):
            group("app", argument("oops", once(STR)), state=_never, commands=(leaf,))

    def test_options_with_nothing_to_parse_them_into_are_refused(self) -> None:
        # A program with no `state` has nowhere to put its own options, so
        # declaring them is a mistake rather than a silently ignored value. The
        # overloads already refuse it (hence the pinned ignore, which fails the
        # build if it ever stops erroring); the runtime guard covers callers who
        # do not run a type checker.
        leaf = command("leaf")(unreached)
        with pytest.raises(DeclarationError, match="no `state` to parse them into"):
            group("app", option("--x", once(STR)), commands=(leaf,))  # type: ignore[call-overload]


@asynccontextmanager
async def _never(streams: Streams, value: str) -> AsyncIterator[None]:  # pragma: no cover - never entered
    yield None


@asynccontextmanager
async def _never_pair(
    streams: Streams, value: str, toggle: bool
) -> AsyncIterator[None]:  # pragma: no cover - never entered
    yield None


class TestArmsAreValues:
    def test_a_command_carries_its_own_description(self) -> None:
        arm = build()
        add = arm.node.child("add")
        assert add is not None
        assert add.summary == "Add a todo."
        assert [p.metavar for p in add.positionals] == ["TEXT"]
        assert [o.canonical for o in add.options] == ["--tag", "--loud"]

    def test_a_command_with_no_summary_describes_itself_from_its_docstring(self) -> None:
        async def handler(streams: Streams) -> int:
            """Trim the backlog."""
            return 0  # pragma: no cover - the docstring is the subject, not the body

        assert command("prune")(handler).node.summary == "Trim the backlog."

    def test_a_docstring_opening_on_the_next_line_reads_the_same(self) -> None:
        async def handler(streams: Streams) -> int:
            """
            Trim the backlog.

            The rest of the docstring is for whoever reads the code, not for
            `--help`, so only the first line crosses.
            """
            return 0  # pragma: no cover - the docstring is the subject, not the body

        assert command("prune")(handler).node.summary == "Trim the backlog."

    def test_an_explicit_summary_beats_the_docstring(self) -> None:
        async def handler(streams: Streams) -> int:
            """Words for the next reader of the code."""
            return 0  # pragma: no cover - the docstring is the subject, not the body

        arm = command("prune", summary="Words for the person running it.")(handler)
        assert arm.node.summary == "Words for the person running it."

    def test_a_command_with_neither_has_no_summary(self) -> None:
        async def handler(streams: Streams) -> int:
            return 0  # pragma: no cover - the absent docstring is the subject, not the body

        assert command("prune")(handler).node.summary == ""

    def test_the_canonical_name_is_the_long_one_whichever_order_it_is_written(self) -> None:
        [short_first] = option(("-t", "--tag"), once(STR)).parameters
        [long_first] = option(("--tag", "-t"), once(STR)).parameters
        assert isinstance(short_first, Option)
        assert isinstance(long_first, Option)
        # Which alias is written first is a presentation choice: it changes the
        # order they are listed in, and nothing about where values are stored or
        # what the placeholder is called.
        assert (
            (short_first.canonical, short_first.metavar)
            == (long_first.canonical, long_first.metavar)
            == ("--tag", "TAG")
        )

    def test_an_option_with_only_a_short_name_falls_back_to_it(self) -> None:
        [only] = option("-x", once(STR)).parameters
        assert only.canonical == "-x"  # type: ignore[union-attr]

    def test_the_same_arm_can_be_placed_under_two_programs(self) -> None:
        # The property that makes a command shippable from a package: it carries
        # its name, parsing, and behaviour, so placing it costs one edit.
        shared = command("ping")(unreached)
        first = group("one", commands=(shared,))
        second = group("two", commands=(shared,))
        assert first.node.child("ping") == second.node.child("ping")

    def test_file_sources_are_recovered_from_the_whole_tree(self) -> None:
        mount = Path("/run/secrets/deep")

        leaf = command("leaf", option("--secret", once(STR), sources=(FromFile(mount), FromEnv("X"))))(unreached)
        nested = group("db", option("--dsn", once(STR)), state=database, commands=(leaf,))
        app = group("app", commands=(command("other")(unreached),))
        assert source_paths(nested.node) == (mount,)
        assert source_paths(app.node) == ()


class TestStateLifetime:
    async def test_a_program_opens_its_state_around_the_command(self) -> None:
        capture = Streams.captured()
        outcome = parse_argv(build(), argv=["show"])
        assert isinstance(outcome, Bound)
        await outcome.action(capture.streams)
        # The command saw the session already open, and it was closed afterwards.
        assert "'opened'" in capture.stdout
        assert capture.stderr == "session closed\n"

    async def test_a_group_nests_its_own_state_inside_its_parents(self) -> None:
        capture = Streams.captured()
        outcome = parse_argv(build(), argv=["db", "--dsn", "sqlite://x", "migrate"])
        assert isinstance(outcome, Bound)
        assert await outcome.action(capture.streams) == 0
        assert capture.stdout == "migrate sqlite://x -> None\n"

    async def test_a_group_unwinds_inside_its_parent(self) -> None:
        seen: list[list[str]] = []

        @command("peek")
        async def peek(state: Database) -> int:
            seen.append(list(state.session.events))
            return 0

        nested = group("db", option("--dsn", once(STR)), state=database, commands=(peek,))
        app = group(
            "app",
            count("-v"),
            option("--endpoint", once(STR)),
            state=session,
            commands=(nested,),
        )
        outcome = parse_argv(app, argv=["--endpoint", "e", "db", "--dsn", "d", "peek"])
        assert isinstance(outcome, Bound)
        await outcome.action(Streams.captured().streams)
        # The command ran with both open, innermost last.
        assert seen == [["opened", "db opened d"]]

    async def test_a_group_is_not_entered_for_a_sibling_command(self) -> None:
        capture = Streams.captured()
        outcome = parse_argv(build(), argv=["show"])
        assert isinstance(outcome, Bound)
        await outcome.action(capture.streams)
        # `db`'s context manager appends to the session's events; `show` never
        # selected it, so it never ran.
        assert "db opened" not in capture.stdout

    async def test_nothing_is_opened_when_the_command_line_is_bad(self) -> None:
        opened: list[str] = []

        @asynccontextmanager
        async def watched(streams: Streams, endpoint: str) -> AsyncIterator[Session]:
            opened.append("opened")
            yield Session.opened(streams, endpoint=endpoint, verbosity=0)

        @command("go", argument("count", once(INT)))
        async def go(state: Session, number: int) -> int:
            state.stdout.write(f"{state.endpoint}:{number}")
            return 0

        app = group("app", option("--endpoint", once(STR)), state=watched, commands=(go,))

        # The control: a good command line does reach the resource, so a later
        # empty `opened` means the rejection stopped it rather than the harness
        # never wiring it up.
        capture = Streams.captured()
        good = parse_argv(app, argv=["--endpoint", "x", "go", "7"])
        assert isinstance(good, Bound)
        assert await good.action(capture.streams) == 0
        assert capture.stdout == "x:7"
        assert opened == ["opened"]

        opened.clear()
        assert not isinstance(parse_argv(app, argv=["--endpoint", "x", "go", "nope"]), Bound)
        # The whole point of extracting eagerly: a rejected command line never
        # reaches the resource.
        assert opened == []


class TestNamespacing:
    async def test_a_stateless_group_passes_its_parents_state_through(self) -> None:
        @command("inner")
        async def inner(state: Session) -> int:
            state.stdout.write(state.endpoint)
            return 0

        namespaced = group("tools", commands=(inner,))
        app = group(
            "app",
            count("-v"),
            option("--endpoint", once(STR)),
            state=session,
            commands=(namespaced,),
        )
        capture = Streams.captured()
        outcome = parse_argv(app, argv=["--endpoint", "http://kept", "tools", "inner"])
        assert isinstance(outcome, Bound)
        assert await outcome.action(capture.streams) == 0
        # Pure namespacing: nothing was built, and the command saw exactly what
        # the program above it built.
        assert capture.stdout == "http://kept"

    async def test_a_tree_with_no_state_anywhere_hands_its_commands_the_streams(self) -> None:
        @command("inner")
        async def inner(state: Streams) -> int:
            state.stdout.write("wrote through the state itself")
            return 0

        app = group("app", commands=(group("tools", commands=(inner,)),))
        capture = Streams.captured()
        outcome = parse_argv(app, argv=["tools", "inner"])
        assert isinstance(outcome, Bound)
        assert await outcome.action(capture.streams) == 0
        # No `state` declared at either level, so the shell's streams reach the
        # command unchanged and there is no ignored parameter anywhere.
        assert capture.stdout == "wrote through the state itself"
