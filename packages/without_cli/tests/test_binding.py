from __future__ import annotations

from pathlib import Path

import pytest
from without_cli import INT
from without_cli import STR
from without_cli import Bound
from without_cli import FromEnv
from without_cli import FromFile
from without_cli import Helped
from without_cli import Rejected
from without_cli import Streams
from without_cli import argument
from without_cli import command
from without_cli import count
from without_cli import default
from without_cli import flag
from without_cli import group
from without_cli import once
from without_cli import option
from without_cli import parse_argv
from without_cli import render_rejection
from without_cli import rest

from .helpers import build
from .helpers import unreached


def _rejected(argv: list[str]) -> Rejected:
    outcome = parse_argv(build(), argv=argv)
    assert isinstance(outcome, Rejected)
    return outcome


class TestScanning:
    @pytest.mark.parametrize(
        ("argv", "expected"),
        [
            (["add", "text", "--tag", "a", "--tag", "b"], "text|a,b\n"),
            (["add", "text", "--tag=a", "--tag=b"], "text|a,b\n"),
            (["add", "text", "-t", "a", "-t", "b"], "text|a,b\n"),
            (["add", "text", "-ta", "-tb"], "text|a,b\n"),
            (["add", "text"], "text|\n"),
            (["add", "text", "--loud"], "TEXT|\n"),
            (["add", "text", "-ta", "--loud"], "TEXT|A\n"),
        ],
    )
    async def test_option_spellings_all_reach_the_handler(self, argv: list[str], expected: str) -> None:
        capture = Streams.captured()
        outcome = parse_argv(build(), argv=argv)
        assert isinstance(outcome, Bound)
        await outcome.action(capture.streams)
        assert capture.stdout == expected

    async def test_short_flags_bundle_into_one_token(self) -> None:
        capture = Streams.captured()
        outcome = parse_argv(build(), argv=["-vvv", "show"])
        assert isinstance(outcome, Bound)
        await outcome.action(capture.streams)
        assert "v3" in capture.stdout

    async def test_a_double_dash_ends_option_parsing(self) -> None:
        capture = Streams.captured()
        outcome = parse_argv(build(), argv=["check", "--", "--loud", "-t"])
        assert isinstance(outcome, Bound)
        await outcome.action(capture.streams)
        assert capture.stdout == "2:--loud|-t\n"

    async def test_a_bare_dash_is_a_positional(self) -> None:
        capture = Streams.captured()
        outcome = parse_argv(build(), argv=["check", "-"])
        assert isinstance(outcome, Bound)
        await outcome.action(capture.streams)
        assert capture.stdout == "1:-\n"

    @pytest.mark.parametrize(
        ("argv", "message"),
        [
            (["--nope", "show"], "unknown option --nope"),
            (["show", "--nope"], "unknown option --nope"),
            (["show", "-q"], "unknown option -q"),
            (["--verbose=3", "show"], "option --verbose takes no value"),
            (["--endpoint"], "option --endpoint expects a value"),
            (["db", "--dsn"], "option --dsn expects a value"),
            (["add", "text", "-t"], "option -t expects a value"),
        ],
    )
    async def test_a_malformed_option_is_refused(self, argv: list[str], message: str) -> None:
        rejected = _rejected(argv)
        assert rejected.message == message

    async def test_an_unexpected_positional_is_refused(self) -> None:
        rejected = _rejected(["done", "1", "2"])
        assert rejected.message == "unexpected argument '2'"


class TestSelection:
    async def test_a_missing_command_is_refused(self) -> None:
        rejected = _rejected([])
        assert rejected.message == "expected a command"

    async def test_an_unknown_command_lists_the_known_ones(self) -> None:
        rejected = _rejected(["frobnicate"])
        assert rejected.message.startswith("unknown command 'frobnicate'")
        assert "add" in rejected.message
        assert "db" in rejected.message

    async def test_options_bind_to_the_level_they_are_spelled_at(self) -> None:
        capture = Streams.captured()
        outcome = parse_argv(
            build(), argv=["-vv", "--endpoint", "http://x", "db", "--dsn", "d", "migrate", "--to", "3"]
        )
        assert isinstance(outcome, Bound)
        assert await outcome.action(capture.streams) == 0
        assert capture.stdout == "migrate d -> 3\n"

    @pytest.mark.parametrize("argv", [["--help"], ["-h"], ["add", "--help"], ["db", "migrate", "--help"]])
    async def test_help_is_an_outcome_not_an_error(self, argv: list[str]) -> None:
        assert isinstance(parse_argv(build(), argv=argv), Helped)

    async def test_help_wins_over_a_missing_required_option(self) -> None:
        # `--dsn` is required, but asking for help must still answer rather than
        # complain about the thing you were asking how to spell.
        assert isinstance(parse_argv(build(), argv=["db", "migrate", "--help"]), Helped)


class TestSources:
    async def test_the_environment_fills_an_omitted_option(self) -> None:
        secret = option("--token", once(STR), sources=(FromEnv("TOKEN"),))

        @command("show", secret)
        async def show(state: Streams, token: str) -> int:
            state.stdout.write(token)
            return 0

        app = group("app", commands=(show,))
        capture = Streams.captured()
        outcome = parse_argv(app, argv=["show"], env={"TOKEN": "from-env"})
        assert isinstance(outcome, Bound)
        await outcome.action(capture.streams)
        assert capture.stdout == "from-env"

    async def test_the_command_line_beats_every_source(self) -> None:
        secret = option("--token", once(STR), sources=(FromEnv("TOKEN"),))

        @command("show", secret)
        async def show(state: Streams, token: str) -> int:
            state.stdout.write(token)
            return 0

        app = group("app", commands=(show,))
        capture = Streams.captured()
        outcome = parse_argv(app, argv=["show", "--token", "explicit"], env={"TOKEN": "from-env"})
        assert isinstance(outcome, Bound)
        await outcome.action(capture.streams)
        assert capture.stdout == "explicit"

    async def test_sources_are_tried_in_order_and_the_first_hit_wins(self) -> None:
        mount = Path("/run/secrets/token")
        secret = option("--token", once(STR), sources=(FromFile(mount), FromEnv("TOKEN")))

        @command("show", secret)
        async def show(state: Streams, token: str) -> int:
            state.stdout.write(token)
            return 0

        app = group("app", commands=(show,))
        capture = Streams.captured()
        outcome = parse_argv(app, argv=["show"], env={"TOKEN": "from-env"}, files={mount: "from-file\n"})
        assert isinstance(outcome, Bound)
        await outcome.action(capture.streams)
        # The file is declared first, and its trailing newline is stripped: a
        # secret mount almost always has one, and a token with `\n` on the end
        # fails somewhere far away from the cause.
        assert capture.stdout == "from-file"

    async def test_a_file_source_can_keep_its_trailing_whitespace(self) -> None:
        mount = Path("/run/secrets/blob")
        blob = option("--blob", once(STR), sources=(FromFile(mount, strip=False),))

        @command("show", blob)
        async def show(state: Streams, value: str) -> int:
            state.stdout.write(repr(value))
            return 0

        app = group("app", commands=(show,))
        capture = Streams.captured()
        outcome = parse_argv(app, argv=["show"], files={mount: "kept\n"})
        assert isinstance(outcome, Bound)
        await outcome.action(capture.streams)
        assert capture.stdout == "'kept\\n'"

    async def test_an_absent_source_leaves_the_cardinality_to_decide(self) -> None:
        needed = option("--token", once(STR), sources=(FromEnv("TOKEN"),))
        fallback = option("--host", default("localhost", STR), sources=(FromEnv("HOST"),))

        @command("show", needed, fallback)
        async def show(state: Streams, token: str, host: str) -> int:
            state.stdout.write(f"{token}@{host}")
            return 0

        app = group("app", commands=(show,))

        # The control: with the source present, the required option is satisfied
        # and the defaulted one falls back, so absence is the only difference
        # between this and the rejection below.
        capture = Streams.captured()
        supplied = parse_argv(app, argv=["show"], env={"TOKEN": "t"})
        assert isinstance(supplied, Bound)
        await supplied.action(capture.streams)
        assert capture.stdout == "t@localhost"

        outcome = parse_argv(app, argv=["show"], env={})
        assert isinstance(outcome, Rejected)
        assert outcome.message == "--token: expected a value, got none"

    async def test_a_flag_can_be_turned_off_by_a_source(self) -> None:
        loud = flag("--loud", sources=(FromEnv("LOUD"),))

        @command("show", loud)
        async def show(state: Streams, value: bool) -> int:
            state.stdout.write(str(value))
            return 0

        app = group("app", commands=(show,))
        for value, expected in (("1", "True"), ("false", "False"), ("on", "True")):
            capture = Streams.captured()
            outcome = parse_argv(app, argv=["show"], env={"LOUD": value})
            assert isinstance(outcome, Bound)
            await outcome.action(capture.streams)
            assert capture.stdout == expected

    async def test_a_count_from_a_source_matches_repetition(self) -> None:
        level = count(("-v", "--verbose"), sources=(FromEnv("VERBOSE"),))

        @command("show", level)
        async def show(state: Streams, value: int) -> int:
            state.stdout.write(str(value))
            return 0

        app = group("app", commands=(show,))
        from_env = Streams.captured()
        outcome = parse_argv(app, argv=["show"], env={"VERBOSE": "3"})
        assert isinstance(outcome, Bound)
        await outcome.action(from_env.streams)

        from_argv = Streams.captured()
        outcome = parse_argv(app, argv=["show", "-vvv"], env={})
        assert isinstance(outcome, Bound)
        await outcome.action(from_argv.streams)

        assert from_env.stdout == from_argv.stdout == "3"

    async def test_a_bad_source_value_is_rejected_like_a_bad_argument(self) -> None:
        port = option("--port", once(INT), sources=(FromEnv("PORT"),))
        app = group("app", commands=(command("show", port)(unreached),))
        outcome = parse_argv(app, argv=["show"], env={"PORT": "not-a-number"})
        assert isinstance(outcome, Rejected)
        assert outcome.message == "--port: expected INT, got 'not-a-number'"


class TestRejectionRendering:
    async def test_a_rejection_names_the_path_and_points_at_help(self) -> None:
        rejected = _rejected(["db", "--dsn", "d", "migrate", "--to", "x"])
        rendered = render_rejection(rejected)
        assert rendered.splitlines() == [
            "todos db migrate: --to: expected INT, got 'x'",
            "usage: todos db migrate [OPTIONS]",
            "try 'todos db migrate --help' for more information",
        ]


class TestPositionals:
    async def test_a_variadic_positional_takes_what_is_left(self) -> None:
        capture = Streams.captured()
        outcome = parse_argv(build(), argv=["check", "a", "b", "c"])
        assert isinstance(outcome, Bound)
        await outcome.action(capture.streams)
        assert capture.stdout == "3:a|b|c\n"

    async def test_a_variadic_positional_accepts_nothing(self) -> None:
        capture = Streams.captured()
        outcome = parse_argv(build(), argv=["check"])
        assert isinstance(outcome, Bound)
        await outcome.action(capture.streams)
        assert capture.stdout == "0:\n"

    async def test_a_missing_required_positional_is_rejected(self) -> None:
        rejected = _rejected(["add"])
        assert rejected.message == "text: expected a value for TEXT"

    async def test_positionals_fill_in_declaration_order(self) -> None:
        @command("pair", argument("first", STR), argument("second", STR), rest("others", STR))
        async def pair(state: Streams, first: str, second: str, others: tuple[str, ...]) -> int:
            state.stdout.write(f"{first}/{second}/{others}")
            return 0

        app = group("app", commands=(pair,))
        capture = Streams.captured()
        outcome = parse_argv(app, argv=["pair", "a", "b", "c", "d"])
        assert isinstance(outcome, Bound)
        await outcome.action(capture.streams)
        assert capture.stdout == "a/b/('c', 'd')"


class TestCardinalities:
    @pytest.mark.parametrize(
        ("argv", "message"),
        [
            (["--endpoint", "a", "--endpoint", "b", "show"], "--endpoint: expected at most one value, got 2"),
        ],
    )
    async def test_a_repeated_singleton_is_rejected(self, argv: list[str], message: str) -> None:
        rejected = _rejected(argv)
        assert rejected.message == message

    async def test_a_repeated_required_singleton_is_rejected(self) -> None:
        rejected = _rejected(["db", "--dsn", "a", "--dsn", "b", "migrate"])
        assert rejected.message == "--dsn: expected one value, got 2"

    async def test_many_accepts_repetition(self) -> None:
        capture = Streams.captured()
        outcome = parse_argv(build(), argv=["add", "t", "-t", "a", "-t", "b", "-t", "c"])
        assert isinstance(outcome, Bound)
        await outcome.action(capture.streams)
        assert capture.stdout == "t|a,b,c\n"
