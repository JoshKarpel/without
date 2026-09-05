from __future__ import annotations

from pathlib import Path

import pytest
from without_cli import ANSWERED
from without_cli import STR
from without_cli import Answered
from without_cli import Cardinality
from without_cli import FromEnv
from without_cli import FromFile
from without_cli import argument
from without_cli import command
from without_cli import default
from without_cli import group
from without_cli import many
from without_cli import once
from without_cli import option
from without_cli import optional
from without_cli import parse_argv
from without_cli import render
from without_cli import usage

from .helpers import build
from .helpers import unreached


def _usage_for(argv: list[str]) -> str:
    outcome = parse_argv(build(), argv=argv, answered=ANSWERED)
    assert isinstance(outcome, Answered)
    return render(outcome.usage)


# Deliberately carries no docstring, where `unreached` does: a command's summary
# falls back to its handler's docstring, so a command with *no* summary can only be
# built from a handler that has none. That is also why this explanation is a comment.
async def _undescribed(state: object, *values: object) -> int:
    raise AssertionError("a command that should not have run was invoked")  # pragma: no cover - never runs


class TestSynopsis:
    def test_a_group_lists_a_command_placeholder(self) -> None:
        assert usage((build().node,)).invocation == "todos [OPTIONS] COMMAND [ARGS]..."

    def test_a_leaf_lists_its_positionals(self) -> None:
        root = build().node
        add = root.child("add")
        assert add is not None
        assert usage((root, add)).invocation == "todos add [OPTIONS] TEXT"

    def test_a_variadic_positional_is_marked(self) -> None:
        root = build().node
        check = root.child("check")
        assert check is not None
        assert usage((root, check)).invocation == "todos check [OPTIONS] [PATHS...]"

    def test_a_command_with_nothing_to_take_says_so(self) -> None:
        app = group("app", commands=(command("bare")(unreached),))
        assert usage((app.node, app.node.children[0])).invocation == "app bare"

    def test_the_path_accumulates_through_groups(self) -> None:
        root = build().node
        db = root.child("db")
        assert db is not None
        migrate = db.child("migrate")
        assert migrate is not None
        assert usage((root, db, migrate)).path == ("todos", "db", "migrate")


class TestInheritance:
    def test_an_ancestors_options_are_carried_but_kept_separate(self) -> None:
        root = build().node
        db = root.child("db")
        assert db is not None
        migrate = db.child("migrate")
        assert migrate is not None
        described = usage((root, db, migrate))
        assert [o.canonical for o in described.options] == ["--to"]
        assert [o.canonical for o in described.inherited] == ["--verbose", "--endpoint", "--dsn"]

    def test_a_nested_command_documents_the_options_spelled_above_it(self) -> None:
        rendered = _usage_for(["db", "migrate", "--help"])
        assert "--dsn DSN" in rendered
        assert "--endpoint ENDPOINT" in rendered


class TestRendering:
    def test_the_root_lists_its_commands_with_their_summaries(self) -> None:
        rendered = _usage_for(["--help"])
        assert "A todo list." in rendered
        listed = [line.split() for line in rendered.splitlines() if line.startswith("  ")]
        assert ["add", "Add", "a", "todo."] in listed
        assert ["db", "Database", "maintenance."] in listed

    def test_a_repeatable_option_is_marked(self) -> None:
        assert "-t, --tag TAG ..." in _usage_for(["add", "--help"])

    def test_a_flag_shows_no_placeholder(self) -> None:
        rendered = _usage_for(["add", "--help"])
        assert "  --loud" in rendered
        assert "--loud LOUD" not in rendered

    def test_sources_and_requiredness_are_annotated(self) -> None:
        mount = Path("/run/secrets/token")
        token = option(
            "--token",
            once(STR),
            sources=(FromFile(mount), FromEnv("TOKEN")),
            summary="API token.",
        )

        app = group("app", commands=(command("show", token)(unreached),))
        described = usage((app.node, app.node.children[0]))
        rendered = render(described)
        assert f"[file: {mount}; env: TOKEN; required]" in rendered

    def test_an_argument_summary_is_shown(self) -> None:
        assert "TEXT  What to do." in _usage_for(["add", "--help"])

    def test_an_option_with_no_summary_leaves_no_trailing_space(self) -> None:
        show = command("show", option("--bare", many(STR)), argument("here", once(STR)))(unreached)
        app = group("app", commands=(show,))
        rendered = render(usage((app.node, app.node.children[0])))
        assert not any(line != line.rstrip() for line in rendered.splitlines())

    def test_a_command_with_no_summary_renders_without_a_description(self) -> None:
        app = group("app", commands=(command("show", argument("things", many(STR)))(_undescribed),))
        rendered = render(usage((app.node, app.node.children[0])))
        assert rendered.startswith("usage: app show [THINGS...]\n\nArguments:\n")

    @pytest.mark.parametrize(
        ("cardinality", "slot"),
        [
            (once(STR), "NOTE"),
            (optional(STR), "[NOTE]"),
            (default("x", STR), "[NOTE]"),
            (many(STR), "[NOTE...]"),
        ],
    )
    def test_a_positional_is_bracketed_when_it_may_be_omitted(
        self, cardinality: Cardinality[object], slot: str
    ) -> None:
        show = command("show", argument("note", cardinality))(unreached)
        app = group("app", commands=(show,))
        assert render(usage((app.node, app.node.children[0]))).startswith(f"usage: app show {slot}\n")
