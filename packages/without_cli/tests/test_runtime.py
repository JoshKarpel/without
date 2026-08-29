from __future__ import annotations

from pathlib import Path

import pytest
from without_cli import STR
from without_cli import Answered
from without_cli import FromEnv
from without_cli import FromFile
from without_cli import Streams
from without_cli import command
from without_cli import group
from without_cli import once
from without_cli import option
from without_cli import parse_argv
from without_cli import read_files
from without_cli import run

from .helpers import build
from .helpers import unreached


class TestRun:
    def test_a_command_supplies_the_exit_code(self) -> None:
        capture = Streams.captured()
        assert run(build(), argv=["done", "3"], env={}, streams=capture.streams) == 1
        assert capture.stdout == "done 3\n"

    def test_help_goes_to_stdout_with_a_zero_code(self) -> None:
        capture = Streams.captured()
        assert run(build(), argv=["--help"], env={}, streams=capture.streams) == 0
        assert capture.stdout.startswith("usage: todos [OPTIONS] COMMAND")
        assert capture.stderr == ""

    def test_a_version_goes_to_stdout_with_a_zero_code(self) -> None:
        app = group("app", commands=(command("show")(unreached),), version="app 1.4.2")
        capture = Streams.captured()
        assert run(app, argv=["--version"], env={}, streams=capture.streams) == 0
        assert capture.stdout == "app 1.4.2\n"
        assert capture.stderr == ""

    def test_a_version_on_a_level_that_declares_none_is_this_shells_rejection(self) -> None:
        # The parser stopped without an opinion; refusing an unversioned level is
        # `run`'s rule, so `run` is where the rejection is built.
        app = group("app", commands=(command("show")(unreached),))
        capture = Streams.captured()
        assert run(app, argv=["--version"], env={}, streams=capture.streams) == 2
        assert capture.stdout == ""
        assert "unknown option --version" in capture.stderr

    def test_a_shell_can_answer_its_own_spellings_without_touching_the_parser(self) -> None:
        # The point of moving the policy out: this whole alternative shell is the
        # `parse_argv` call plus a match, and `without-cli` knows none of it.
        app = group("app", commands=(command("show")(unreached),))
        outcome = parse_argv(app, argv=["--license"], answered=("--license", "-?"))
        assert isinstance(outcome, Answered)
        assert outcome.spelling == "--license"

    def test_a_bad_command_line_goes_to_stderr_with_code_two(self) -> None:
        capture = Streams.captured()
        assert run(build(), argv=["frobnicate"], env={}, streams=capture.streams) == 2
        assert capture.stdout == ""
        assert "unknown command 'frobnicate'" in capture.stderr

    def test_stdin_reaches_the_command_chunk_by_chunk(self) -> None:
        capture = Streams.captured(["one\n", "two\n"])
        assert run(build(), argv=["consume"], env={}, streams=capture.streams) == 0
        assert capture.stdout == "[one\n][two\n]"

    def test_a_string_stdin_arrives_as_one_chunk(self) -> None:
        capture = Streams.captured("whole thing")
        assert run(build(), argv=["consume"], env={}, streams=capture.streams) == 0
        assert capture.stdout == "[whole thing]"

    def test_the_environment_is_an_argument(self) -> None:
        token = option("--token", once(STR), sources=(FromEnv("TOKEN"),))

        @command("show", token)
        async def show(state: Streams, value: str) -> int:
            state.stdout.write(value)
            return 0

        app = group("app", commands=(show,))
        capture = Streams.captured()
        # No monkeypatching of `os.environ`: the environment is a value here.
        assert run(app, argv=["show"], env={"TOKEN": "injected"}, streams=capture.streams) == 0
        assert capture.stdout == "injected"

    def test_files_are_an_argument_too(self) -> None:
        mount = Path("/run/secrets/token")
        token = option("--token", once(STR), sources=(FromFile(mount),))

        @command("show", token)
        async def show(state: Streams, value: str) -> int:
            state.stdout.write(value)
            return 0

        app = group("app", commands=(show,))
        capture = Streams.captured()
        assert run(app, argv=["show"], env={}, files={mount: "secret\n"}, streams=capture.streams) == 0
        assert capture.stdout == "secret"

    def test_argv_defaults_to_the_process_argv(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("sys.argv", ["todos", "done", "2"])
        capture = Streams.captured()
        assert run(build(), env={}, streams=capture.streams) == 0
        assert capture.stdout == "done 2\n"

    def test_the_environment_defaults_to_the_process_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TOKEN", "ambient")
        token = option("--token", once(STR), sources=(FromEnv("TOKEN"),))

        @command("show", token)
        async def show(state: Streams, value: str) -> int:
            state.stdout.write(value)
            return 0

        app = group("app", commands=(show,))
        capture = Streams.captured()
        assert run(app, argv=["show"], streams=capture.streams) == 0
        assert capture.stdout == "ambient"

    def test_streams_default_to_the_process_streams(self, capsys: pytest.CaptureFixture[str]) -> None:
        assert run(build(), argv=["done", "4"], env={}) == 0
        assert capsys.readouterr().out == "done 4\n"


class TestReadFiles:
    def test_a_missing_file_is_absence_rather_than_an_error(self, tmp_path: Path) -> None:
        present = tmp_path / "here"
        present.write_text("value\n")
        missing = tmp_path / "gone"
        assert read_files([present, missing]) == {present: "value\n"}

    def test_an_unreadable_file_raises(self, tmp_path: Path) -> None:
        # A file that exists but cannot be read is a broken deployment, not an
        # unconfigured one, so it is not quietly treated as absent. Reading a
        # directory is the portable way to provoke that: POSIX raises
        # `IsADirectoryError` and Windows `PermissionError`, so the assertion is
        # on their common base.
        directory = tmp_path / "a-directory"
        directory.mkdir()
        with pytest.raises(OSError, match="a-directory") as raised:
            read_files([directory])
        assert not isinstance(raised.value, FileNotFoundError)

    def test_run_reads_the_files_a_program_names(self, tmp_path: Path) -> None:
        mount = tmp_path / "token"
        mount.write_text("from-disk\n")
        token = option("--token", once(STR), sources=(FromFile(mount),))

        @command("show", token)
        async def show(state: Streams, value: str) -> int:
            state.stdout.write(value)
            return 0

        app = group("app", commands=(show,))
        capture = Streams.captured()
        # `files` omitted, so `run` does the reading, from paths recovered from
        # the tree rather than from anything restated.
        assert run(app, argv=["show"], env={}, streams=capture.streams) == 0
        assert capture.stdout == "from-disk"
