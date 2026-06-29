from __future__ import annotations

import io
import sys

import pytest
from integration.transform.cli import CliSettings
from integration.transform.cli import main
from integration.transform.cli import serve
from integration.transform.cli import stdin_lines
from integration.transform.cli import transform_lines
from integration.transform.core import Mode
from integration.transform.core import TransformConfig
from without import collect
from without import stream
from without_env import EnvContext


async def test_transform_lines_transforms_each_line_with_the_config_mode() -> None:
    process = transform_lines(TransformConfig(default_mode=Mode.TITLE))

    transformed = await collect(process(stream(["the quiet part", "out loud"])))

    assert transformed == ["The Quiet Part", "Out Loud"]


async def test_serve_emits_each_line_transformed_with_the_sampled_config() -> None:
    config = EnvContext(settings=CliSettings(transform=TransformConfig(default_mode=Mode.UPPER)))
    emitted: list[str] = []

    await serve(config, stream(["hello", "world"]), emitted.append)

    assert emitted == ["HELLO", "WORLD"]


def test_cli_settings_prompt_defaults_to_an_angle_bracket() -> None:
    assert CliSettings().prompt == ">"


def test_cli_settings_embed_the_domain_config_separately_from_the_shell_knob() -> None:
    settings = CliSettings(transform=TransformConfig(default_mode=Mode.LOWER), prompt="$")

    assert settings.transform == TransformConfig(default_mode=Mode.LOWER)
    assert settings.prompt == "$"


async def test_stdin_lines_prompts_then_yields_each_line_stripped_until_eof(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(sys, "stdin", io.StringIO("first\nsecond\n"))

    lines = await collect(stdin_lines("? "))

    assert lines == ["first", "second"]
    # The prompt precedes every read, including the final one that hits EOF, which
    # writes a closing newline before the stream ends.
    assert capsys.readouterr().out == "? ? ? \n"


def test_main_runs_the_cli_over_stdin_under_the_env_config(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("TEXT_TRANSFORM__DEFAULT_MODE", "lower")
    monkeypatch.setenv("TEXT_PROMPT", "$ ")
    monkeypatch.setattr(sys, "stdin", io.StringIO("HELLO\nWoRlD\n"))

    main()

    out = capsys.readouterr().out
    assert "hello" in out
    assert "world" in out
    assert "$ " in out
