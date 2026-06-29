from __future__ import annotations

from integration.transform.cli import CliSettings
from integration.transform.cli import serve
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
