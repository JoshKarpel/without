from collections.abc import AsyncIterator
from pathlib import Path

from pydantic import BaseModel
from without import collect
from without import sample
from without.testing import tick
from without_configmap import read_yaml_file
from without_configmap import watch_config


class DbConfig(BaseModel):
    host: str
    port: int = 5432


def _write_config(mount: Path, body: str) -> None:
    (mount / "config.yaml").write_text(body)


def test_read_yaml_file_parses_and_validates_the_mounted_file(tmp_path: Path) -> None:
    _write_config(tmp_path, "host: db.internal\nport: 6543\n")

    config = read_yaml_file(DbConfig, "config.yaml")(tmp_path)

    assert config.host == "db.internal"
    assert config.port == 6543


def test_read_yaml_file_falls_back_to_declared_defaults(tmp_path: Path) -> None:
    _write_config(tmp_path, "host: only-host\n")

    assert read_yaml_file(DbConfig, "config.yaml")(tmp_path).port == 5432


async def test_watch_config_reparses_the_mount_on_each_change(tmp_path: Path) -> None:
    _write_config(tmp_path, "host: first\n")

    async def changes(mount: Path) -> AsyncIterator[object]:
        _write_config(mount, "host: second\n")
        yield object()
        _write_config(mount, "host: third\n")
        yield object()

    source = watch_config(tmp_path, read_yaml_file(DbConfig, "config.yaml"), changes=changes)

    reloaded = await collect(source)

    assert [config.host for config in reloaded] == ["first", "second", "third"]


async def test_sampled_config_context_tracks_a_reload(tmp_path: Path) -> None:
    _write_config(tmp_path, "host: before\n")

    async def changes(mount: Path) -> AsyncIterator[object]:
        _write_config(mount, "host: after\n")
        yield object()

    source = watch_config(tmp_path, read_yaml_file(DbConfig, "config.yaml"), changes=changes)

    async with sample(source) as config:
        assert config.current().host == "before"
        await tick()
        assert config.current().host == "after"
