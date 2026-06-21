from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from without import Context, sample
from without.testing import tick
from without_configmap import read_yaml_file, watch_config
from without_env import EnvContext


class Limits(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LIMITS_")

    max_connections: int


class Routing(BaseModel):
    upstream: str


def _write_routing(mount: Path, upstream: str) -> None:
    (mount / "routing.yaml").write_text(f"upstream: {upstream}\n")


async def test_static_and_reloading_contexts_coexist(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("LIMITS_MAX_CONNECTIONS", "32")
    _write_routing(tmp_path, "db.before")

    limits: Context[Limits] = EnvContext.load(Limits)

    async def reloads(mount: Path) -> AsyncIterator[object]:
        _write_routing(mount, "db.after")
        yield object()

    routing_source = watch_config(tmp_path, read_yaml_file(Routing, "routing.yaml"), changes=reloads)

    async with sample(routing_source) as routing:
        assert isinstance(limits, Context)
        assert isinstance(routing, Context)

        assert limits.current().max_connections == 32
        assert routing.current().upstream == "db.before"

        await tick()
        assert routing.current().upstream == "db.after"
