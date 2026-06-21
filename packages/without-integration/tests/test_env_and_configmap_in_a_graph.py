from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from without import Context, Registry, sample
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


async def test_env_and_configmap_contexts_feed_one_declared_graph(
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

    registry = Registry()

    @registry.node
    def handle(limits: object, routing: object, request: object) -> object: ...

    edges = registry.graph().edges()

    assert ("limits", "handle") in edges
    assert ("routing", "handle") in edges

    assert limits.current().max_connections == 32

    async with sample(routing_source) as routing:
        assert routing.current().upstream == "db.before"
        await tick()
        assert routing.current().upstream == "db.after"
