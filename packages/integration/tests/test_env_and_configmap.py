from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from without_configmap import read_yaml_file
from without_configmap import watch_config
from without_env import EnvContext
from without_streams import Context
from without_streams import Processor
from without_streams import Transition
from without_streams import collect
from without_streams import from_scan
from without_streams import sample


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

        assert (await routing.updated()).upstream == "db.after"


@dataclass(frozen=True, slots=True)
class Request:
    id: int


@dataclass(frozen=True, slots=True)
class Routed:
    request_id: int
    upstream: str
    sequence: int
    within_limit: bool


def make_router(limits: Context[Limits], routing: Context[Routing]) -> Processor[Request, Routed]:
    async def step(request: Request, handled: int) -> Transition[int, Routed]:
        sequence = handled + 1
        routed = Routed(
            request_id=request.id,
            upstream=routing.current().upstream,
            sequence=sequence,
            within_limit=sequence <= limits.current().max_connections,
        )
        return Transition(state=sequence, output=routed)

    return from_scan(0, step)


async def test_processor_reads_both_contexts_and_tracks_a_reload_mid_stream(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("LIMITS_MAX_CONNECTIONS", "2")
    _write_routing(tmp_path, "db.before")

    limits: Context[Limits] = EnvContext.load(Limits)

    async def reloads(mount: Path) -> AsyncIterator[object]:
        _write_routing(mount, "db.after")
        yield object()

    routing_source = watch_config(tmp_path, read_yaml_file(Routing, "routing.yaml"), changes=reloads)

    async with sample(routing_source) as routing:

        async def requests() -> AsyncIterator[Request]:
            yield Request(id=10)
            yield Request(id=11)
            await routing.updated()  # request 12 is held back until the reload has landed
            yield Request(id=12)

        routed = await collect(make_router(limits, routing)(requests()))

    assert routed == [
        Routed(request_id=10, upstream="db.before", sequence=1, within_limit=True),
        Routed(request_id=11, upstream="db.before", sequence=2, within_limit=True),
        Routed(request_id=12, upstream="db.after", sequence=3, within_limit=False),
    ]
