import pytest
from pydantic_settings import BaseSettings, SettingsConfigDict
from without import Context, Registry
from without_env import EnvContext


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="APP_")

    greeting: str


def test_env_context_supplies_config_to_a_declared_graph(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_GREETING", "howdy")

    config: Context[AppConfig] = EnvContext.load(AppConfig)

    registry = Registry()

    @registry.node
    def greet(config: object, request: object) -> object: ...

    graph = registry.graph()

    assert ("config", "greet") in graph.edges()
    assert "config([config])" in graph.to_mermaid()
    assert config.current().greeting == "howdy"
