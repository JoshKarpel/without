import pytest
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from without_env import EnvContext
from without_streams import Context


class ServerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SVC_")

    host: str
    port: int = 8080


def test_load_parses_values_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SVC_HOST", "db.internal")
    monkeypatch.setenv("SVC_PORT", "5433")

    context = EnvContext.load(ServerSettings)

    assert context.current().host == "db.internal"
    assert context.current().port == 5433


def test_declared_default_is_used_when_the_variable_is_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SVC_HOST", "lonely-host")
    monkeypatch.delenv("SVC_PORT", raising=False)

    assert EnvContext.load(ServerSettings).current().port == 8080


def test_env_context_satisfies_the_core_context_protocol(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SVC_HOST", "structural")

    assert isinstance(EnvContext.load(ServerSettings), Context)
