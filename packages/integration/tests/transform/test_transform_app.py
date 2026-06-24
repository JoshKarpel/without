from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from integration.transform.app import text_transform_app
from integration.transform.core import Settings
from without.testing import tick
from without_asgi import ASGIApp
from without_asgi import RawMessage
from without_configmap import read_yaml_file
from without_configmap import watch_config


def _write_settings(mount: Path, default_mode: str, max_bytes: int) -> None:
    (mount / "settings.yaml").write_text(f"default_mode: {default_mode}\nmax_bytes: {max_bytes}\n")


def _status(message: RawMessage) -> int:
    status = message["status"]
    assert isinstance(status, int)
    return status


def _body(message: RawMessage) -> bytes:
    body = message["body"]
    assert isinstance(body, bytes)
    return body


def _json_body(message: RawMessage) -> object:
    return json.loads(_body(message))


def _has_header(message: RawMessage, name: bytes, value: bytes) -> bool:
    headers = message["headers"]
    assert isinstance(headers, list)
    return [name, value] in headers


async def _blocked(mount: Path) -> AsyncIterator[object]:
    await asyncio.Event().wait()
    yield object()


@asynccontextmanager
async def _running(app: ASGIApp) -> AsyncIterator[None]:
    inbox: asyncio.Queue[RawMessage] = asyncio.Queue()
    outbox: asyncio.Queue[RawMessage] = asyncio.Queue()

    async def receive() -> RawMessage:
        return await inbox.get()

    async def send(message: RawMessage) -> None:
        await outbox.put(message)

    async def drive() -> None:
        await app({"type": "lifespan", "asgi": {"version": "3.0"}}, receive, send)

    lifespan = asyncio.create_task(drive())
    await inbox.put({"type": "lifespan.startup"})
    assert await outbox.get() == {"type": "lifespan.startup.complete"}
    try:
        yield
    finally:
        await inbox.put({"type": "lifespan.shutdown"})
        assert await outbox.get() == {"type": "lifespan.shutdown.complete"}
        await lifespan


async def _request(app: ASGIApp, method: str, path: str, *, query: bytes = b"", body: bytes = b"") -> list[RawMessage]:
    scope: RawMessage = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "path": path,
        "headers": [],
        "query_string": query,
    }
    request = iter([{"type": "http.request", "body": body, "more_body": False}])

    async def receive() -> RawMessage:
        return next(request)

    sent: list[RawMessage] = []

    async def send(message: RawMessage) -> None:
        sent.append(message)

    await app(scope, receive, send)
    return sent


async def test_transforms_the_body_with_the_config_default(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with _running(app):
        start, body = await _request(app, "POST", "/transform", body=b"hello world")

    assert _status(start) == 200
    assert _body(body) == b"HELLO WORLD"


@pytest.mark.parametrize(
    ("method", "path"),
    [("POST", "/transform"), ("GET", "/modes"), ("GET", "/missing")],
)
async def test_every_route_carries_the_clacks_overhead_header(method: str, path: str, tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with _running(app):
        start, _body = await _request(app, method, path)

    assert _has_header(start, b"x-clacks-overhead", b"GNU Terry Pratchett")


async def test_access_log_middleware_prints_the_method_path_and_status(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with _running(app):
        await _request(app, "GET", "/missing")

    assert "GET /missing -> 404" in capsys.readouterr().out


async def test_query_mode_overrides_the_config_default(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with _running(app):
        _start, body = await _request(app, "POST", "/transform", query=b"mode=title", body=b"the quiet part")

    assert _body(body) == b"The Quiet Part"


async def test_unknown_mode_is_a_400(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with _running(app):
        start, body = await _request(app, "POST", "/transform", query=b"mode=shout", body=b"hi")

    assert _status(start) == 400
    assert _json_body(body) == {"error": "unknown mode: shout"}


async def test_body_over_the_configured_limit_is_a_413(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=4)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with _running(app):
        start, body = await _request(app, "POST", "/transform", body=b"toolong")

    assert _status(start) == 413
    assert _json_body(body) == {"error": "body exceeds 4 bytes"}


async def test_modes_lists_modes_and_the_current_default(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="title", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with _running(app):
        start, body = await _request(app, "GET", "/modes")

    assert _status(start) == 200
    assert _json_body(body) == {"modes": ["upper", "lower", "title"], "default": "title"}


async def test_unknown_route_is_a_404(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with _running(app):
        start, body = await _request(app, "GET", "/nope")

    assert _status(start) == 404
    assert _json_body(body) == {"error": "no route for GET /nope"}


async def test_handlers_pick_up_a_config_reload_mid_lifetime(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    reload_now = asyncio.Event()

    async def reloads(mount: Path) -> AsyncIterator[object]:
        await reload_now.wait()
        _write_settings(mount, default_mode="lower", max_bytes=1024)
        yield object()

    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=reloads)
    app = text_transform_app(source)

    async with _running(app):
        _start, before = await _request(app, "POST", "/transform", body=b"Echo")
        assert _body(before) == b"ECHO"

        reload_now.set()
        for _ in range(10):
            await tick()

        _start, after = await _request(app, "POST", "/transform", body=b"Echo")
        assert _body(after) == b"echo"


async def test_websocket_scope_is_unsupported(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async def receive() -> RawMessage:
        raise AssertionError("websocket scope should be rejected before reading events")

    async def send(message: RawMessage) -> None:
        raise AssertionError("websocket scope should send nothing")

    async with _running(app):
        with pytest.raises(NotImplementedError, match="websocket"):
            await app({"type": "websocket", "asgi": {"version": "3.0"}, "path": "/ws", "headers": []}, receive, send)
