from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from without.testing import tick
from without_asgi import ASGIApp, Message
from without_configmap import read_yaml_file, watch_config
from without_integration.flags.app import feature_flags_app
from without_integration.flags.core import Flags


def _write_flags(mount: Path, flags: dict[str, bool]) -> None:
    body = "\n".join(f"  {name}: {str(enabled).lower()}" for name, enabled in flags.items())
    (mount / "flags.yaml").write_text(f"flags:\n{body}\n")


def _status(message: Message) -> int:
    status = message["status"]
    assert isinstance(status, int)
    return status


def _json_body(message: Message) -> object:
    body = message["body"]
    assert isinstance(body, bytes)
    return json.loads(body)


def _has_header(message: Message, name: bytes, value: bytes) -> bool:
    headers = message["headers"]
    assert isinstance(headers, list)
    return [name, value] in headers


async def _blocked(mount: Path) -> AsyncIterator[object]:
    await asyncio.Event().wait()
    yield object()


@asynccontextmanager
async def _running(app: ASGIApp) -> AsyncIterator[None]:
    inbox: asyncio.Queue[Message] = asyncio.Queue()
    outbox: asyncio.Queue[Message] = asyncio.Queue()

    async def receive() -> Message:
        return await inbox.get()

    async def send(message: Message) -> None:
        await outbox.put(message)

    async def drive() -> None:
        await app({"type": "lifespan"}, receive, send)

    lifespan = asyncio.create_task(drive())
    await inbox.put({"type": "lifespan.startup"})
    assert await outbox.get() == {"type": "lifespan.startup.complete"}
    try:
        yield
    finally:
        await inbox.put({"type": "lifespan.shutdown"})
        assert await outbox.get() == {"type": "lifespan.shutdown.complete"}
        await lifespan


async def _get(app: ASGIApp, path: str, query: bytes = b"") -> list[Message]:
    scope: Message = {"type": "http", "method": "GET", "path": path, "headers": [], "query_string": query}
    request = iter([{"type": "http.request", "body": b"", "more_body": False}])

    async def receive() -> Message:
        return next(request)

    sent: list[Message] = []

    async def send(message: Message) -> None:
        sent.append(message)

    await app(scope, receive, send)
    return sent


async def test_serves_current_flags_with_the_middleware_header(tmp_path: Path) -> None:
    _write_flags(tmp_path, {"dark_mode": True})
    source = watch_config(tmp_path, read_yaml_file(Flags, "flags.yaml"), changes=_blocked)
    app = feature_flags_app(source)

    async with _running(app):
        start, body = await _get(app, "/flags")

    assert _status(start) == 200
    assert _has_header(start, b"x-flags-source", b"configmap")
    assert _json_body(body) == {"flags": {"dark_mode": True}}


async def test_serves_a_single_flag_by_name(tmp_path: Path) -> None:
    _write_flags(tmp_path, {"dark_mode": True, "beta": False})
    source = watch_config(tmp_path, read_yaml_file(Flags, "flags.yaml"), changes=_blocked)
    app = feature_flags_app(source)

    async with _running(app):
        _start, body = await _get(app, "/flag", query=b"name=beta")

    assert _json_body(body) == {"name": "beta", "enabled": False}


async def test_missing_name_parameter_is_a_400(tmp_path: Path) -> None:
    _write_flags(tmp_path, {"dark_mode": True})
    source = watch_config(tmp_path, read_yaml_file(Flags, "flags.yaml"), changes=_blocked)
    app = feature_flags_app(source)

    async with _running(app):
        start, body = await _get(app, "/flag")

    assert _status(start) == 400
    assert _json_body(body) == {"error": "missing 'name' query parameter"}


async def test_unknown_route_is_a_404(tmp_path: Path) -> None:
    _write_flags(tmp_path, {"dark_mode": True})
    source = watch_config(tmp_path, read_yaml_file(Flags, "flags.yaml"), changes=_blocked)
    app = feature_flags_app(source)

    async with _running(app):
        start, body = await _get(app, "/nope")

    assert _status(start) == 404
    assert _json_body(body) == {"error": "no route for GET /nope"}


async def test_handlers_pick_up_a_config_reload_mid_lifetime(tmp_path: Path) -> None:
    _write_flags(tmp_path, {"dark_mode": True})
    reload_now = asyncio.Event()

    async def reloads(mount: Path) -> AsyncIterator[object]:
        await reload_now.wait()
        _write_flags(mount, {"dark_mode": False, "beta": True})
        yield object()

    source = watch_config(tmp_path, read_yaml_file(Flags, "flags.yaml"), changes=reloads)
    app = feature_flags_app(source)

    async with _running(app):
        _start, before = await _get(app, "/flags")
        assert _json_body(before) == {"flags": {"dark_mode": True}}

        reload_now.set()
        for _ in range(10):
            await tick()

        _start, after = await _get(app, "/flags")
        assert _json_body(after) == {"flags": {"dark_mode": False, "beta": True}}


async def test_websocket_scope_is_unsupported(tmp_path: Path) -> None:
    _write_flags(tmp_path, {"dark_mode": True})
    source = watch_config(tmp_path, read_yaml_file(Flags, "flags.yaml"), changes=_blocked)
    app = feature_flags_app(source)

    async def receive() -> Message:
        raise AssertionError("websocket scope should be rejected before reading events")

    async def send(message: Message) -> None:
        raise AssertionError("websocket scope should send nothing")

    async with _running(app):
        with pytest.raises(NotImplementedError, match="websocket"):
            await app({"type": "websocket", "path": "/ws"}, receive, send)
