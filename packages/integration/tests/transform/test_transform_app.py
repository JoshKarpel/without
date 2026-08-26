from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from integration.transform.app import Settings
from integration.transform.app import log_connect
from integration.transform.app import mode_param
from integration.transform.app import request_digest
from integration.transform.app import text_transform_app
from integration.transform.app import transform_socket
from integration.transform.core import Mode
from integration.transform.core import TransformConfig
from without_asgi import ASGIApp
from without_asgi import Disconnect
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import RawMessage
from without_asgi import RequestBody
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_asgi import ResponseTrailers
from without_asgi import WebsocketAccept
from without_asgi import WebsocketBinary
from without_asgi import WebsocketClose
from without_asgi import WebsocketConnect
from without_asgi import WebsocketDisconnect
from without_asgi import WebsocketReceive
from without_asgi import WebsocketScope
from without_asgi import WebsocketSend
from without_asgi import WebsocketText
from without_asgi.scope import parse_http_scope
from without_asgi.scope import parse_websocket_scope
from without_configmap import read_yaml_file
from without_configmap import watch_config
from without_streams import Stream
from without_streams import collect
from without_streams import stream_from_iterable

from packages.integration.tests.helpers import drive_websocket
from packages.integration.tests.helpers import running


def _write_settings(mount: Path, default_mode: str, max_bytes: int) -> None:
    (mount / "settings.yaml").write_text(
        f"transform:\n  default_mode: {default_mode}\nhttp:\n  max_bytes: {max_bytes}\n"
    )


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
    assert isinstance(headers, (list, tuple))
    return any(header[0] == name and header[1] == value for header in headers)


async def _blocked(mount: Path) -> AsyncIterator[object]:
    await asyncio.Event().wait()
    yield object()  # pragma: no cover - the wait never returns; the yield only makes this a never-signalling change source


async def _request(
    app: ASGIApp,
    method: str,
    path: str,
    *,
    query: bytes = b"",
    body: bytes = b"",
    extensions: dict[str, dict[str, object]] | None = None,
) -> list[RawMessage]:
    scope: RawMessage = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "path": path,
        "headers": [],
        "query_string": query,
        **({"extensions": extensions} if extensions is not None else {}),
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

    async with running(app):
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

    async with running(app):
        start, _body = await _request(app, method, path)

    assert _has_header(start, b"x-clacks-overhead", b"GNU Terry Pratchett")


async def test_access_log_middleware_prints_the_request_and_the_response(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with running(app):
        await _request(app, "GET", "/missing")

    out = capsys.readouterr().out
    assert "--> GET /missing" in out
    assert "<-- GET /missing 404" in out


async def test_access_timing_middleware_reports_the_request_duration(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with running(app):
        await _request(app, "GET", "/missing")

    out = capsys.readouterr().out
    assert re.search(r"<-> GET /missing \d+\.\d+ ms", out)


async def test_request_digest_falls_back_to_a_header_without_the_trailers_extension(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)
    body = b"the quick brown fox"

    async with running(app):
        start, _body = await _request(app, "POST", "/transform", body=body)

    expected = b"sha-256=" + hashlib.sha256(body).hexdigest().encode()
    assert start.get("trailers") is not True
    assert _has_header(start, b"x-request-digest", expected)


async def test_request_digest_uses_a_trailer_when_the_server_advertises_the_extension(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)
    body = b"the quick brown fox"

    async with running(app):
        messages = await _request(app, "POST", "/transform", body=body, extensions={"http.response.trailers": {}})

    start = messages[0]
    assert start["trailers"] is True
    assert not _has_header(start, b"x-request-digest", b"sha-256=" + hashlib.sha256(body).hexdigest().encode())

    trailers = messages[-1]
    assert trailers["type"] == "http.response.trailers"
    expected = b"sha-256=" + hashlib.sha256(body).hexdigest().encode()
    assert _has_header(trailers, b"x-request-digest", expected)


async def test_query_mode_overrides_the_config_default(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with running(app):
        _start, body = await _request(app, "POST", "/transform", query=b"mode=title", body=b"the quiet part")

    assert _body(body) == b"The Quiet Part"


async def test_unknown_mode_is_a_400(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with running(app):
        start, body = await _request(app, "POST", "/transform", query=b"mode=shout", body=b"hi")

    assert _status(start) == 400
    assert _json_body(body) == {"error": "unknown mode: shout"}


async def test_body_over_the_configured_limit_is_a_413(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=4)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with running(app):
        start, body = await _request(app, "POST", "/transform", body=b"toolong")

    assert _status(start) == 413
    assert _json_body(body) == {"error": "body exceeds 4 bytes"}


async def test_non_utf8_body_is_a_400(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with running(app):
        start, body = await _request(app, "POST", "/transform", body=b"\xff\xfe")

    assert _status(start) == 400
    assert _json_body(body) == {"error": "body is not valid UTF-8"}


async def test_modes_lists_modes_and_the_current_default(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="title", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with running(app):
        start, body = await _request(app, "GET", "/modes")

    assert _status(start) == 200
    assert _json_body(body) == {"modes": ["upper", "lower", "title"], "default": "title"}


async def test_unknown_route_is_a_404(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with running(app):
        start, body = await _request(app, "GET", "/nope")

    assert _status(start) == 404
    assert _json_body(body) == {"error": "no route for GET /nope"}


async def test_handlers_pick_up_a_config_reload_mid_lifetime(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    reload_now = asyncio.Event()
    reload_landed = asyncio.Event()

    async def reloads(mount: Path) -> AsyncIterator[object]:
        await reload_now.wait()
        _write_settings(mount, default_mode="lower", max_bytes=1024)
        yield object()
        # The app's internal sample drain only pulls the next change after it has
        # published this one, so resuming here means the reload is live.
        reload_landed.set()

    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=reloads)
    app = text_transform_app(source)

    async with running(app):
        _start, before = await _request(app, "POST", "/transform", body=b"Echo")
        assert _body(before) == b"ECHO"

        reload_now.set()
        await reload_landed.wait()

        _start, after = await _request(app, "POST", "/transform", body=b"Echo")
        assert _body(after) == b"echo"


async def test_unrouted_websocket_path_is_closed_without_reading(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="upper", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    sent: list[RawMessage] = []

    async def receive() -> RawMessage:
        raise AssertionError(
            "the fallback closes without reading events"
        )  # pragma: no cover - asserts receive is never called

    async def send(message: RawMessage) -> None:
        sent.append(message)

    async with running(app):
        await app({"type": "websocket", "asgi": {"version": "3.0"}, "path": "/ws", "headers": []}, receive, send)

    assert sent == [{"type": "websocket.close", "code": 1000, "reason": ""}]


async def test_websocket_stream_transforms_each_text_frame_with_the_default_mode(tmp_path: Path) -> None:
    _write_settings(tmp_path, default_mode="title", max_bytes=1024)
    source = watch_config(tmp_path, read_yaml_file(Settings, "settings.yaml"), changes=_blocked)
    app = text_transform_app(source)

    async with running(app):
        sent = await drive_websocket(
            app,
            "/stream",
            [
                {"type": "websocket.connect"},
                {"type": "websocket.receive", "text": "the quiet part"},
                {"type": "websocket.receive", "text": "out loud"},
                {"type": "websocket.disconnect", "code": 1000},
            ],
        )

    assert sent[0]["type"] == "websocket.accept"
    assert sent[1] == {"type": "websocket.send", "text": "The Quiet Part"}
    assert sent[2] == {"type": "websocket.send", "text": "Out Loud"}


@pytest.mark.parametrize(
    ("query_string", "expected"),
    [
        (b"mode=title", "title"),
        (b"mode=title&other=1", "title"),
        (b"other=1", None),
        (b"", None),
    ],
)
def test_mode_param_reads_the_mode_parameter(query_string: bytes, expected: str | None) -> None:
    assert mode_param(query_string) == expected


def _ws_scope(path: str) -> WebsocketScope:
    return parse_websocket_scope({"type": "websocket", "asgi": {"version": "3.0"}, "path": path, "headers": []})


def _http_scope(extensions: dict[str, dict[str, object]] | None = None) -> HttpScope:
    raw: dict[str, object] = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "path": "/transform",
        "headers": [],
        "query_string": b"",
    }
    if extensions is not None:
        raw["extensions"] = extensions
    return parse_http_scope(raw)


async def test_transform_socket_transforms_text_then_closes_on_a_binary_frame() -> None:
    # Drive the processor directly with a finite stream: a binary frame closes (the
    # protocol is text-only) and the loop exits when the stream runs dry without a
    # disconnect, the connection-end path the live socket reaches only via disconnect.
    settings = Settings(transform=TransformConfig(default_mode=Mode.UPPER))
    processor = transform_socket(settings, _ws_scope("/stream"))

    outputs = await collect(
        processor(
            stream_from_iterable(
                [
                    WebsocketConnect(),
                    WebsocketReceive(WebsocketText(text="quiet")),
                    WebsocketReceive(WebsocketBinary(data=b"\x00\x01")),
                ]
            )
        )
    )

    assert outputs == [
        WebsocketAccept(),
        WebsocketSend(WebsocketText(text="QUIET")),
        WebsocketClose(code=1003, reason="text frames only"),
    ]


async def _respond_after_draining(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
    async for _event in inputs:
        pass
    yield ResponseStart(status=200, headers=((b"content-type", b"text/plain"),))
    yield ResponseBody(body=b"done", more_body=False)


def _request_digests(outputs: list[Outbound]) -> list[bytes]:
    return [
        value
        for event in outputs
        if isinstance(event, (ResponseStart, ResponseTrailers))
        for name, value in event.headers
        if name == b"x-request-digest"
    ]


@pytest.mark.parametrize("extensions", [None, {"http.response.trailers": {}}])
async def test_request_digest_hashes_only_body_chunks_and_passes_a_disconnect_through(
    extensions: dict[str, dict[str, object]] | None,
) -> None:
    # A non-body inbound event (a disconnect) must be forwarded without being hashed,
    # on both the trailer path (supports the extension) and the header fallback. The
    # digest therefore reflects only the body chunk.
    processor = request_digest(_respond_after_draining, object(), _http_scope(extensions))

    outputs = await collect(processor(stream_from_iterable([RequestBody(body=b"hi", more_body=True), Disconnect()])))

    assert _request_digests(outputs) == [b"sha-256=" + hashlib.sha256(b"hi").hexdigest().encode()]


async def test_log_connect_prints_the_path_and_forwards_every_frame(capsys: pytest.CaptureFixture[str]) -> None:
    forwarded = await collect(
        log_connect(
            stream_from_iterable([WebsocketConnect(), WebsocketDisconnect(code=1000, reason="bye")]),
            _ws_scope("/stream"),
        )
    )

    assert forwarded == [WebsocketConnect(), WebsocketDisconnect(code=1000, reason="bye")]
    assert "=== WS /stream" in capsys.readouterr().out
