from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.settings
import pytest
from without_asgi import ASGIApp
from without_asgi import RawScope
from without_asgi import Receive
from without_asgi import Send
from without_http.testing import AUTHORITY
from without_http.testing import served_pipe

from .test_h2_server import h2_client
from .test_h2_server import request_headers
from .test_server import echo_app
from .test_server import receive_after_done_app

# The server is driven over an in-memory pipe rather than a bound socket: every claim
# here is about the HTTP/2 codec and the server's own stream handling, which run
# unchanged either way, so a port would only add accept-path timing and ephemeral-port
# churn under xdist. The two things a pipe cannot reproduce, TLS and an abortive `RST`
# as distinct from a `FIN`, keep their tests on a real socket.

_LOGGER = "without_http.server"

_ILLEGAL_FRAME = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00"  # DATA frame on stream 0


async def chunked_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Stream the response body across several `more_body` chunks, then a final empty one."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    for part in (b"one", b"two", b"three"):
        await send({"type": "http.response.body", "body": part, "more_body": True})
    await send({"type": "http.response.body", "body": b"", "more_body": False})


async def early_hint_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Send a 103 early hint before the final response."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await send({"type": "http.response.early_hint", "links": [b"</style.css>; rel=preload"]})
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": b"hinted"})


async def start_only_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Send the response start, then return without finishing the body."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})


def contained_after_reset_app(second_sent: asyncio.Event) -> ASGIApp:
    """
    Send a first chunk, then swallow the cancellation the client's reset triggers (the
    documented escape hatch: an app may shield work from cancellation) and attempt a
    second send. That send hits the now-closed stream and is contained by the server,
    after which `second_sent` is set.
    """

    async def app(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            raise RuntimeError("this app serves only http")
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
        await send({"type": "http.response.body", "body": b"first", "more_body": True})
        with suppress(asyncio.CancelledError):
            await asyncio.Event().wait()  # cancelled by the client's reset; the app ignores it
        await send({"type": "http.response.body", "body": b"second", "more_body": True})
        second_sent.set()

    return app


def never_responds_app(entered: asyncio.Event) -> ASGIApp:
    """
    Never send a response, so an in-flight request stays open until cancelled, setting
    `entered` once the server has dispatched the request to this app.
    """

    async def app(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            raise RuntimeError("this app serves only http")
        entered.set()
        await asyncio.Event().wait()

    return app


async def server_push_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Attempt a server push, which HTTP/2 in without-http does not support."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await send({"type": "http.response.push", "path": "/pushed", "headers": []})


async def _drive(app: ASGIApp, method: str, path: str, *, body: bytes = b"") -> tuple[int, bytes, bool]:
    """Drive one h2c request to a terminal event (end, reset, or EOF) and report the outcome."""
    async with served_pipe(app) as (reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, request_headers(method, path), end_stream=not body)
        if body:
            conn.send_data(stream_id, body, end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()

        status = 0
        chunks: list[bytes] = []
        reset = False
        finished = False
        while not finished:
            data = await reader.read(65536)
            if not data:  # pragma: no cover - a terminal stream event ends the drive before EOF
                break
            for event in conn.receive_data(data):
                match event:
                    case h2.events.ResponseReceived(headers=headers):
                        status = int(next(value for name, value in headers if name == b":status"))
                    case h2.events.DataReceived(stream_id=sid, data=chunk, flow_controlled_length=length):
                        chunks.append(chunk)
                        conn.acknowledge_received_data(length, sid)
                    case h2.events.StreamEnded():
                        finished = True
                    case h2.events.StreamReset():
                        reset = True
                        finished = True
                    case _:
                        pass
            writer.write(conn.data_to_send())
            await writer.drain()
    return status, b"".join(chunks), reset


async def test_streams_a_chunked_response_over_h2() -> None:
    status, body, _reset = await _drive(chunked_app, "GET", "/c")

    assert status == 200
    assert body == b"onetwothree"


async def test_sends_an_early_hint_before_the_response_over_h2() -> None:
    status, body, _reset = await _drive(early_hint_app, "GET", "/h")

    assert status == 200
    assert body == b"hinted"


async def test_an_unfinished_response_resets_the_stream() -> None:
    status, _body, reset = await _drive(start_only_app, "GET", "/s")

    assert status == 200
    assert reset is True


async def test_receiving_after_the_request_body_yields_a_disconnect_over_h2() -> None:
    status, body, _reset = await _drive(receive_after_done_app, "POST", "/x", body=b"payload")

    assert status == 200
    assert body == b"http.disconnect"


@pytest.mark.security("a client reset mid-response cancels the stream task and contains the doomed send")
async def test_resetting_a_stream_mid_response_is_contained() -> None:
    second_sent = asyncio.Event()
    async with served_pipe(contained_after_reset_app(second_sent)) as (reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, request_headers("GET", "/big"), end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()
        got_first = False
        async with asyncio.timeout(5):
            while not got_first:
                for event in conn.receive_data(await reader.read(65536)):
                    if isinstance(event, h2.events.DataReceived):
                        got_first = True
                writer.write(conn.data_to_send())
                await writer.drain()
        # Reset the stream but keep the connection open, so the app's next chunk hits the
        # closed stream and the server contains the send_data failure.
        conn.reset_stream(stream_id, error_code=h2.errors.ErrorCodes.CANCEL)
        writer.write(conn.data_to_send())
        await writer.drain()
        async with asyncio.timeout(5):
            await second_sent.wait()  # the app attempted its post-reset send and the server contained it


async def test_a_settings_update_wakes_an_active_stream() -> None:
    async with served_pipe(echo_app) as (reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, request_headers("POST", "/u"), end_stream=False)
        writer.write(conn.data_to_send())
        await writer.drain()
        await asyncio.sleep(0.05)  # let the server open the stream
        conn.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 12345})
        writer.write(conn.data_to_send())
        await writer.drain()
        await asyncio.sleep(0.05)
        conn.send_data(stream_id, b"hello", end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()

        status = 0
        body = b""
        finished = False
        while not finished:
            data = await reader.read(65536)
            if not data:  # pragma: no cover - the response ends the stream before EOF
                break
            for event in conn.receive_data(data):
                if isinstance(event, h2.events.ResponseReceived):
                    status = int(next(value for name, value in event.headers if name == b":status"))
                elif isinstance(event, h2.events.DataReceived):
                    body += event.data
                    conn.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                elif isinstance(event, h2.events.StreamEnded):
                    finished = True
            writer.write(conn.data_to_send())
            await writer.drain()

    assert status == 200
    assert body == b"POST /u hello"


async def test_a_client_goaway_closes_the_connection() -> None:
    async with served_pipe(echo_app) as (reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, request_headers("GET", "/g"), end_stream=True)
        conn.close_connection()  # GOAWAY in the same first packet
        writer.write(conn.data_to_send())
        await writer.drain()
        with suppress(ConnectionResetError, OSError):
            while await reader.read(65536):
                pass


async def test_an_illegal_first_frame_closes_the_connection() -> None:
    async with served_pipe(echo_app) as (reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        writer.write(conn.data_to_send() + _ILLEGAL_FRAME)  # preface plus a DATA frame on stream 0
        await writer.drain()
        with suppress(ConnectionResetError, OSError):
            while await reader.read(65536):
                pass


async def test_an_unsupported_extension_over_h2_becomes_a_500() -> None:
    status, _body, _reset = await _drive(server_push_app, "GET", "/p")

    assert status == 500


async def test_resetting_an_in_flight_stream_disconnects_it() -> None:
    entered = asyncio.Event()
    async with served_pipe(never_responds_app(entered)) as (_reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, request_headers("GET", "/r"), end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()
        async with asyncio.timeout(5):
            await entered.wait()  # the server has dispatched the request; the stream is in-flight
        conn.reset_stream(stream_id, error_code=h2.errors.ErrorCodes.CANCEL)
        writer.write(conn.data_to_send())
        await writer.drain()
        await asyncio.sleep(0.05)  # let the server observe the reset


async def test_an_in_flight_stream_is_cancelled_on_server_shutdown() -> None:
    entered = asyncio.Event()
    async with served_pipe(never_responds_app(entered)) as (_reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, request_headers("GET", "/slow"), end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()
        async with asyncio.timeout(5):
            await entered.wait()  # the never-finishing stream is in-flight
    # leaving the served block cancels the in-flight stream task


def cancel_recording_app(entered: asyncio.Event, cancelled: asyncio.Event) -> ASGIApp:
    """Set `entered` once dispatched, then record a cancellation in `cancelled`."""

    async def app(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            raise RuntimeError("this app serves only http")
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    return app


@pytest.mark.security("a client RST_STREAM cancels the stream's app task promptly", cve="CVE-2023-44487")
async def test_a_client_reset_cancels_the_stream_task() -> None:
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    async with served_pipe(cancel_recording_app(entered, cancelled)) as (_reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, request_headers("GET", "/r"), end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()
        async with asyncio.timeout(5):
            await entered.wait()
        conn.reset_stream(stream_id, error_code=h2.errors.ErrorCodes.CANCEL)
        writer.write(conn.data_to_send())
        await writer.drain()
        async with asyncio.timeout(5):
            await cancelled.wait()  # the reset cancelled the app task


@pytest.mark.security("the server advertises MAX_CONCURRENT_STREAMS to bound concurrent streams", cve="CVE-2023-44487")
async def test_advertises_the_configured_max_concurrent_streams() -> None:
    async with served_pipe(echo_app, max_concurrent_streams=7) as (reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        writer.write(conn.data_to_send())
        await writer.drain()
        settings_seen = False
        async with asyncio.timeout(5):
            while not settings_seen:
                for event in conn.receive_data(await reader.read(65536)):
                    if isinstance(event, h2.events.RemoteSettingsChanged):
                        settings_seen = True
                writer.write(conn.data_to_send())
                await writer.drain()
        assert conn.remote_settings.max_concurrent_streams == 7


@pytest.mark.security("a stream-reset flood past the budget drops the connection (Rapid Reset)", cve="CVE-2023-44487")
async def test_a_reset_flood_closes_the_connection() -> None:
    async with served_pipe(echo_app, max_stream_resets=2) as (reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        writer.write(conn.data_to_send())
        await writer.drain()
        for _ in range(3):  # three resets exceed the budget of two
            stream_id = conn.get_next_available_stream_id()
            conn.send_headers(stream_id, request_headers("GET", "/x"), end_stream=True)
            conn.reset_stream(stream_id, error_code=h2.errors.ErrorCodes.CANCEL)
            writer.write(conn.data_to_send())
            await writer.drain()
        terminated = False
        async with asyncio.timeout(5):
            while not terminated:
                data = await reader.read(65536)
                if not data:  # pragma: no cover - the GOAWAY arrives before the socket EOF
                    terminated = True
                    break
                for event in conn.receive_data(data):
                    if isinstance(event, h2.events.ConnectionTerminated):
                        terminated = True
        assert terminated


@pytest.mark.security("a non-ASCII HTTP/2 :path is answered with a complete 400 rather than hanging the stream")
async def test_a_non_ascii_path_becomes_a_400(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger=_LOGGER):
        async with served_pipe(echo_app) as (reader, writer):
            conn = h2.connection.H2Connection(
                config=h2.config.H2Configuration(
                    client_side=True,
                    header_encoding=None,
                    validate_outbound_headers=False,
                    normalize_outbound_headers=False,
                )
            )
            conn.initiate_connection()
            stream_id = conn.get_next_available_stream_id()
            headers = [
                (b":method", b"GET"),
                (b":path", b"/caf\xc3\xa9\xff"),  # not decodable as ASCII
                (b":scheme", b"http"),
                (b":authority", AUTHORITY),
            ]
            conn.send_headers(stream_id, headers, end_stream=True)
            writer.write(conn.data_to_send())
            await writer.drain()
            outcome = await _collect_stream(reader, writer, conn)

    assert outcome.status == 400
    assert (b"content-type", b"text/plain; charset=utf-8") in outcome.headers
    assert outcome.body == b"bad request\n"
    assert outcome.ended is True
    assert f"Rejecting HTTP/2 stream {stream_id} with a non-ASCII :method or :path" in caplog.text


@dataclass(frozen=True, slots=True)
class _Outcome:
    """Everything one driven HTTP/2 response stream reveals to the client."""

    status: int
    headers: tuple[tuple[bytes, bytes], ...]
    body: bytes
    reset: bool
    ended: bool
    windowed: bool
    informational: tuple[tuple[tuple[bytes, bytes], ...], ...]


async def _collect_stream(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    conn: h2.connection.H2Connection,
) -> _Outcome:
    """Read one already-requested stream to its terminal event (end or reset) and report it."""
    status = 0
    headers: tuple[tuple[bytes, bytes], ...] = ()
    chunks: list[bytes] = []
    reset = False
    ended = False
    windowed = False
    informational: list[tuple[tuple[bytes, bytes], ...]] = []
    async with asyncio.timeout(5):
        while not (ended or reset):
            data = await reader.read(65536)
            if not data:  # pragma: no cover - a terminal stream event ends the drive before EOF
                break
            for event in conn.receive_data(data):
                match event:
                    case h2.events.InformationalResponseReceived(headers=hs):
                        informational.append(tuple((bytes(n), bytes(v)) for n, v in hs))
                    case h2.events.ResponseReceived(headers=hs):
                        headers = tuple((bytes(n), bytes(v)) for n, v in hs)
                        status = next((int(v) for n, v in hs if n == b":status"), 0)
                    case h2.events.DataReceived(stream_id=sid, data=chunk, flow_controlled_length=length):
                        chunks.append(chunk)
                        conn.acknowledge_received_data(length, sid)
                    case h2.events.StreamEnded():
                        ended = True
                    case h2.events.StreamReset():
                        reset = True
                    case h2.events.WindowUpdated():  # pragma: no cover - no test provokes a window update; the drive asserts it stays absent
                        windowed = True
                    case _:
                        pass
            writer.write(conn.data_to_send())
            await writer.drain()
    return _Outcome(status, headers, b"".join(chunks), reset, ended, windowed, tuple(informational))


async def _drive_full(app: ASGIApp, method: str, path: str, *, body: bytes = b"") -> _Outcome:
    async with served_pipe(app) as (reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, request_headers(method, path), end_stream=not body)
        if body:
            conn.send_data(stream_id, body, end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()
        return await _collect_stream(reader, writer, conn)


async def request_flag_reporting_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """
    Echo the exact `more_body` flag of every `http.request` chunk it receives.

    The final `END_STREAM` chunk must carry `more_body=False`, not a falsy stand-in
    like `None`: a raw app reads the flag verbatim (the server does not coerce it), so
    reporting the value distinguishes the real `False` from any other falsy value.
    """
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    flags: list[object] = []
    while True:
        message = await receive()
        if message["type"] != "http.request":  # pragma: no cover - the test never disconnects mid-body
            break
        flags.append(message["more_body"])
        if not message["more_body"]:
            break
    payload = ",".join(repr(flag) for flag in flags).encode()
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": payload})


async def test_the_final_request_chunk_reports_more_body_false() -> None:
    outcome = await _drive_full(request_flag_reporting_app, "POST", "/flags", body=b"payload")

    assert outcome.status == 200
    assert outcome.body == b"True,False"


async def silent_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Return without ever sending a response, so the server must synthesize a 500."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")


async def test_a_handler_that_sends_nothing_gets_a_plain_500() -> None:
    outcome = await _drive_full(silent_app, "GET", "/none")

    assert outcome.status == 500
    assert (b"content-type", b"text/plain; charset=utf-8") in outcome.headers
    assert outcome.body == b"internal server error\n"
    assert outcome.ended is True


async def push_then_respond_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """
    Attempt a server push, which HTTP/2 rejects with `NotImplementedError`; reflect that
    message back in a 500 so the exact rejection is observable, and fall through to a
    distinguishable 200 if the push were ever silently accepted.
    """
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    try:
        await send({"type": "http.response.push", "path": "/pushed", "headers": []})
    except NotImplementedError as exc:
        message = str(exc).encode()
        await send({"type": "http.response.start", "status": 500, "headers": [(b"content-type", b"text/plain")]})
        await send({"type": "http.response.body", "body": message})
        return
    # the push always raises, so this silent-accept fall-through never runs
    await send(
        {"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]}
    )  # pragma: no cover
    await send({"type": "http.response.body", "body": b"push silently accepted"})  # pragma: no cover


async def test_a_server_push_raises_a_named_not_implemented_error() -> None:
    outcome = await _drive_full(push_then_respond_app, "GET", "/p")

    assert outcome.status == 500
    assert outcome.body == b"ServerPush is not supported over HTTP/2"


async def partial_then_return_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Start a response and stream one non-final chunk, then return without ending it."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": b"partial", "more_body": True})


async def test_a_response_left_unfinished_resets_the_stream() -> None:
    outcome = await _drive_full(partial_then_return_app, "GET", "/u")

    assert outcome.status == 200
    assert outcome.body == b"partial"
    assert outcome.reset is True


async def test_an_early_hint_arrives_as_a_103_informational_response() -> None:
    outcome = await _drive_full(early_hint_app, "GET", "/h")

    assert outcome.status == 200
    assert outcome.body == b"hinted"
    assert ((b":status", b"103"), (b"link", b"</style.css>; rel=preload")) in outcome.informational


async def test_a_get_receives_no_spurious_window_update() -> None:
    outcome = await _drive_full(echo_app, "GET", "/w")

    assert outcome.status == 200
    assert outcome.windowed is False


async def test_resets_within_the_budget_leave_the_connection_serving() -> None:
    async with served_pipe(echo_app, max_stream_resets=2) as (reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        writer.write(conn.data_to_send())
        await writer.drain()
        for _ in range(2):  # exactly the budget of two resets, so the connection survives
            reset_id = conn.get_next_available_stream_id()
            conn.send_headers(reset_id, request_headers("GET", "/x"), end_stream=True)
            conn.reset_stream(reset_id, error_code=h2.errors.ErrorCodes.CANCEL)
            writer.write(conn.data_to_send())
            await writer.drain()

        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, request_headers("GET", "/ok"), end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()
        status = 0
        body = b""
        settled = False
        async with asyncio.timeout(5):
            while not settled:
                data = await reader.read(65536)
                if not data:  # pragma: no cover - a served response or a GOAWAY ends the drive first
                    break
                for event in conn.receive_data(data):
                    if isinstance(event, h2.events.ResponseReceived) and event.stream_id == stream_id:
                        status = next((int(v) for n, v in event.headers if n == b":status"), 0)
                    elif isinstance(event, h2.events.DataReceived) and event.stream_id == stream_id:
                        body += event.data
                        conn.acknowledge_received_data(event.flow_controlled_length, event.stream_id)
                    elif (isinstance(event, h2.events.StreamEnded) and event.stream_id == stream_id) or isinstance(
                        event, h2.events.ConnectionTerminated
                    ):
                        settled = True
                writer.write(conn.data_to_send())
                await writer.drain()

    assert status == 200
    assert body == b"GET /ok "


@pytest.mark.security("a reset flood past the budget sends a GOAWAY with ENHANCE_YOUR_CALM", cve="CVE-2023-44487")
async def test_a_reset_flood_sends_goaway_enhance_your_calm(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger=_LOGGER):
        async with served_pipe(echo_app, max_stream_resets=2) as (reader, writer):
            conn = h2_client()
            conn.initiate_connection()
            writer.write(conn.data_to_send())
            await writer.drain()
            for _ in range(3):  # three resets exceed the budget of two
                stream_id = conn.get_next_available_stream_id()
                conn.send_headers(stream_id, request_headers("GET", "/x"), end_stream=True)
                conn.reset_stream(stream_id, error_code=h2.errors.ErrorCodes.CANCEL)
                writer.write(conn.data_to_send())
                await writer.drain()
            error_code = None
            terminated = False
            async with asyncio.timeout(
                5
            ):  # pragma: no branch - the block's last statement; coverage misreads its exit arcs
                while not terminated:
                    data = await reader.read(65536)
                    if not data:  # pragma: no cover - the GOAWAY arrives before the socket EOF
                        break
                    for event in conn.receive_data(data):
                        if isinstance(event, h2.events.ConnectionTerminated):
                            error_code = event.error_code
                            terminated = True

    assert terminated  # a real GOAWAY frame, not merely a socket EOF
    assert error_code == h2.errors.ErrorCodes.ENHANCE_YOUR_CALM
    assert "Closing HTTP/2 connection after 3 stream resets exceeded the budget of 2" in caplog.text


@pytest.mark.security("a client RST_STREAM logs and cancels the stream's app task", cve="CVE-2023-44487")
async def test_a_client_reset_logs_and_cancels_the_stream_task(caplog: pytest.LogCaptureFixture) -> None:
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    with caplog.at_level(logging.INFO, logger=_LOGGER):
        async with served_pipe(cancel_recording_app(entered, cancelled)) as (_reader, writer):
            conn = h2_client()
            conn.initiate_connection()
            stream_id = conn.get_next_available_stream_id()
            conn.send_headers(stream_id, request_headers("GET", "/r"), end_stream=True)
            writer.write(conn.data_to_send())
            await writer.drain()
            async with asyncio.timeout(5):
                await entered.wait()
            conn.reset_stream(stream_id, error_code=h2.errors.ErrorCodes.CANCEL)
            writer.write(conn.data_to_send())
            await writer.drain()
            async with asyncio.timeout(
                5
            ):  # pragma: no branch - the block's last statement; coverage misreads its exit arcs
                await cancelled.wait()  # the reset cancelled the app task

    assert cancelled.is_set()
    assert f"Cancelling HTTP/2 stream {stream_id} after a client reset" in caplog.text


async def test_an_in_flight_stream_task_is_cancelled_on_server_shutdown() -> None:
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    async with served_pipe(cancel_recording_app(entered, cancelled)) as (_reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        stream_id = conn.get_next_available_stream_id()
        conn.send_headers(stream_id, request_headers("GET", "/slow"), end_stream=True)
        writer.write(conn.data_to_send())
        await writer.drain()
        async with asyncio.timeout(5):
            await entered.wait()  # the never-finishing stream is in-flight
    # leaving the served block must cancel the tracked stream task, not a stand-in
    async with asyncio.timeout(5):
        await cancelled.wait()


async def test_a_client_that_stays_idle_is_closed_after_the_idle_timeout() -> None:
    async with served_pipe(echo_app, idle_timeout=timedelta(seconds=0.2)) as (reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        writer.write(conn.data_to_send())  # send the preface, then stay idle
        await writer.drain()
        async with asyncio.timeout(5):
            while await reader.read(65536):  # drains the server preface, then blocks until the idle close
                pass


async def test_an_illegal_first_frame_sends_a_goaway() -> None:
    async with served_pipe(echo_app) as (reader, writer):
        conn = h2_client()
        conn.initiate_connection()
        writer.write(conn.data_to_send() + _ILLEGAL_FRAME)  # preface plus a DATA frame on stream 0
        await writer.drain()
        terminated = False
        async with asyncio.timeout(5):
            while not terminated:
                data = await reader.read(65536)
                if not data:  # pragma: no cover - the GOAWAY arrives before the socket EOF
                    break
                for event in conn.receive_data(data):
                    if isinstance(event, h2.events.ConnectionTerminated):
                        terminated = True

    assert terminated  # the protocol error is answered with a GOAWAY, not a bare socket close
