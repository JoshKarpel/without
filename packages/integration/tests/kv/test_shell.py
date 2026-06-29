import asyncio
import gc
import socket
import struct
from contextlib import AbstractAsyncContextManager

from integration.kv import EMPTY_STORE
from integration.kv import Connected
from integration.kv import Get
from integration.kv import Nil
from integration.kv import Reply
from integration.kv import Request
from integration.kv import ServeConfig
from integration.kv import Set
from integration.kv import Store
from integration.kv import Stored
from integration.kv import Value
from integration.kv import apply
from integration.kv import make_keyspace
from integration.kv import make_session
from integration.kv import serve
from without import Fold
from without import Stream
from without import from_fold
from without import stream
from without_env import EnvContext


def _serve(
    consumer: Fold[Connected[Request, Reply], object] | None = None,
    *,
    drain_timeout: float = 5.0,
    idle_timeout: float | None = None,
) -> AbstractAsyncContextManager[asyncio.Server]:
    config = EnvContext(settings=ServeConfig(drain_timeout=drain_timeout, idle_timeout=idle_timeout))
    return serve(consumer or make_keyspace(), make_session, config=config)


async def test_keyspace_sends_each_reply_on_the_events_own_channel() -> None:
    sent: list[Reply] = []

    async def send(reply: Reply) -> None:
        sent.append(reply)

    events: list[Connected[Request, Reply]] = [
        Connected(send=send, payload=Set(key="color", value="blue")),
        Connected(send=send, payload=Get(key="color")),
        Connected(send=send, payload=Get(key="missing")),
    ]

    final = await make_keyspace()(stream(events))

    assert sent == [Stored(), Value(value="blue"), Nil()]
    assert final.entries == {"color": "blue"}


async def _roundtrip(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, command: str) -> str:
    writer.write(f"{command}\n".encode())
    await writer.drain()
    return (await reader.readline()).decode().rstrip("\n")


async def test_server_answers_a_clients_requests_over_the_wire() -> None:
    # Each reply is prefixed with this connection's request number (connection-scoped state).
    async with _serve() as running:
        host, port = running.sockets[0].getsockname()[:2]
        reader, writer = await asyncio.open_connection(host, port)
        try:
            assert await _roundtrip(reader, writer, "SET greeting hello there") == "1 OK"
            assert await _roundtrip(reader, writer, "GET greeting") == "2 hello there"
            assert await _roundtrip(reader, writer, "GET absent") == "3 (nil)"
            assert await _roundtrip(reader, writer, "DEL greeting") == "4 1"
            assert await _roundtrip(reader, writer, "GET greeting") == "5 (nil)"
            assert await _roundtrip(reader, writer, "PING") == "6 ERR unknown command 'PING'"
        finally:
            writer.close()
            await writer.wait_closed()


async def test_counter_is_connection_scoped_while_keyspace_is_shared() -> None:
    # The request number resets per connection (threaded in each session's own from_scan), but the store
    # is common to all (the single keyspace fold): the second connection counts from 1 yet sees the first's write.
    async with _serve() as running:
        host, port = running.sockets[0].getsockname()[:2]
        first_reader, first_writer = await asyncio.open_connection(host, port)
        second_reader, second_writer = await asyncio.open_connection(host, port)
        try:
            assert await _roundtrip(first_reader, first_writer, "SET shared mango") == "1 OK"
            assert await _roundtrip(first_reader, first_writer, "GET shared") == "2 mango"
            assert await _roundtrip(second_reader, second_writer, "GET shared") == "1 mango"
        finally:
            for writer in (first_writer, second_writer):
                writer.close()
                await writer.wait_closed()


async def test_reply_reaches_a_client_that_half_closes_after_sending() -> None:
    # The client sends a request, half-closes its write side (the server's reader hits EOF), then waits for
    # the reply. The connection ends when its line stream runs dry, after the in-flight round trip completes.
    async with _serve() as running:
        host, port = running.sockets[0].getsockname()[:2]
        reader, writer = await asyncio.open_connection(host, port)
        writer.write(b"SET fruit mango\n")
        await writer.drain()
        writer.write_eof()

        assert (await reader.readline()).decode().rstrip("\n") == "1 OK"
        writer.close()
        await writer.wait_closed()


async def test_shutdown_drains_inflight_requests() -> None:
    # A consumer slow enough that the session is still working through its lines when shutdown begins;
    # draining must answer every line rather than dropping them when the context exits.
    async def respond(event: Connected[Request, Reply], store: Store) -> Store:
        await asyncio.sleep(0.05)
        transition = await apply(event.payload, store)
        await event.send(transition.output)
        return transition.state

    consumer: Fold[Connected[Request, Reply], Store] = from_fold(EMPTY_STORE, respond)
    commands = ["SET a 1", "SET b 2", "SET c 3"]
    async with _serve(consumer, drain_timeout=5.0) as running:
        host, port = running.sockets[0].getsockname()[:2]
        reader, writer = await asyncio.open_connection(host, port)
        for command in commands:
            writer.write(f"{command}\n".encode())
        await writer.drain()
        writer.write_eof()

    replies = [(await reader.readline()).decode().rstrip("\n") for _ in commands]
    assert replies == ["1 OK", "2 OK", "3 OK"]
    writer.close()
    await writer.wait_closed()


async def test_a_client_reset_does_not_disturb_other_connections() -> None:
    # A client that sends a command then abortively resets (RST) makes that session's read raise
    # ConnectionResetError; it must be contained, leaving no unhandled loop error and the server serving.
    loop = asyncio.get_running_loop()
    loop_errors: list[dict[str, object]] = []
    loop.set_exception_handler(lambda _loop, context: loop_errors.append(context))
    try:
        async with _serve() as running:
            host, port = running.sockets[0].getsockname()[:2]
            rude = socket.create_connection((host, port))
            rude.sendall(b"SET k v\n")
            await asyncio.sleep(0.05)  # let the server apply the SET, then reset before reading the reply
            rude.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))  # RST on close
            rude.close()

            reader, writer = await asyncio.open_connection(host, port)
            try:
                assert await _roundtrip(reader, writer, "GET k") == "1 v"  # shared store survived; fresh counter
            finally:
                writer.close()
                await writer.wait_closed()
        gc.collect()  # force any unretrieved session-task exception to reach the loop handler
    finally:
        loop.set_exception_handler(None)

    assert loop_errors == []


async def test_idle_connection_is_reaped_after_the_idle_timeout() -> None:
    # A client that connects and stays silent past idle_timeout is reaped: the session's read times out,
    # ends the stream, and the writer closes, so the client observes EOF without ever sending anything.
    async with _serve(idle_timeout=0.1) as running:
        host, port = running.sockets[0].getsockname()[:2]
        reader, writer = await asyncio.open_connection(host, port)
        try:
            assert await reader.read() == b""  # server closes the idle connection; read returns EOF
        finally:
            writer.close()
            await writer.wait_closed()


async def test_idle_timeout_does_not_disturb_an_active_client() -> None:
    # The idle clock is per-read: a client that keeps sending within the window is never reaped.
    async with _serve(idle_timeout=0.2) as running:
        host, port = running.sockets[0].getsockname()[:2]
        reader, writer = await asyncio.open_connection(host, port)
        try:
            assert await _roundtrip(reader, writer, "SET k v") == "1 OK"
            await asyncio.sleep(0.1)  # under the window
            assert await _roundtrip(reader, writer, "GET k") == "2 v"
        finally:
            writer.close()
            await writer.wait_closed()


async def test_shutdown_cancels_a_session_when_the_consumer_wedges() -> None:
    # A consumer that never answers leaves a session parked awaiting its reply. Shutdown must terminate
    # (bounded) and cancel the stuck session rather than leave it leaked as a pending task.
    async def wedged(events: Stream[Connected[Request, Reply]]) -> object:
        async for _event in events:
            await asyncio.Event().wait()  # never reply; block forever on the first request
        return None  # pragma: no cover - the consumer wedges on its first event; this only types the no-events path

    baseline = asyncio.all_tasks()
    async with asyncio.timeout(5.0):
        async with _serve(wedged, drain_timeout=0.1) as running:
            host, port = running.sockets[0].getsockname()[:2]
            _, writer = await asyncio.open_connection(host, port)
            writer.write(b"GET k\n")
            await writer.drain()
            await asyncio.sleep(0.05)  # let the session enqueue its request and park awaiting the reply
        leaked = asyncio.all_tasks() - baseline

    writer.close()
    await writer.wait_closed()
    assert leaked == set()


async def test_shutdown_is_bounded_when_an_idle_client_never_disconnects() -> None:
    # An idle connection leaves a session parked with no lines; shutdown must cut it off (bounded by the
    # drain budget) rather than block forever waiting for the client to leave.
    writer: asyncio.StreamWriter | None = None
    async with asyncio.timeout(5.0):
        async with _serve(drain_timeout=0.2) as running:
            host, port = running.sockets[0].getsockname()[:2]
            _, writer = await asyncio.open_connection(host, port)
            await asyncio.sleep(0.05)

    assert writer is not None
    writer.close()
    await writer.wait_closed()
