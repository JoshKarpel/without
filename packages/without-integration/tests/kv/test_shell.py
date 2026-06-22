import asyncio
from contextlib import AbstractAsyncContextManager

from without import Fold, from_fold
from without.testing import stream
from without_env import EnvContext
from without_integration.kv import (
    EMPTY_STORE,
    Connected,
    Get,
    Nil,
    Reply,
    Request,
    ServeConfig,
    Set,
    Store,
    Stored,
    Value,
    apply,
    encode_reply,
    make_responder,
    parse_request,
    serve,
)


def _serve(
    consumer: Fold[Connected[Request, Reply], object] | None = None,
    *,
    drain_timeout: float = 5.0,
) -> AbstractAsyncContextManager[asyncio.Server]:
    config = EnvContext(settings=ServeConfig(drain_timeout=drain_timeout))
    return serve(consumer or make_responder(), decode=parse_request, encode=encode_reply, config=config)


async def test_responder_sends_each_reply_on_the_events_own_channel() -> None:
    sent: list[Reply] = []

    async def send(reply: Reply) -> None:
        sent.append(reply)

    events: list[Connected[Request, Reply]] = [
        Connected(send=send, payload=Set(key="color", value="blue")),
        Connected(send=send, payload=Get(key="color")),
        Connected(send=send, payload=Get(key="missing")),
    ]

    final = await make_responder()(stream(events))

    assert sent == [Stored(), Value(value="blue"), Nil()]
    assert final.entries == {"color": "blue"}


async def _roundtrip(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, command: str) -> str:
    writer.write(f"{command}\n".encode())
    await writer.drain()
    return (await reader.readline()).decode().rstrip("\n")


async def test_server_answers_a_clients_requests_over_the_wire() -> None:
    async with _serve() as running:
        host, port = running.sockets[0].getsockname()[:2]
        reader, writer = await asyncio.open_connection(host, port)
        try:
            assert await _roundtrip(reader, writer, "SET greeting hello there") == "OK"
            assert await _roundtrip(reader, writer, "GET greeting") == "hello there"
            assert await _roundtrip(reader, writer, "GET absent") == "(nil)"
            assert await _roundtrip(reader, writer, "DEL greeting") == "1"
            assert await _roundtrip(reader, writer, "GET greeting") == "(nil)"
            assert await _roundtrip(reader, writer, "PING") == "ERR unknown command 'PING'"
        finally:
            writer.close()
            await writer.wait_closed()


async def test_server_shares_one_long_lived_keyspace_across_connections() -> None:
    async with _serve() as running:
        host, port = running.sockets[0].getsockname()[:2]
        writer_reader, writer_writer = await asyncio.open_connection(host, port)
        reader_reader, reader_writer = await asyncio.open_connection(host, port)
        try:
            assert await _roundtrip(writer_reader, writer_writer, "SET shared mango") == "OK"
            assert await _roundtrip(reader_reader, reader_writer, "GET shared") == "mango"
        finally:
            for writer in (writer_writer, reader_writer):
                writer.close()
                await writer.wait_closed()


async def test_reply_reaches_a_client_that_half_closes_after_sending() -> None:
    # The client sends a request, half-closes its write side (the server's reader hits EOF), then waits
    # for the reply. The reply is produced asynchronously by the shared consumer, so the connection must
    # not close on EOF or the reply is dropped.
    async with _serve() as running:
        host, port = running.sockets[0].getsockname()[:2]
        reader, writer = await asyncio.open_connection(host, port)
        writer.write(b"SET fruit mango\n")
        await writer.drain()
        writer.write_eof()

        assert (await reader.readline()).decode().rstrip("\n") == "OK"
        writer.close()
        await writer.wait_closed()


async def test_shutdown_drains_an_enqueued_backlog() -> None:
    # A consumer slow enough that requests are still queued when shutdown begins; draining must answer the
    # whole backlog rather than dropping it when the context exits.
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
    assert replies == ["OK", "OK", "OK"]
    writer.close()
    await writer.wait_closed()


async def test_shutdown_is_bounded_when_an_idle_client_never_disconnects() -> None:
    # An idle connection leaves a reader parked with no traffic; shutdown must cut it off (bounded by the
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
