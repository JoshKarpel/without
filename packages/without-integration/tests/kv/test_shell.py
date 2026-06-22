import asyncio

from without.testing import stream
from without_integration.kv import (
    Connected,
    Get,
    Nil,
    Reply,
    Request,
    Set,
    Stored,
    Value,
    encode_reply,
    make_responder,
    parse_request,
    serve,
)


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
    async with serve(make_responder(), decode=parse_request, encode=encode_reply) as running:
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
    async with serve(make_responder(), decode=parse_request, encode=encode_reply) as running:
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
