import asyncio

import pytest
from without.testing import collect, stream
from without_integration.kv import (
    Connected,
    Delete,
    Deleted,
    Error,
    Get,
    Malformed,
    Nil,
    Reply,
    Request,
    Set,
    Stored,
    Value,
    encode_reply,
    make_responder,
    make_store,
    parse_request,
    serve,
)


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        ("GET color", Get(key="color")),
        ("get color", Get(key="color")),
        ("SET color blue", Set(key="color", value="blue")),
        ("SET motto live long and prosper", Set(key="motto", value="live long and prosper")),
        ("DEL color", Delete(key="color")),
        ("", Malformed(line="", reason="empty command")),
        ("GET", Malformed(line="GET", reason="GET takes exactly one key")),
        ("GET a b", Malformed(line="GET a b", reason="GET takes exactly one key")),
        ("SET lonely", Malformed(line="SET lonely", reason="SET takes a key and a value")),
        ("DEL a b", Malformed(line="DEL a b", reason="DEL takes exactly one key")),
        ("PING", Malformed(line="PING", reason="unknown command 'PING'")),
    ],
)
def test_parse_request_classifies_every_line(line: str, expected: Request) -> None:
    assert parse_request(line) == expected


@pytest.mark.parametrize(
    ("reply", "expected"),
    [
        (Value(value="blue"), "blue"),
        (Nil(), "(nil)"),
        (Stored(), "OK"),
        (Deleted(existed=True), "1"),
        (Deleted(existed=False), "0"),
        (Error(message="unknown command 'PING'"), "ERR unknown command 'PING'"),
    ],
)
def test_encode_reply_renders_one_line(reply: Reply, expected: str) -> None:
    assert encode_reply(reply) == expected


async def test_store_threads_the_keyspace_across_a_request_stream() -> None:
    requests: list[Request] = [
        Set(key="color", value="blue"),
        Get(key="color"),
        Get(key="missing"),
        Delete(key="color"),
        Get(key="color"),
        Delete(key="color"),
    ]

    replies = await collect(make_store()(stream(requests)))

    assert replies == [
        Stored(),
        Value(value="blue"),
        Nil(),
        Deleted(existed=True),
        Nil(),
        Deleted(existed=False),
    ]


async def test_store_turns_a_malformed_request_into_an_error_without_touching_state() -> None:
    requests: list[Request] = [
        Set(key="color", value="blue"),
        Malformed(line="PING", reason="unknown command 'PING'"),
        Get(key="color"),
    ]

    replies = await collect(make_store()(stream(requests)))

    assert replies == [Stored(), Error(message="unknown command 'PING'"), Value(value="blue")]


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
