import pytest
from without.testing import collect, stream
from without_integration.kv import (
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
    make_store,
    parse_request,
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
