from __future__ import annotations

import pytest
from without_asgi.types import WebsocketBinary
from without_asgi.types import WebsocketText
from without_asgi.types import decode_websocket_data
from without_asgi.types import narrow_headers


def test_narrow_headers_names_the_actual_type_of_a_non_iterable() -> None:
    with pytest.raises(TypeError, match=r"^expected an iterable of bytes pairs, got int$"):
        narrow_headers(42)


def test_decode_websocket_data_reads_text() -> None:
    assert decode_websocket_data({"text": "hello"}) == WebsocketText(text="hello")


def test_decode_websocket_data_reads_bytes() -> None:
    assert decode_websocket_data({"bytes": b"payload"}) == WebsocketBinary(data=b"payload")


def test_decode_websocket_data_rejects_both_text_and_bytes() -> None:
    with pytest.raises(ValueError, match=r"^websocket message has both text and bytes$"):
        decode_websocket_data({"text": "hello", "bytes": b"payload"})


def test_decode_websocket_data_rejects_neither_text_nor_bytes() -> None:
    with pytest.raises(ValueError, match=r"^websocket message has neither text nor bytes$"):
        decode_websocket_data({})
