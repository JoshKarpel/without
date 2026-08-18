from __future__ import annotations

import bz2
import gzip
from collections.abc import AsyncGenerator
from collections.abc import AsyncIterator
from compression import zstd
from datetime import timedelta

import brotli
import pytest
from without_asgi import ASGIApp
from without_asgi import RawScope
from without_asgi import Receive
from without_asgi import Send
from without_asgi import parse_http_scope
from without_http import DEFAULT_DECOMPRESSORS
from without_http import ClientRequest
from without_http import ClientResponse
from without_http import ConnectionPool
from without_http import ResponseBody
from without_http import ResponseHead
from without_http import ResponseTrailers
from without_http import Timeout
from without_http import brotli_compress
from without_http import compressing
from without_http import decompress
from without_http import gzip_compress
from without_http import request
from without_http import serving
from without_http import zstd_compress
from without_http.testing import mock_client


async def _read_body(receive: Receive) -> bytes:
    body = b""
    more = True
    while more:
        message = await receive()
        if message["type"] == "http.disconnect":  # pragma: no cover - clients here never disconnect mid-body
            break
        chunk = message.get("body", b"")
        assert isinstance(chunk, bytes)
        body += chunk
        more = bool(message.get("more_body", False))
    return body


async def report_app(scope: RawScope, receive: Receive, send: Send) -> None:
    """Answer with the request's coding headers and its body, gunzipped when so encoded."""
    if scope["type"] != "http":
        raise RuntimeError("this app serves only http")
    head = parse_http_scope(scope)

    def header(name: bytes) -> bytes:
        return next((value for header_name, value in head.headers if header_name == name), b"absent")

    body = await _read_body(receive)
    if header(b"content-encoding") == b"gzip":
        body = gzip.decompress(body)
    elif header(b"content-encoding") == b"zstd":
        body = zstd.decompress(body)
    elif header(b"content-encoding") == b"bzip2":
        body = bz2.decompress(body)
    elif header(b"content-encoding") == b"br":
        body = brotli.decompress(body)
    payload = (
        f"encoding={header(b'content-encoding').decode()}"
        f" length={header(b'content-length').decode()}"
        f" accept={header(b'accept-encoding').decode()}"
        f" type={header(b'content-type').decode()}"
        f" body={body.decode()}"
    ).encode()
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": payload})


def encoded_app(encoding: bytes, chunks: tuple[bytes, ...]) -> ASGIApp:
    """An app answering with `chunks` as the streamed body under `content-encoding: encoding`."""

    async def app(scope: RawScope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            raise RuntimeError("this app serves only http")
        await _read_body(receive)
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-encoding", encoding)]})
        for chunk in chunks:
            await send({"type": "http.response.body", "body": chunk, "more_body": True})
        await send({"type": "http.response.body", "body": b""})

    return app


async def test_gzip_compress_gzips_the_request_body_seen_server_side() -> None:
    async with serving(report_app) as server, ConnectionPool() as pool:
        client = gzip_compress()(pool)
        url = f"http://{server.host}:{server.port}/upload"
        async with request(client, "POST", url, body=b"squeeze me") as (head, body):
            assert head.status == 200
            assert await body.read() == b"encoding=gzip length=absent accept=absent type=absent body=squeeze me"


async def test_gzip_compress_streams_a_request_body_over_http2() -> None:
    async def chunks() -> AsyncIterator[bytes]:
        yield b"first part, "
        yield b"second part"

    async with serving(report_app) as server, ConnectionPool(force_http2_cleartext=True) as pool:
        client = gzip_compress()(pool)
        url = f"http://{server.host}:{server.port}/upload"
        async with request(client, "POST", url, body=chunks()) as (_head, body):
            assert (
                await body.read()
                == b"encoding=gzip length=absent accept=absent type=absent body=first part, second part"
            )


async def test_brotli_compress_encodes_the_request_body_seen_server_side() -> None:
    async with serving(report_app) as server, ConnectionPool() as pool:
        client = brotli_compress()(pool)
        url = f"http://{server.host}:{server.port}/upload"
        async with request(client, "POST", url, body=b"brotli squeeze") as (head, body):
            assert head.status == 200
            assert await body.read() == b"encoding=br length=absent accept=absent type=absent body=brotli squeeze"


async def test_zstd_compress_encodes_the_request_body_seen_server_side() -> None:
    async with serving(report_app) as server, ConnectionPool() as pool:
        client = zstd_compress()(pool)
        url = f"http://{server.host}:{server.port}/upload"
        async with request(client, "POST", url, body=b"zstandard squeeze") as (head, body):
            assert head.status == 200
            assert await body.read() == b"encoding=zstd length=absent accept=absent type=absent body=zstandard squeeze"


async def test_gzip_compress_skips_a_bodyless_request() -> None:
    async with serving(report_app) as server, ConnectionPool() as pool:
        client = gzip_compress()(pool)
        async with request(client, "GET", f"http://{server.host}:{server.port}/items") as (_head, body):
            assert await body.read() == b"encoding=absent length=absent accept=absent type=absent body="


async def test_gzip_compress_leaves_an_already_encoded_body_alone() -> None:
    pre_squeezed = gzip.compress(b"already encoded by the caller")
    async with serving(report_app) as server, ConnectionPool() as pool:
        client = gzip_compress()(pool)
        url = f"http://{server.host}:{server.port}/upload"
        async with request(client, "POST", url, headers=((b"content-encoding", b"gzip"),), body=pre_squeezed) as (
            _head,
            body,
        ):
            # A re-compressed body would decode to gzip bytes rather than the text, and
            # the rewrite would have dropped the content-length.
            expected_length = str(len(pre_squeezed)).encode()
            assert await body.read() == (
                b"encoding=gzip length="
                + expected_length
                + b" accept=absent type=absent body=already encoded by the caller"
            )


async def test_compressing_composes_a_coding_the_package_does_not_ship() -> None:
    async with serving(report_app) as server, ConnectionPool() as pool:
        client = compressing(b"bzip2", bz2.BZ2Compressor)(pool)
        url = f"http://{server.host}:{server.port}/upload"
        async with request(client, "POST", url, body=b"brought our own") as (_head, body):
            assert await body.read() == b"encoding=bzip2 length=absent accept=absent type=absent body=brought our own"


async def test_decompress_decodes_a_coding_from_a_caller_extended_table() -> None:
    payload = b"weird stuff " * 40
    async with serving(encoded_app(b"bzip2", (bz2.compress(payload),))) as server, ConnectionPool() as pool:
        client = decompress({**DEFAULT_DECOMPRESSORS, b"bzip2": bz2.BZ2Decompressor})(pool)
        async with request(client, "GET", f"http://{server.host}:{server.port}/zipped") as (head, body):
            assert not any(name == b"content-encoding" for name, _ in head.headers)
            assert await body.read() == payload


async def test_decompress_derives_its_offer_from_the_table_with_keys_lowercased() -> None:
    async with serving(report_app) as server, ConnectionPool() as pool:
        client = decompress({**DEFAULT_DECOMPRESSORS, b"BZIP2": bz2.BZ2Decompressor})(pool)
        async with request(client, "GET", f"http://{server.host}:{server.port}/items") as (_head, body):
            assert await body.read() == b"encoding=absent length=absent accept=br, bzip2, gzip, zstd type=absent body="


async def test_decompress_advertises_and_decodes_a_gzip_response() -> None:
    payload = b"unzip me " * 40
    async with serving(encoded_app(b"gzip", (gzip.compress(payload),))) as server, ConnectionPool() as pool:
        client = decompress()(pool)
        async with request(client, "GET", f"http://{server.host}:{server.port}/zipped") as (head, body):
            assert head.status == 200
            assert not any(name in (b"content-encoding", b"content-length") for name, _ in head.headers)
            assert await body.read() == payload


async def test_decompress_decodes_a_brotli_response() -> None:
    payload = b"brotli payload " * 40
    async with serving(encoded_app(b"br", (brotli.compress(payload),))) as server, ConnectionPool() as pool:
        client = decompress()(pool)
        async with request(client, "GET", f"http://{server.host}:{server.port}/zipped") as (_head, body):
            assert await body.read() == payload


async def test_decompress_decodes_a_zstd_response() -> None:
    payload = b"zstandard payload " * 40
    async with serving(encoded_app(b"zstd", (zstd.compress(payload),))) as server, ConnectionPool() as pool:
        client = decompress()(pool)
        async with request(client, "GET", f"http://{server.host}:{server.port}/zipped") as (_head, body):
            assert await body.read() == payload


async def test_decompress_decodes_a_body_streamed_across_chunks() -> None:
    payload = b"chunked compressed payload " * 100
    compressed = gzip.compress(payload)
    third = len(compressed) // 3
    chunks = (compressed[:third], compressed[third : 2 * third], compressed[2 * third :])
    async with serving(encoded_app(b"gzip", chunks)) as server, ConnectionPool() as pool:
        client = decompress()(pool)
        async with request(client, "GET", f"http://{server.host}:{server.port}/zipped") as (_head, body):
            assert await body.read() == payload


async def test_decompress_decodes_concatenated_gzip_members_in_one_chunk() -> None:
    members = gzip.compress(b"first member ") + gzip.compress(b"second member ") + gzip.compress(b"third member")
    async with serving(encoded_app(b"gzip", (members,))) as server, ConnectionPool() as pool:
        client = decompress()(pool)
        async with request(client, "GET", f"http://{server.host}:{server.port}/zipped") as (_head, body):
            assert await body.read() == b"first member second member third member"


async def test_decompress_decodes_gzip_members_arriving_as_separate_chunks() -> None:
    chunks = (gzip.compress(b"member on its own chunk, "), gzip.compress(b"and the next one"))
    async with serving(encoded_app(b"gzip", chunks)) as server, ConnectionPool() as pool:
        client = decompress()(pool)
        async with request(client, "GET", f"http://{server.host}:{server.port}/zipped") as (_head, body):
            assert await body.read() == b"member on its own chunk, and the next one"


async def test_decompress_decodes_gzip_members_whose_boundaries_fall_mid_chunk() -> None:
    members = gzip.compress(b"alpha ") + gzip.compress(b"beta ") + gzip.compress(b"gamma")
    third = len(members) // 3
    chunks = (members[:third], members[third : 2 * third], members[2 * third :])
    async with serving(encoded_app(b"gzip", chunks)) as server, ConnectionPool() as pool:
        client = decompress()(pool)
        async with request(client, "GET", f"http://{server.host}:{server.port}/zipped") as (_head, body):
            assert await body.read() == b"alpha beta gamma"


async def test_decompress_decodes_concatenated_zstd_frames() -> None:
    frames = zstd.compress(b"first frame ") + zstd.compress(b"second frame")
    async with serving(encoded_app(b"zstd", (frames,))) as server, ConnectionPool() as pool:
        client = decompress()(pool)
        async with request(client, "GET", f"http://{server.host}:{server.port}/zipped") as (_head, body):
            assert await body.read() == b"first frame second frame"


async def test_decompress_raises_when_a_later_gzip_member_is_truncated() -> None:
    trailing = gzip.compress(b"cut short " * 40)

    def answer(_request: ClientRequest) -> ClientResponse:
        async def events() -> AsyncGenerator[bytes | ResponseTrailers]:
            yield gzip.compress(b"whole member ") + trailing[: len(trailing) // 2]

        return ClientResponse(ResponseHead(200, ((b"content-encoding", b"gzip"),)), ResponseBody(events()))

    client = decompress()(mock_client(answer))
    with pytest.raises(ConnectionError, match="compressed stream"):
        async with request(client, "GET", "http://mock.test/zipped") as (_head, body):
            await body.read()


async def test_decompress_sends_its_accept_encoding_offer() -> None:
    async with serving(report_app) as server, ConnectionPool() as pool:
        client = decompress()(pool)
        async with request(client, "GET", f"http://{server.host}:{server.port}/items") as (_head, body):
            assert await body.read() == b"encoding=absent length=absent accept=br, gzip, zstd type=absent body="


async def test_decompress_keeps_a_callers_own_accept_encoding() -> None:
    async with serving(report_app) as server, ConnectionPool() as pool:
        client = decompress()(pool)
        url = f"http://{server.host}:{server.port}/items"
        async with request(client, "GET", url, headers=((b"accept-encoding", b"identity"),)) as (_head, body):
            assert await body.read() == b"encoding=absent length=absent accept=identity type=absent body="


async def test_decompress_leaves_an_unknown_encoding_untouched() -> None:
    raw = b"LZW bytes the client cannot decode"
    async with serving(encoded_app(b"compress", (raw,))) as server, ConnectionPool() as pool:
        client = decompress()(pool)
        async with request(client, "GET", f"http://{server.host}:{server.port}/zipped") as (head, body):
            assert (b"content-encoding", b"compress") in head.headers
            assert await body.read() == raw


async def test_decompress_raises_on_a_truncated_compressed_body() -> None:
    compressed = gzip.compress(b"cut short " * 40)

    def answer(_request: ClientRequest) -> ClientResponse:
        async def events() -> AsyncGenerator[bytes | ResponseTrailers]:
            yield compressed[: len(compressed) // 2]

        return ClientResponse(ResponseHead(200, ((b"content-encoding", b"gzip"),)), ResponseBody(events()))

    client = decompress()(mock_client(answer))
    with pytest.raises(ConnectionError, match="compressed stream"):
        async with request(client, "GET", "http://mock.test/zipped") as (_head, body):
            await body.read()


async def test_decompress_tolerates_an_empty_encoded_body() -> None:
    async with serving(encoded_app(b"gzip", ())) as server, ConnectionPool() as pool:
        client = decompress()(pool)
        async with request(client, "GET", f"http://{server.host}:{server.port}/zipped") as (head, body):
            assert not any(name == b"content-encoding" for name, _ in head.headers)
            assert await body.read() == b""


async def test_decompress_passes_trailers_through_undecoded() -> None:
    def answer(_request: ClientRequest) -> ClientResponse:
        async def events() -> AsyncGenerator[bytes | ResponseTrailers]:
            yield gzip.compress(b"body before trailers")
            yield b""  # an empty chunk mid-stream decodes to nothing and is dropped
            yield ResponseTrailers(((b"x-checksum", b"abc123"),))

        return ClientResponse(ResponseHead(200, ((b"content-encoding", b"gzip"),)), ResponseBody(events()))

    client = decompress()(mock_client(answer))
    async with request(client, "GET", "http://mock.test/zipped") as (_head, body):
        chunks, trailers = await body.read_with_trailers()
        assert chunks == b"body before trailers"
        assert trailers == (ResponseTrailers(((b"x-checksum", b"abc123"),)),)


async def test_decompress_releases_the_connection_when_a_body_is_abandoned() -> None:
    payload = gzip.compress(b"abandoned " * 40)
    timeout = Timeout(pool=timedelta(seconds=2))
    async with (
        serving(encoded_app(b"gzip", (payload,))) as server,
        ConnectionPool(max_connections_per_host=1) as pool,
    ):
        client = decompress()(pool)
        url = f"http://{server.host}:{server.port}/zipped"
        async with request(client, "GET", url, timeout=timeout) as (head, _body):
            assert head.status == 200  # exit without reading: the wrapped body must still release
        async with request(client, "GET", url, timeout=timeout) as (_head, body):
            assert await body.read() == b"abandoned " * 40
