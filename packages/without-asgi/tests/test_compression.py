from __future__ import annotations

import gzip
import zlib
from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from compression import zstd
from dataclasses import dataclass
from dataclasses import field
from typing import cast

import brotli
import pytest
from hypothesis import given
from hypothesis import strategies as st
from without import Stream
from without import stream_from_iterable
from without_asgi import Asgi
from without_asgi import EarlyHint
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import PathSend
from without_asgi import RawHeaders
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_asgi import ResponseTrailers
from without_asgi import ServerPush
from without_asgi import ZeroCopySend
from without_asgi.compression import DEFAULT_COMPRESSORS
from without_asgi.compression import GZIP_CONTAINER
from without_asgi.compression import MAX_RANDOM_BYTES
from without_asgi.compression import PADDED_COMPRESSORS
from without_asgi.compression import Compressor
from without_asgi.compression import OffloadedBodyAfterEncoding
from without_asgi.compression import StreamingCompressor
from without_asgi.compression import _PaddedGzipCompressor
from without_asgi.compression import compress
from without_asgi.compression import is_compressible
from without_asgi.compression import negotiate_coding
from without_asgi.compression import padded_gzip_compressor
from without_asgi.compression import padded_zstd_compressor
from without_asgi.routing import HttpMiddleware

# A body long enough to clear the default `minimum_size` floor, and compressible
# enough that the encoded form is unmistakably shorter than the plain one.
BODY = b'{"todos":[' + b'{"title":"write the docs","done":false},' * 40 + b"]}"
SHORT = b'{"ok":true}'


@dataclass(frozen=True, slots=True)
class _FileDescriptor:
    """The whole of what `ZeroCopySend` asks of a file: something with a descriptor."""

    def fileno(self) -> int:
        return 7  # pragma: no cover - only its presence satisfies the SupportsFileno protocol; never called


def _scope(*, headers: RawHeaders = ()) -> HttpScope:
    return HttpScope(
        asgi=Asgi(version="3.0", spec_version="2.4"),
        http_version="1.1",
        method="GET",
        scheme="http",
        path="/todos",
        raw_path=b"/todos",
        query_string=b"",
        root_path="",
        headers=headers,
        client=None,
        server=None,
        extensions=None,
    )


def _accepting(offer: bytes) -> HttpScope:
    return _scope(headers=((b"accept-encoding", offer),))


def _handler(events: Iterable[Outbound]) -> HttpHandler:
    def handler(inputs: Stream[Inbound]) -> Stream[Outbound]:
        return stream_from_iterable(tuple(events))

    return handler


async def _run(middleware: HttpMiddleware[object], scope: HttpScope, events: Iterable[Outbound]) -> list[Outbound]:
    handler = middleware(_handler(events), None, scope)
    return [event async for event in handler(stream_from_iterable(()))]


def _head(events: Sequence[Outbound]) -> ResponseStart:
    start = events[0]
    assert isinstance(start, ResponseStart)
    return start


def _body(events: Sequence[Outbound]) -> bytes:
    return b"".join(event.body for event in events if isinstance(event, ResponseBody))


def _values(head: ResponseStart, name: bytes) -> list[bytes]:
    return [value for key, value in head.headers if key.lower() == name]


def _one(head: ResponseStart, name: bytes) -> bytes | None:
    values = _values(head, name)
    assert len(values) <= 1
    return values[0] if values else None


def _json_response(body: bytes = BODY, *, headers: RawHeaders = ()) -> tuple[Outbound, ...]:
    return (
        ResponseStart(
            status=200,
            headers=((b"content-type", b"application/json"), (b"content-length", str(len(body)).encode()), *headers),
        ),
        ResponseBody(body=body, more_body=False),
    )


class TestNegotiateCoding:
    @pytest.mark.parametrize(
        ("accept_encoding", "available", "expected"),
        [
            pytest.param(None, (b"zstd", b"gzip"), None, id="absent-field-stays-identity"),
            pytest.param(b"", (b"zstd", b"gzip"), None, id="empty-field-refuses-every-coding"),
            pytest.param(b"gzip", (b"zstd", b"gzip"), b"gzip", id="only-what-is-offered-and-available"),
            pytest.param(b"gzip, deflate, br", (b"zstd", b"gzip"), b"gzip", id="unavailable-codings-skipped"),
            pytest.param(b"zstd, gzip", (b"zstd", b"gzip"), b"zstd", id="equal-weights-take-server-preference"),
            pytest.param(b"zstd, gzip", (b"gzip", b"zstd"), b"gzip", id="server-preference-is-the-table-order"),
            pytest.param(b"gzip;q=0.5, zstd;q=1.0", (b"gzip", b"zstd"), b"zstd", id="weight-beats-server-preference"),
            pytest.param(b"zstd;q=0.5, gzip;q=1.0", (b"zstd", b"gzip"), b"gzip", id="weight-beats-order-either-way"),
            pytest.param(b"gzip;q=0", (b"gzip",), None, id="zero-weight-means-unacceptable"),
            pytest.param(b"*", (b"zstd", b"gzip"), b"zstd", id="wildcard-matches-every-coding"),
            pytest.param(b"gzip, *;q=0", (b"zstd", b"gzip"), b"gzip", id="named-coding-beats-a-refusing-wildcard"),
            pytest.param(b"*;q=0", (b"zstd", b"gzip"), None, id="refusing-wildcard-leaves-nothing"),
            pytest.param(b"identity;q=1.0, gzip;q=0.5", (b"gzip",), None, id="identity-can-outrank-a-coding"),
            pytest.param(b"gzip;q=1.0, identity;q=0.5, *;q=0", (b"gzip",), b"gzip", id="rfc-9110-worked-example"),
            pytest.param(b"gzip;q=0.5, *;q=0.9", (b"gzip",), None, id="wildcard-lifts-unlisted-identity"),
            pytest.param(b"identity;q=0", (b"gzip",), None, id="unsatisfiable-request-still-gets-identity"),
            pytest.param(b"GZIP;Q=0.5", (b"gzip",), b"gzip", id="tokens-and-parameters-are-case-insensitive"),
            pytest.param(b"  gzip ;  q=0.5  ", (b"gzip",), b"gzip", id="whitespace-around-parts-ignored"),
            pytest.param(b"gzip,", (b"gzip",), b"gzip", id="trailing-comma-tolerated"),
            pytest.param(b"gzip;q=bogus", (b"gzip",), None, id="unreadable-weight-drops-the-entry"),
            pytest.param(b"gzip;q=7", (b"gzip",), None, id="out-of-range-weight-drops-the-entry"),
            pytest.param(b"gzip;q=nan", (b"gzip",), None, id="nan-weight-drops-the-entry"),
            pytest.param(b"gzip;level=9", (b"gzip",), b"gzip", id="a-non-q-parameter-is-ignored"),
            pytest.param(b"gzip;q=0.001", (b"gzip",), b"gzip", id="the-least-preferred-weight-is-still-acceptable"),
        ],
    )
    def test_picks_the_coding_rfc_9110_calls_for(
        self, accept_encoding: bytes | None, available: tuple[bytes, ...], expected: bytes | None
    ) -> None:
        assert negotiate_coding(accept_encoding, available) == expected

    def test_offers_nothing_when_the_server_has_no_codings(self) -> None:
        assert negotiate_coding(b"gzip, zstd, br", ()) is None


class TestIsCompressible:
    @pytest.mark.parametrize(
        ("content_type", "expected"),
        [
            pytest.param(b"text/html", True, id="text-subtypes-compress"),
            pytest.param(b"text/plain; charset=utf-8", True, id="parameters-are-stripped-before-matching"),
            pytest.param(b"TEXT/HTML", True, id="media-types-are-case-insensitive"),
            pytest.param(b"text/event-stream", False, id="event-streams-are-excluded-despite-being-text"),
            pytest.param(b"application/json", True, id="json-compresses"),
            pytest.param(b"application/problem+json", True, id="the-json-suffix-covers-vendor-types"),
            pytest.param(b"image/svg+xml", True, id="the-xml-suffix-reaches-svg"),
            pytest.param(b"application/vnd.oai.openapi+yaml", True, id="the-yaml-suffix-reaches-an-openapi-document"),
            pytest.param(b"application/geo+json-seq", True, id="the-json-seq-suffix-is-its-own-registration"),
            pytest.param(b"application/x-ndjson", True, id="ndjson-compresses"),
            pytest.param(b"application/json-seq", True, id="json-text-sequences-compress-like-ndjson"),
            pytest.param(b"image/png", False, id="already-compressed-images-are-left-alone"),
            pytest.param(b"video/mp4", False, id="video-is-left-alone"),
            pytest.param(b"application/octet-stream", False, id="unknown-bytes-are-left-alone"),
            pytest.param(b"application/zip", False, id="archives-are-left-alone"),
            pytest.param(None, False, id="an-undeclared-type-is-left-alone"),
        ],
    )
    def test_allows_only_types_worth_the_cpu(self, content_type: bytes | None, expected: bool) -> None:
        assert is_compressible(content_type) is expected


class TestBufferedResponses:
    async def test_encodes_a_body_the_client_accepts(self) -> None:
        events = await _run(compress(), _accepting(b"gzip"), _json_response())
        head = _head(events)
        assert _one(head, b"content-encoding") == b"gzip"
        assert gzip.decompress(_body(events)) == BODY

    async def test_redescribes_the_encoded_body_with_an_exact_length(self) -> None:
        events = await _run(compress(), _accepting(b"gzip"), _json_response())
        head = _head(events)
        encoded = _body(events)
        assert _one(head, b"content-length") == str(len(encoded)).encode()
        assert len(encoded) < len(BODY)

    async def test_prefers_brotli_when_the_client_accepts_everything(self) -> None:
        events = await _run(compress(), _accepting(b"gzip, zstd, br"), _json_response())
        assert _one(_head(events), b"content-encoding") == b"br"
        assert brotli.decompress(_body(events)) == BODY

    async def test_prefers_zstd_over_gzip(self) -> None:
        events = await _run(compress(), _accepting(b"gzip, zstd"), _json_response())
        assert _one(_head(events), b"content-encoding") == b"zstd"
        assert zstd.decompress(_body(events)) == BODY

    async def test_leaves_the_body_alone_without_an_accept_encoding(self) -> None:
        events = await _run(compress(), _scope(), _json_response())
        head = _head(events)
        assert _one(head, b"content-encoding") is None
        assert _one(head, b"content-length") == str(len(BODY)).encode()
        assert _body(events) == BODY

    async def test_leaves_the_body_alone_for_a_coding_it_cannot_produce(self) -> None:
        events = await _run(compress(), _accepting(b"deflate"), _json_response())
        assert _one(_head(events), b"content-encoding") is None
        assert _body(events) == BODY

    async def test_leaves_a_body_under_the_minimum_size_alone(self) -> None:
        events = await _run(compress(), _accepting(b"gzip"), _json_response(SHORT))
        head = _head(events)
        assert _one(head, b"content-encoding") is None
        assert _body(events) == SHORT

    async def test_weighs_a_body_that_declares_no_length_by_its_bytes(self) -> None:
        source = (
            ResponseStart(status=200, headers=((b"content-type", b"application/json"),)),
            ResponseBody(body=SHORT, more_body=False),
        )
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _one(_head(events), b"content-encoding") is None
        assert _body(events) == SHORT

    async def test_honors_a_lowered_minimum_size(self) -> None:
        events = await _run(compress(minimum_size=1), _accepting(b"gzip"), _json_response(SHORT))
        assert _one(_head(events), b"content-encoding") == b"gzip"
        assert gzip.decompress(_body(events)) == SHORT

    async def test_reads_an_accept_encoding_split_across_header_lines(self) -> None:
        """Reading only the first line would miss `gzip` and answer unencoded."""
        scope = _scope(headers=((b"accept-encoding", b"deflate"), (b"accept-encoding", b"gzip")))
        events = await _run(compress(), scope, _json_response())
        assert _one(_head(events), b"content-encoding") == b"gzip"


class TestCandidacy:
    async def test_leaves_a_non_compressible_media_type_untouched(self) -> None:
        source = (
            ResponseStart(status=200, headers=((b"content-type", b"image/png"),)),
            ResponseBody(body=BODY, more_body=False),
        )
        events = await _run(compress(), _accepting(b"gzip"), source)
        head = _head(events)
        assert _one(head, b"content-encoding") is None
        assert _values(head, b"vary") == []
        assert _body(events) == BODY

    async def test_leaves_an_already_encoded_body_untouched(self) -> None:
        source = (
            ResponseStart(status=200, headers=((b"content-type", b"application/json"), (b"content-encoding", b"br"))),
            ResponseBody(body=BODY, more_body=False),
        )
        events = await _run(compress(), _accepting(b"gzip"), source)
        head = _head(events)
        assert _values(head, b"content-encoding") == [b"br"]
        assert _values(head, b"vary") == []
        assert _body(events) == BODY

    @pytest.mark.parametrize("status", [204, 206, 304])
    async def test_leaves_a_status_no_coding_applies_to_untouched(self, status: int) -> None:
        source = (
            ResponseStart(status=status, headers=((b"content-type", b"application/json"),)),
            ResponseBody(body=BODY, more_body=False),
        )
        events = await _run(compress(minimum_size=0), _accepting(b"gzip"), source)
        head = _head(events)
        assert _one(head, b"content-encoding") is None
        assert _body(events) == BODY

    @pytest.mark.parametrize("status", [204, 206])
    async def test_declares_no_variance_on_a_status_it_never_encodes(self, status: int) -> None:
        source = (
            ResponseStart(status=status, headers=((b"content-type", b"application/json"),)),
            ResponseBody(body=BODY, more_body=False),
        )
        events = await _run(compress(minimum_size=0), _accepting(b"gzip"), source)
        assert _values(_head(events), b"vary") == []

    async def test_leaves_a_range_response_describing_the_bytes_it_still_carries(self) -> None:
        """
        `content-range` names offsets into the *identity* representation, so encoding
        the range would leave it describing bytes the client no longer holds.
        """
        source = (
            ResponseStart(
                status=206,
                headers=(
                    (b"content-type", b"application/json"),
                    (b"content-range", b"bytes 0-499/10000"),
                ),
            ),
            ResponseBody(body=BODY, more_body=False),
        )
        events = await _run(compress(minimum_size=0), _accepting(b"gzip"), source)
        head = _head(events)
        assert _one(head, b"content-encoding") is None
        assert _one(head, b"content-range") == b"bytes 0-499/10000"
        assert _body(events) == BODY

    async def test_honors_an_injected_compressibility_policy(self) -> None:
        source = (
            ResponseStart(status=200, headers=((b"content-type", b"application/octet-stream"),)),
            ResponseBody(body=BODY, more_body=False),
        )
        events = await _run(compress(compressible=lambda _type: True), _accepting(b"gzip"), source)
        assert _one(_head(events), b"content-encoding") == b"gzip"


class TestVary:
    async def test_declares_the_variance_even_when_nothing_is_encoded(self) -> None:
        events = await _run(compress(), _scope(), _json_response())
        assert _values(_head(events), b"vary") == [b"accept-encoding"]

    async def test_declares_the_variance_on_an_encoded_response(self) -> None:
        events = await _run(compress(), _accepting(b"gzip"), _json_response())
        assert _values(_head(events), b"vary") == [b"accept-encoding"]

    async def test_keeps_a_variance_the_handler_already_declared(self) -> None:
        source = _json_response(headers=((b"vary", b"cookie"),))
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _values(_head(events), b"vary") == [b"cookie", b"accept-encoding"]

    async def test_does_not_repeat_a_variance_the_handler_already_declared(self) -> None:
        source = _json_response(headers=((b"vary", b"cookie, Accept-Encoding"),))
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _values(_head(events), b"vary") == [b"cookie, Accept-Encoding"]

    async def test_leaves_an_unlimited_variance_alone(self) -> None:
        source = _json_response(headers=((b"vary", b"*"),))
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _values(_head(events), b"vary") == [b"*"]

    async def test_declares_the_variance_on_a_revalidated_response(self) -> None:
        """
        RFC 9110 §15.4.5 asks a `304` to carry the fields its `200` would have, and
        names `vary` because a shared cache reads it to pick the stored variant to
        update. A `304` that dropped it would leave the cache updating whichever
        encoding it happened to hold.
        """
        source = (ResponseStart(status=304, headers=((b"etag", b'"v1"'),)),)
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _values(_head(events), b"vary") == [b"accept-encoding"]

    async def test_does_not_repeat_a_variance_a_revalidated_response_declared(self) -> None:
        source = (ResponseStart(status=304, headers=((b"vary", b"accept-encoding"),)),)
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _values(_head(events), b"vary") == [b"accept-encoding"]

    async def test_encodes_nothing_on_a_revalidated_response(self) -> None:
        source = (ResponseStart(status=304, headers=((b"etag", b'"v1"'),)),)
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _one(_head(events), b"content-encoding") is None


class TestEtag:
    async def test_weakens_a_strong_validator_when_the_bytes_change(self) -> None:
        source = _json_response(headers=((b"etag", b'"v1"'),))
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _one(_head(events), b"etag") == b'W/"v1"'

    async def test_leaves_a_weak_validator_alone(self) -> None:
        source = _json_response(headers=((b"etag", b'W/"v1"'),))
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _one(_head(events), b"etag") == b'W/"v1"'

    async def test_leaves_a_strong_validator_alone_when_nothing_is_encoded(self) -> None:
        source = _json_response(headers=((b"etag", b'"v1"'),))
        events = await _run(compress(), _scope(), source)
        assert _one(_head(events), b"etag") == b'"v1"'

    async def test_weakens_a_revalidated_validator_for_a_client_that_negotiates_a_coding(self) -> None:
        """
        RFC 9111 §4.3.4 has a cache copy a `304`'s fields onto the stored entry its
        `vary` key selects, which for this client is the encoded one. A strong tag
        landing there would let a later `If-Range` match under strong comparison and
        stitch identity range bytes into an encoded body.
        """
        source = (ResponseStart(status=304, headers=((b"etag", b'"v1"'),)),)
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _one(_head(events), b"etag") == b'W/"v1"'

    async def test_leaves_a_revalidated_validator_alone_for_a_client_holding_identity_bytes(self) -> None:
        """A client that negotiates no coding holds the unencoded body, so its tag is still true of it."""
        source = (ResponseStart(status=304, headers=((b"etag", b'"v1"'),)),)
        events = await _run(compress(), _scope(), source)
        assert _one(_head(events), b"etag") == b'"v1"'

    async def test_weakens_a_revalidated_validator_for_a_type_it_would_have_encoded(self) -> None:
        source = (ResponseStart(status=304, headers=((b"content-type", b"application/json"), (b"etag", b'"v1"'))),)
        events = await _run(compress(), _accepting(b"gzip"), source)
        head = _head(events)
        assert _one(head, b"etag") == b'W/"v1"'
        assert _values(head, b"vary") == [b"accept-encoding"]

    @pytest.mark.parametrize(
        "described",
        [
            pytest.param((b"content-type", b"video/mp4"), id="a-type-no-coding-applies-to"),
            pytest.param((b"content-encoding", b"br"), id="a-body-the-app-encoded-itself"),
        ],
    )
    async def test_leaves_a_revalidated_validator_alone_for_bytes_it_never_encodes(
        self, described: tuple[bytes, bytes]
    ) -> None:
        """
        A `304` that says what it revalidates settles what the stored `200` is. Where
        that is a representation this middleware never re-encodes, the stored bytes
        are the ones the strong tag was stated for, and weakening it would break
        every later `If-Range`, which needs strong comparison, into a full response.
        """
        source = (ResponseStart(status=304, headers=(described, (b"etag", b'"v1"'))),)
        events = await _run(compress(), _accepting(b"gzip"), source)
        head = _head(events)
        assert _one(head, b"etag") == b'"v1"'
        assert _values(head, b"vary") == []

    async def test_leaves_a_revalidated_validator_alone_for_a_body_under_the_floor(self) -> None:
        """
        The floor settles what the stored `200` is just as its media type does. A `304`
        may state the size of the representation it revalidates (RFC 9110 §8.6), and
        under `minimum_size` that body went out unencoded whatever this client offered,
        so the strong tag is still true of the bytes the client holds.
        """
        described = ((b"content-type", b"text/html"), (b"content-length", b"300"), (b"etag", b'"v1"'))
        events = await _run(
            compress(minimum_size=500), _accepting(b"gzip"), (ResponseStart(status=304, headers=described),)
        )
        head = _head(events)
        assert _one(head, b"etag") == b'"v1"'
        # The variance is still declared: candidacy is a property of the resource, and
        # a `200` of this type below the floor carries `vary` too.
        assert _values(head, b"vary") == [b"accept-encoding"]

    async def test_weakens_a_revalidated_validator_for_a_body_over_the_floor(self) -> None:
        described = ((b"content-type", b"text/html"), (b"content-length", b"900"), (b"etag", b'"v1"'))
        events = await _run(
            compress(minimum_size=500), _accepting(b"gzip"), (ResponseStart(status=304, headers=described),)
        )
        assert _one(_head(events), b"etag") == b'W/"v1"'


# A decoder fed one encoded event at a time, which is what makes "delivered as it is
# produced" checkable: a codec buffering its output leaves these returning nothing
# until the very end.
INCREMENTAL_DECODERS: dict[bytes, Callable[[], Callable[[bytes], bytes]]] = {
    b"gzip": lambda: zlib.decompressobj(GZIP_CONTAINER).decompress,
    b"zstd": lambda: zstd.ZstdDecompressor().decompress,
    b"br": lambda: brotli.Decompressor().process,
}


def _streamed(chunks: Sequence[bytes], *, headers: RawHeaders = ()) -> tuple[Outbound, ...]:
    """A response whose head declares no length and whose body arrives in pieces."""
    return (
        ResponseStart(status=200, headers=((b"content-type", b"application/x-ndjson"), *headers)),
        *(ResponseBody(body=chunk, more_body=index < len(chunks) - 1) for index, chunk in enumerate(chunks)),
    )


def _recorded(chunks: Sequence[bytes], *, headers: RawHeaders = ()) -> tuple[HttpHandler, list[bytes]]:
    """A streaming handler, paired with the list of chunks it has released so far."""
    released: list[bytes] = []

    def handler(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
        async def events() -> AsyncIterator[Outbound]:
            yield ResponseStart(status=200, headers=((b"content-type", b"application/x-ndjson"), *headers))
            for index, chunk in enumerate(chunks):
                released.append(chunk)
                yield ResponseBody(body=chunk, more_body=index < len(chunks) - 1)

        return events()

    return handler, released


class TestStreamingResponses:
    async def test_encodes_a_streamed_body(self) -> None:
        chunks = [BODY[:300], BODY[300:600], BODY[600:]]
        events = await _run(compress(), _accepting(b"gzip"), _streamed(chunks))
        head = _head(events)
        assert _one(head, b"content-encoding") == b"gzip"
        assert gzip.decompress(_body(events)) == BODY

    async def test_drops_the_length_it_can_no_longer_state(self) -> None:
        source = _streamed([BODY[:600], BODY[600:]], headers=((b"content-length", str(len(BODY)).encode()),))
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _one(_head(events), b"content-length") is None

    async def test_ends_the_encoded_stream_with_a_final_body_event(self) -> None:
        events = await _run(compress(), _accepting(b"gzip"), _streamed([BODY[:600], BODY[600:]]))
        bodies = [event for event in events if isinstance(event, ResponseBody)]
        assert [event.more_body for event in bodies] == [*([True] * (len(bodies) - 1)), False]

    async def test_encodes_an_undeclared_stream_too_small_for_the_floor(self) -> None:
        """Nothing weighs a stream whose head declared no length, so it commits on its first chunk."""
        chunks = [SHORT, SHORT]
        events = await _run(compress(), _accepting(b"gzip"), _streamed(chunks))
        assert _one(_head(events), b"content-encoding") == b"gzip"
        assert gzip.decompress(_body(events)) == b"".join(chunks)

    async def test_leaves_a_stream_behind_a_declared_small_length_alone(self) -> None:
        """
        A declared length answers the floor for a stream with none of the holding
        `weigh_undeclared_bodies` costs. Without it the floor never reaches a
        `file_response`, which stats the size and then yields the file chunk by
        chunk, so a stylesheet gzip can only grow would go out encoded.
        """
        declared = str(2 * len(SHORT)).encode()
        source = _streamed([SHORT, SHORT], headers=((b"content-length", declared),))
        events = await _run(compress(), _accepting(b"gzip"), source)
        head = _head(events)
        assert _one(head, b"content-encoding") is None
        assert _one(head, b"content-length") == declared
        assert _body(events) == SHORT * 2

    async def test_weighs_the_bytes_of_a_stream_whose_declared_length_cannot_be_read(self) -> None:
        source = _streamed([BODY[:600], BODY[600:]], headers=((b"content-length", b"about a kilobyte"),))
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _one(_head(events), b"content-encoding") == b"gzip"
        assert gzip.decompress(_body(events)) == BODY

    async def test_releases_a_weighed_stream_that_never_reaches_the_floor(self) -> None:
        chunks = [SHORT, SHORT]
        events = await _run(compress(weigh_undeclared_bodies=True), _accepting(b"gzip"), _streamed(chunks))
        head = _head(events)
        assert _one(head, b"content-encoding") is None
        assert _body(events) == b"".join(chunks)

    async def test_releases_the_head_with_the_first_chunk_of_a_stream(self) -> None:
        """
        The head carries the `content-encoding`, so holding it holds the whole
        response. Committing on the first chunk is what keeps a slow stream moving:
        nothing waits on bytes the app has not produced yet.
        """
        source, released = _recorded([BODY[:100], BODY[100:600], BODY[600:]])
        handler = compress()(source, None, _accepting(b"gzip"))
        seen: list[Outbound] = []
        async for event in handler(stream_from_iterable(())):
            if isinstance(event, ResponseStart):
                assert released == [BODY[:100]]
            seen.append(event)
        assert gzip.decompress(_body(seen)) == BODY

    async def test_holds_the_head_of_a_weighed_stream_until_the_floor_resolves(self) -> None:
        """What weighing a stream buys in bytes it spends here, holding produced chunks to weigh them."""
        source, released = _recorded([BODY[:100], BODY[100:600], BODY[600:]])
        handler = compress(weigh_undeclared_bodies=True)(source, None, _accepting(b"gzip"))
        seen: list[Outbound] = []
        async for event in handler(stream_from_iterable(())):
            if isinstance(event, ResponseStart):
                assert released == [BODY[:100], BODY[100:600]]
            seen.append(event)
        assert gzip.decompress(_body(seen)) == BODY

    async def test_commits_a_declared_stream_on_its_first_chunk_even_when_weighing(self) -> None:
        """
        A declared length answered the floor before any body event, so there is
        nothing left for weighing to hold for: what `weigh_undeclared_bodies` buys
        is an answer for a body whose size is *unknown*, and paying its latency for
        a known one would buy nothing.
        """
        declared = ((b"content-length", str(len(BODY)).encode()),)
        source, released = _recorded([BODY[:100], BODY[100:600], BODY[600:]], headers=declared)
        handler = compress(weigh_undeclared_bodies=True)(source, None, _accepting(b"gzip"))
        seen: list[Outbound] = []
        async for event in handler(stream_from_iterable(())):
            if isinstance(event, ResponseStart):
                assert released == [BODY[:100]]
            seen.append(event)
        assert gzip.decompress(_body(seen)) == BODY

    async def test_leaves_a_truncated_stream_truncated(self) -> None:
        """
        A body that stops without a final event is already broken. Committed on its
        first chunk, what goes out is an encoded stream that stops too, which is the
        breakage the transport already signals rather than a second one.
        """
        source = (
            ResponseStart(status=200, headers=((b"content-type", b"application/x-ndjson"),)),
            ResponseBody(body=SHORT, more_body=True),
        )
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _one(_head(events), b"content-encoding") == b"gzip"
        assert zlib.decompressobj(GZIP_CONTAINER).decompress(_body(events)) == SHORT

    async def test_passes_a_truncated_weighed_stream_below_the_floor_through_unencoded(self) -> None:
        source = (
            ResponseStart(status=200, headers=((b"content-type", b"application/x-ndjson"),)),
            ResponseBody(body=SHORT, more_body=True),
        )
        events = await _run(compress(weigh_undeclared_bodies=True), _accepting(b"gzip"), source)
        assert _one(_head(events), b"content-encoding") is None
        assert _body(events) == SHORT

    async def test_emits_only_the_head_when_no_body_ever_arrives(self) -> None:
        source = (ResponseStart(status=200, headers=((b"content-type", b"application/json"),)),)
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert len(events) == 1
        assert _values(_head(events), b"vary") == [b"accept-encoding"]

    async def test_passes_trailers_through_after_an_encoded_stream(self) -> None:
        trailers = ResponseTrailers(headers=((b"digest", b"sha-256=abc"),))
        source = (*_streamed([BODY[:300], BODY[300:600], BODY[600:]]), trailers)
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert events[-1] == trailers
        assert gzip.decompress(_body(events)) == BODY

    async def test_encodes_every_chunk_of_a_stream(self) -> None:
        chunks = [BODY[:300], BODY[300:600], BODY[600:800], BODY[800:]]
        events = await _run(compress(), _accepting(b"gzip"), _streamed(chunks))
        assert gzip.decompress(_body(events)) == BODY

    async def test_spends_no_event_on_an_empty_chunk(self) -> None:
        """
        An empty chunk has nothing to deliver, and ending a block for it would spend
        framing bytes on nothing, so it produces no event at all. A leading one
        cannot commit the response either, for the same reason: there is nothing yet
        to encode.
        """
        source = (
            ResponseStart(status=200, headers=((b"content-type", b"application/x-ndjson"),)),
            ResponseBody(body=b"", more_body=True),
            ResponseBody(body=BODY[:300], more_body=True),
            ResponseBody(body=b"", more_body=True),
            ResponseBody(body=BODY[300:], more_body=False),
        )
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _one(_head(events), b"content-encoding") == b"gzip"
        bodies = [event.body for event in events if isinstance(event, ResponseBody)]
        assert len(bodies) == 2
        assert gzip.decompress(b"".join(bodies)) == BODY

    @pytest.mark.parametrize("coding", [b"gzip", b"zstd", b"br"])
    async def test_delivers_each_chunk_as_the_app_produces_it(self, coding: bytes) -> None:
        """
        Streaming past the floor has to mean *streaming*. Left to decide for itself, a
        codec emits almost nothing per `compress` call and holds the rest until the
        stream ends, so a body that arrives in pieces would reach the client as one
        burst at the end, with every byte of it resident in the codec until then.
        Decoding event by event is what tells the two apart.
        """
        chunks = [BODY[:300], BODY[300:600], BODY[600:800], BODY[800:]]
        events = await _run(compress(), _accepting(coding), _streamed(chunks))
        assert _one(_head(events), b"content-encoding") == coding
        decode = INCREMENTAL_DECODERS[coding]()
        bodies = [event.body for event in events if isinstance(event, ResponseBody)]
        # Every chunk decodes whole out of an event of its own.
        assert [decode(body) for body in bodies] == chunks


class TestOtherEvents:
    async def test_passes_events_before_the_head_through_untouched(self) -> None:
        hint = EarlyHint(links=(b"</app.css>; rel=preload",))
        events = await _run(compress(), _accepting(b"gzip"), (hint, *_json_response()))
        assert events[0] == hint
        assert _one(_head(events[1:]), b"content-encoding") == b"gzip"

    async def test_passes_trailers_through_after_an_encoded_body(self) -> None:
        trailers = ResponseTrailers(headers=((b"digest", b"sha-256=abc"),))
        events = await _run(compress(), _accepting(b"gzip"), (*_json_response(), trailers))
        assert events[-1] == trailers
        assert gzip.decompress(_body(events)) == BODY

    async def test_passes_trailers_through_after_a_declared_body_under_the_floor(self) -> None:
        trailers = ResponseTrailers(headers=((b"digest", b"sha-256=abc"),))
        events = await _run(compress(), _accepting(b"gzip"), (*_json_response(SHORT), trailers))
        assert events[-1] == trailers
        assert _body(events) == SHORT

    async def test_passes_trailers_through_after_an_undeclared_body_under_the_floor(self) -> None:
        trailers = ResponseTrailers(headers=((b"digest", b"sha-256=abc"),))
        source = (
            ResponseStart(status=200, headers=((b"content-type", b"application/json"),)),
            ResponseBody(body=SHORT, more_body=False),
            trailers,
        )
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert events[-1] == trailers
        assert _body(events) == SHORT

    async def test_states_no_length_for_a_head_that_announced_trailers(self) -> None:
        """
        HTTP/1.1 carries trailers only in the chunked coding, so the exact length this
        response could otherwise state would frame the body by length and strand the
        trailers behind it.
        """
        source = (
            ResponseStart(
                status=200,
                headers=((b"content-type", b"application/json"), (b"content-length", str(len(BODY)).encode())),
                trailers=True,
            ),
            ResponseBody(body=BODY, more_body=False),
            ResponseTrailers(headers=((b"digest", b"sha-256=abc"),)),
        )
        events = await _run(compress(), _accepting(b"gzip"), source)
        head = _head(events)
        assert head.trailers
        assert _one(head, b"content-encoding") == b"gzip"
        assert _one(head, b"content-length") is None
        assert gzip.decompress(_body(events)) == BODY

    async def test_encodes_a_body_behind_a_push_the_app_sent_first(self) -> None:
        """
        `http.response.push` may be sent any time after the head and before the final
        body event, so one can arrive while the floor is still being weighed. It says
        nothing about the encoding, and it cannot go out ahead of the head it follows.
        """
        push = ServerPush(path="/app.css", headers=((b"accept", b"text/css"),))
        source = (
            ResponseStart(status=200, headers=((b"content-type", b"application/json"),)),
            push,
            ResponseBody(body=BODY, more_body=False),
        )
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _one(_head(events), b"content-encoding") == b"gzip"
        assert events[1] == push
        assert gzip.decompress(_body(events)) == BODY

    async def test_releases_a_push_held_behind_a_committed_stream(self) -> None:
        head, *bodies = _streamed([BODY[:600], BODY[600:]])
        push = ServerPush(path="/app.css", headers=((b"accept", b"text/css"),))
        events = await _run(compress(), _accepting(b"gzip"), (head, push, *bodies))
        assert _one(_head(events), b"content-encoding") == b"gzip"
        assert events[1] == push
        assert gzip.decompress(_body(events)) == BODY

    async def test_releases_a_push_held_behind_a_body_under_the_floor(self) -> None:
        push = ServerPush(path="/app.css", headers=((b"accept", b"text/css"),))
        source = (
            ResponseStart(status=200, headers=((b"content-type", b"application/json"),)),
            push,
            ResponseBody(body=SHORT, more_body=False),
        )
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _one(_head(events), b"content-encoding") is None
        assert events[1] == push
        assert _body(events) == SHORT

    async def test_leaves_an_offloaded_body_unencoded(self) -> None:
        """`PathSend` hands the transfer below Python, so there are no bytes here to encode."""
        offloaded = PathSend(path="/srv/data.json")
        source = (
            ResponseStart(status=200, headers=((b"content-type", b"application/json"),)),
            offloaded,
        )
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _one(_head(events), b"content-encoding") is None
        assert offloaded in events
        # No body event either: the app sent none, so neither does the middleware.
        assert not [event for event in events if isinstance(event, ResponseBody)]

    async def test_releases_a_buffered_prefix_before_an_offloaded_body(self) -> None:
        """A floor the prefix has not cleared leaves the middleware uncommitted, so the offload still fits."""
        offloaded = PathSend(path="/srv/data.json")
        source = (
            ResponseStart(status=200, headers=((b"content-type", b"application/json"),)),
            ResponseBody(body=SHORT, more_body=True),
            offloaded,
        )
        events = await _run(compress(weigh_undeclared_bodies=True), _accepting(b"gzip"), source)
        assert _one(_head(events), b"content-encoding") is None
        assert _body(events) == SHORT
        assert events[-1] == offloaded

    @pytest.mark.parametrize(
        "offloaded",
        [
            pytest.param(PathSend(path="/srv/data.json"), id="path-send"),
            pytest.param(ZeroCopySend(file=_FileDescriptor()), id="zero-copy-send"),
        ],
    )
    async def test_refuses_to_offload_a_body_it_has_committed_to_encoding(self, offloaded: Outbound) -> None:
        """
        `ZeroCopySend` carries `more_body` precisely so it can follow body events the
        app has already sent, and the bytes either extension sends are ones this
        middleware never sees. Spliced into a stream whose head declares
        `content-encoding`, they make a body no decoder can read, so the response is
        truncated instead.
        """
        source = (
            ResponseStart(status=200, headers=((b"content-type", b"application/json"),)),
            ResponseBody(body=BODY, more_body=True),
            offloaded,
        )
        with pytest.raises(OffloadedBodyAfterEncoding):
            await _run(compress(), _accepting(b"gzip"), source)

    async def test_refuses_to_offload_after_an_encoded_body_has_finished(self) -> None:
        """The stream is closed by then, so the offloaded bytes would trail a complete encoded body."""
        source = (*_json_response(), PathSend(path="/srv/data.json"))
        with pytest.raises(OffloadedBodyAfterEncoding):
            await _run(compress(), _accepting(b"gzip"), source)

    async def test_passes_events_after_an_offloaded_body_through(self) -> None:
        trailers = ResponseTrailers(headers=((b"digest", b"sha-256=abc"),))
        source = (
            ResponseStart(status=200, headers=((b"content-type", b"application/json"),)),
            PathSend(path="/srv/data.json"),
            trailers,
        )
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert events[-1] == trailers

    async def test_emits_nothing_for_a_handler_that_emits_nothing(self) -> None:
        assert await _run(compress(), _accepting(b"gzip"), ()) == []

    async def test_passes_a_response_that_never_starts_through(self) -> None:
        """No head means nothing to describe, so the events go by untouched."""
        hint = EarlyHint(links=(b"</app.css>; rel=preload",))
        assert await _run(compress(), _accepting(b"gzip"), (hint,)) == [hint]


@dataclass(slots=True, eq=False)
class _Doubler:
    """A `StreamingCompressor` that plugs into the table without any real codec behind it."""

    chunks: list[bytes] = field(default_factory=list)

    def compress(self, data: bytes, /) -> bytes:
        self.chunks.append(data)
        return data * 2

    def flush_block(self) -> bytes:
        return b"|"

    def flush(self) -> bytes:
        return b"!"


@dataclass(slots=True, eq=False)
class _Hoarder:
    """
    A `Compressor` with no way to flush short of ending the stream.

    Real codecs buffer, so how much comes back from any one `compress` call is not
    something a test should depend on. These two fakes pin the extremes instead:
    `_Doubler` can be flushed at any point, `_Hoarder` only at the end, which is the
    difference `StreamingCompressor` names and the middleware branches on.
    """

    held: list[bytes] = field(default_factory=list)

    def compress(self, data: bytes, /) -> bytes:
        self.held.append(data)
        return b""

    def flush(self) -> bytes:
        return b"".join(self.held)


class TestCodingTable:
    async def test_registers_a_coding_the_package_does_not_ship(self) -> None:
        made: list[_Doubler] = []

        def make() -> Compressor:
            doubler = _Doubler()
            made.append(doubler)
            return doubler

        events = await _run(compress({b"funky": make}), _accepting(b"funky"), _json_response())
        head = _head(events)
        assert _one(head, b"content-encoding") == b"funky"
        assert _body(events) == BODY * 2 + b"!"
        assert [chunk for doubler in made for chunk in doubler.chunks] == [BODY]

    def test_extends_the_default_table_without_touching_it(self) -> None:
        extended = DEFAULT_COMPRESSORS | {b"funky": _Doubler}
        assert list(extended) == [b"br", b"zstd", b"gzip", b"funky"]
        assert b"funky" not in DEFAULT_COMPRESSORS

    def test_refuses_to_let_the_default_table_be_mutated(self) -> None:
        with pytest.raises(TypeError):
            DEFAULT_COMPRESSORS[b"funky"] = _Doubler  # type: ignore[index]

    async def test_negotiates_only_the_codings_the_table_holds(self) -> None:
        gzip_only = {b"gzip": DEFAULT_COMPRESSORS[b"gzip"]}
        events = await _run(compress(gzip_only), _accepting(b"zstd, gzip"), _json_response())
        assert _one(_head(events), b"content-encoding") == b"gzip"

    async def test_matches_a_coding_the_table_spells_differently(self) -> None:
        events = await _run(compress({b"GZIP": DEFAULT_COMPRESSORS[b"gzip"]}), _accepting(b"gzip"), _json_response())
        assert _one(_head(events), b"content-encoding") == b"gzip"

    async def test_builds_one_compressor_per_response(self) -> None:
        made: list[_Doubler] = []

        def make() -> Compressor:
            doubler = _Doubler()
            made.append(doubler)
            return doubler

        middleware = compress({b"funky": make})
        await _run(middleware, _accepting(b"funky"), _json_response())
        await _run(middleware, _accepting(b"funky"), _json_response())
        assert len(made) == 2

    async def test_flushes_a_block_per_chunk_out_of_a_streaming_codec(self) -> None:
        chunks = [BODY[:300], BODY[300:600], BODY[600:800], BODY[800:]]
        events = await _run(compress({b"funky": _Doubler}), _accepting(b"funky"), _streamed(chunks))
        bodies = [event.body for event in events if isinstance(event, ResponseBody)]
        # Every chunk keeps its own event and every one of them is flushed. The last
        # rides out on the flush that ends the stream rather than paying for a block
        # of its own.
        assert bodies == [*(chunk * 2 + b"|" for chunk in chunks[:-1]), chunks[-1] * 2 + b"!"]

    async def test_leaves_a_stream_unencoded_for_a_codec_that_cannot_flush(self) -> None:
        """
        `_Hoarder` can only be emptied by ending the stream, so encoding through it
        would hold the whole body back and deliver it in one burst. Unencoded is the
        honest answer: it costs bytes, where encoding would cost the incremental
        delivery the app streamed for.
        """
        chunks = [BODY[:300], BODY[300:600], BODY[600:800], BODY[800:]]
        events = await _run(compress({b"funky": _Hoarder}), _accepting(b"funky"), _streamed(chunks))
        head = _head(events)
        assert _one(head, b"content-encoding") is None
        assert _values(head, b"vary") == [b"accept-encoding"]
        assert _body(events) == BODY

    async def test_still_encodes_a_buffered_body_for_a_codec_that_cannot_flush(self) -> None:
        """The buffered case never needs a mid-stream flush, so the plain protocol is enough for it."""
        events = await _run(compress({b"funky": _Hoarder}), _accepting(b"funky"), _json_response())
        assert _one(_head(events), b"content-encoding") == b"funky"
        assert _body(events) == BODY


def _encoded(make: Callable[[], Compressor], data: bytes, *, chunk: int | None = None) -> bytes:
    compressor = make()
    if chunk is None:
        return compressor.compress(data) + compressor.flush()
    pieces = (data[start : start + chunk] for start in range(0, len(data), chunk))
    return b"".join(compressor.compress(piece) for piece in pieces) + compressor.flush()


# What a padded container costs when the random run happens to be empty: gzip pays the
# NUL that terminates its filename field, zstd the 8-byte header of a skippable frame.
EMPTY_PADDING_OVERHEAD = {b"gzip": 1, b"zstd": 8}
DECODERS: dict[bytes, Callable[[bytes], bytes]] = {b"gzip": gzip.decompress, b"zstd": zstd.decompress}
PADDED_FACTORIES: dict[bytes, Callable[[int], Compressor]] = {
    b"gzip": padded_gzip_compressor,
    b"zstd": padded_zstd_compressor,
}


class TestPaddedCompressors:
    """
    Heal The Breach: the padded codings put a random-length run somewhere the
    container defines and the decoder ignores, so the response length stops being a
    function of the content alone.
    """

    @pytest.mark.parametrize("coding", [b"gzip", b"zstd"])
    def test_padded_output_still_decodes(self, coding: bytes) -> None:
        assert DECODERS[coding](_encoded(PADDED_COMPRESSORS[coding], BODY)) == BODY

    @pytest.mark.parametrize("coding", [b"gzip", b"zstd"])
    def test_padded_output_still_decodes_when_fed_in_pieces(self, coding: bytes) -> None:
        """Gzip's header arrives with whichever `compress` call zlib decides to emit it on."""
        assert DECODERS[coding](_encoded(PADDED_COMPRESSORS[coding], BODY, chunk=7)) == BODY

    @pytest.mark.parametrize("coding", [b"gzip", b"zstd"])
    def test_padded_output_still_decodes_when_flushed_per_chunk(self, coding: bytes) -> None:
        """The streaming path ends a block per chunk, which the padding has to survive."""
        compressor = PADDED_COMPRESSORS[coding]()
        assert isinstance(compressor, StreamingCompressor)
        pieces = (BODY[start : start + 7] for start in range(0, len(BODY), 7))
        encoded = b"".join(compressor.compress(piece) + compressor.flush_block() for piece in pieces)
        assert DECODERS[coding](encoded + compressor.flush()) == BODY

    def test_padded_zstd_decodes_for_a_reader_that_stops_at_the_first_frame(self) -> None:
        """
        A decoder is entitled to stop at the end of the frame it just read, and the
        stdlib's incremental one does. Padding placed *before* the data would hand it
        an empty body and strand the whole payload in `unused_data`, so the skippable
        frame goes last, where every decoder has already seen the content.
        """
        decompressor = zstd.ZstdDecompressor()
        assert decompressor.decompress(_encoded(padded_zstd_compressor, BODY)) == BODY
        # What such a reader is left holding is the padding alone, which it may
        # discard: the body was already whole before it stopped.
        assert zstd.decompress(decompressor.unused_data) == b""

    @pytest.mark.parametrize("coding", [b"gzip", b"zstd"])
    def test_an_empty_budget_costs_only_the_container_field(self, coding: bytes) -> None:
        plain = len(_encoded(DEFAULT_COMPRESSORS[coding], BODY))
        padded = len(_encoded(lambda: PADDED_FACTORIES[coding](0), BODY))
        assert padded - plain == EMPTY_PADDING_OVERHEAD[coding]

    @pytest.mark.parametrize("coding", [b"gzip", b"zstd"])
    def test_the_budget_bounds_what_padding_costs(self, coding: bytes) -> None:
        plain = len(_encoded(DEFAULT_COMPRESSORS[coding], BODY))
        ceiling = plain + EMPTY_PADDING_OVERHEAD[coding] + MAX_RANDOM_BYTES
        assert all(plain < len(_encoded(PADDED_COMPRESSORS[coding], BODY)) <= ceiling for _ in range(50))

    @pytest.mark.parametrize("coding", [b"gzip", b"zstd"])
    def test_the_encoded_length_varies_between_responses(self, coding: bytes) -> None:
        """
        The whole point: one body must not always encode to one length. Every draw
        landing on the same length would need 50 consecutive hits on one of 101
        budgets, so this cannot flake in any run anyone will see.
        """
        lengths = {len(_encoded(PADDED_COMPRESSORS[coding], BODY)) for _ in range(50)}
        assert len(lengths) > 1

    def test_offers_only_the_codings_a_container_can_pad(self) -> None:
        """
        Brotli is absent by construction, not by omission: its bindings expose no
        metadata block to hide a random run in, so a table including it would leave
        the coding a browser reaches for first unpadded.
        """
        assert set(PADDED_COMPRESSORS) == {b"gzip", b"zstd"}
        assert b"br" not in PADDED_COMPRESSORS

    async def test_serves_a_padded_coding_to_a_client_that_prefers_brotli(self) -> None:
        events = await _run(compress(PADDED_COMPRESSORS), _accepting(b"br, gzip"), _json_response())
        head = _head(events)
        assert _one(head, b"content-encoding") == b"gzip"
        assert gzip.decompress(_body(events)) == BODY

    async def test_leaves_a_body_unencoded_when_only_brotli_is_offered(self) -> None:
        events = await _run(compress(PADDED_COMPRESSORS), _accepting(b"br"), _json_response())
        assert _one(_head(events), b"content-encoding") is None
        assert _body(events) == BODY

    def test_splices_the_filename_however_the_header_arrives(self) -> None:
        """
        `zlib` happens to emit the 10-byte gzip header whole on its first call, but
        the wrapper does not get to assume that: a codec that dribbled it out would
        otherwise have random bytes spliced into the middle of its own header.
        """

        @dataclass(slots=True, eq=False)
        class _Dribbler:
            """A gzip `Compressor` that hands its header back a byte at a time."""

            _real: StreamingCompressor = field(
                default_factory=lambda: cast(StreamingCompressor, DEFAULT_COMPRESSORS[b"gzip"]())
            )
            _pending: bytes = b""

            def compress(self, data: bytes, /) -> bytes:
                self._pending += self._real.compress(data)
                head, self._pending = self._pending[:1], self._pending[1:]
                return head

            def flush_block(self) -> bytes:
                held, self._pending = self._pending, b""
                return held + self._real.flush_block()

            def flush(self) -> bytes:
                return self._pending + self._real.flush()

        padded = _PaddedGzipCompressor(_Dribbler(), b"deadbeef")
        pieces = (BODY[start : start + 7] for start in range(0, len(BODY), 7))
        encoded = b"".join(padded.compress(piece) + padded.flush_block() for piece in pieces)
        assert gzip.decompress(encoded + padded.flush()) == BODY


# Every coding the shipped tables can produce, with the decoder that reads it back. A
# padded coding decodes with the plain one: the random run it carries lives in a part
# of the container the decoder is required to ignore.
DECODED_BY: dict[bytes, Callable[[bytes], bytes]] = {
    b"gzip": gzip.decompress,
    b"zstd": zstd.decompress,
    b"br": brotli.decompress,
}


@given(
    chunks=st.lists(st.binary(max_size=400), min_size=1, max_size=6),
    minimum_size=st.integers(min_value=0, max_value=800),
    offer=st.sampled_from([b"gzip", b"zstd", b"br", b"br, zstd, gzip", b"gzip;q=0.5, zstd", b"identity"]),
    declares_length=st.booleans(),
    weighs_undeclared=st.booleans(),
    table=st.sampled_from([DEFAULT_COMPRESSORS, PADDED_COMPRESSORS]),
)
async def test_a_response_of_any_shape_describes_the_bytes_it_carries(
    chunks: list[bytes],
    minimum_size: int,
    offer: bytes,
    declares_length: bool,
    weighs_undeclared: bool,
    table: Mapping[bytes, Callable[[], Compressor]],
) -> None:
    """
    The invariant every path through the middleware owes the client, over the shapes a
    response comes in: how the body is split, whether the head declared a length,
    whether the floor is above or below it, which coding was negotiated, and whether
    the coding pads. Whatever comes out decodes back to exactly what went in, and a
    `content-length` is stated only when it counts the bytes on the wire, since a head
    describing a body it does not have is the one failure a decoder cannot recover
    from.
    """
    body = b"".join(chunks)
    declared = ((b"content-length", str(len(body)).encode()),) if declares_length else ()
    source: tuple[Outbound, ...] = (
        ResponseStart(status=200, headers=((b"content-type", b"application/json"), *declared)),
        *(ResponseBody(body=chunk, more_body=index < len(chunks) - 1) for index, chunk in enumerate(chunks)),
    )
    middleware = compress(table, minimum_size=minimum_size, weigh_undeclared_bodies=weighs_undeclared)

    events = await _run(middleware, _accepting(offer), source)

    head = _head(events)
    carried = _body(events)
    coding = _one(head, b"content-encoding")
    assert coding is None or coding in table
    assert (DECODED_BY[coding](carried) if coding is not None else carried) == body
    length = _one(head, b"content-length")
    assert length is None or int(length) == len(carried)
    # The body is complete: the client is told where it ends rather than left holding a
    # stream the decoder would still be waiting on.
    bodies = [event for event in events if isinstance(event, ResponseBody)]
    assert bodies
    assert not bodies[-1].more_body
