from __future__ import annotations

import gzip
from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from compression import zstd
from dataclasses import dataclass
from dataclasses import field

import brotli
import pytest
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
from without_asgi.compression import DEFAULT_COMPRESSORS
from without_asgi.compression import MAX_RANDOM_BYTES
from without_asgi.compression import PADDED_COMPRESSORS
from without_asgi.compression import Compressor
from without_asgi.compression import _PaddedGzipCompressor
from without_asgi.compression import compress
from without_asgi.compression import is_compressible
from without_asgi.compression import negotiate_coding
from without_asgi.compression import padded_gzip_compressor
from without_asgi.compression import padded_zstd_compressor
from without_asgi.routing import HttpMiddleware

# A body long enough to clear the default `minimum_size` gate, and compressible
# enough that the encoded form is unmistakably shorter than the plain one.
BODY = b'{"todos":[' + b'{"title":"write the docs","done":false},' * 40 + b"]}"
SHORT = b'{"ok":true}'


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
            pytest.param(b"application/x-ndjson", True, id="ndjson-compresses"),
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

    @pytest.mark.parametrize("status", [204, 304])
    async def test_leaves_a_bodyless_status_untouched(self, status: int) -> None:
        source = (
            ResponseStart(status=status, headers=((b"content-type", b"application/json"),)),
            ResponseBody(body=b"", more_body=False),
        )
        events = await _run(compress(minimum_size=0), _accepting(b"gzip"), source)
        head = _head(events)
        assert _one(head, b"content-encoding") is None
        assert _values(head, b"vary") == []

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


def _streamed(chunks: Sequence[bytes], *, headers: RawHeaders = ()) -> tuple[Outbound, ...]:
    """A response whose head declares no length and whose body arrives in pieces."""
    return (
        ResponseStart(status=200, headers=((b"content-type", b"application/x-ndjson"), *headers)),
        *(ResponseBody(body=chunk, more_body=index < len(chunks) - 1) for index, chunk in enumerate(chunks)),
    )


class TestStreamingResponses:
    async def test_encodes_a_stream_that_crosses_the_gate(self) -> None:
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

    async def test_releases_a_stream_that_never_reaches_the_gate(self) -> None:
        chunks = [SHORT, SHORT]
        events = await _run(compress(), _accepting(b"gzip"), _streamed(chunks))
        head = _head(events)
        assert _one(head, b"content-encoding") is None
        assert _body(events) == b"".join(chunks)

    async def test_holds_the_head_until_the_gate_resolves(self) -> None:
        """
        The head cannot go out before the size is known, since it carries the
        `content-encoding` that decision produces.
        """
        released: list[bytes] = []

        async def source(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
            yield ResponseStart(status=200, headers=((b"content-type", b"application/x-ndjson"),))
            for chunk in (BODY[:100], BODY[100:]):
                released.append(chunk)
                yield ResponseBody(body=chunk, more_body=chunk != BODY[100:])

        handler = compress()(source, None, _accepting(b"gzip"))
        seen: list[Outbound] = []
        async for event in handler(stream_from_iterable(())):
            if isinstance(event, ResponseStart):
                assert len(released) == 2
            seen.append(event)
        assert gzip.decompress(_body(seen)) == BODY

    async def test_passes_a_truncated_stream_through_unencoded(self) -> None:
        """A body that stops without a final event is already broken; encoding it would hide that."""
        source = (
            ResponseStart(status=200, headers=((b"content-type", b"application/x-ndjson"),)),
            ResponseBody(body=SHORT, more_body=True),
        )
        events = await _run(compress(), _accepting(b"gzip"), source)
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

    async def test_encodes_every_chunk_after_the_gate(self) -> None:
        chunks = [BODY[:300], BODY[300:600], BODY[600:800], BODY[800:]]
        events = await _run(compress(), _accepting(b"gzip"), _streamed(chunks))
        assert gzip.decompress(_body(events)) == BODY


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
        offloaded = PathSend(path="/srv/data.json")
        source = (
            ResponseStart(status=200, headers=((b"content-type", b"application/json"),)),
            ResponseBody(body=SHORT, more_body=True),
            offloaded,
        )
        events = await _run(compress(), _accepting(b"gzip"), source)
        assert _body(events) == SHORT
        assert events[-1] == offloaded

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
    """A `Compressor` that plugs into the table without any real codec behind it."""

    chunks: list[bytes] = field(default_factory=list)

    def compress(self, data: bytes, /) -> bytes:
        self.chunks.append(data)
        return data * 2

    def flush(self) -> bytes:
        return b"!"


@dataclass(slots=True, eq=False)
class _Hoarder:
    """
    A `Compressor` that emits nothing until the stream ends.

    Real codecs buffer, so how much comes back from any one `compress` call is not
    something a test should depend on. These two fakes pin the extremes instead:
    `_Doubler` emits on every call, `_Hoarder` on none until `flush`.
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

    async def test_forwards_a_streaming_codecs_output_as_it_is_produced(self) -> None:
        chunks = [BODY[:300], BODY[300:600], BODY[600:800], BODY[800:]]
        events = await _run(compress({b"funky": _Doubler}), _accepting(b"funky"), _streamed(chunks))
        bodies = [event.body for event in events if isinstance(event, ResponseBody)]
        # The prefix is released as one chunk, then each later chunk keeps its own event.
        assert bodies == [
            b"".join(chunks[:2]) * 2,
            chunks[2] * 2,
            chunks[3] * 2,
            b"!",
        ]

    async def test_emits_no_body_event_for_a_codec_holding_bytes_back(self) -> None:
        chunks = [BODY[:300], BODY[300:600], BODY[600:800], BODY[800:]]
        events = await _run(compress({b"funky": _Hoarder}), _accepting(b"funky"), _streamed(chunks))
        bodies = [event for event in events if isinstance(event, ResponseBody)]
        assert [event.body for event in bodies] == [BODY]
        assert [event.more_body for event in bodies] == [False]


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

            _real: Compressor = field(default_factory=lambda: DEFAULT_COMPRESSORS[b"gzip"]())
            _pending: bytes = b""

            def compress(self, data: bytes, /) -> bytes:
                self._pending += self._real.compress(data)
                head, self._pending = self._pending[:1], self._pending[1:]
                return head

            def flush(self) -> bytes:
                return self._pending + self._real.flush()

        padded = _PaddedGzipCompressor(_Dribbler(), b"deadbeef")
        pieces = (BODY[start : start + 7] for start in range(0, len(BODY), 7))
        encoded = b"".join(padded.compress(piece) for piece in pieces) + padded.flush()
        assert gzip.decompress(encoded) == BODY
