from __future__ import annotations

import secrets
import struct
import zlib
from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from compression import zstd
from dataclasses import dataclass
from dataclasses import replace
from types import MappingProxyType
from typing import Protocol
from typing import runtime_checkable

import brotli
from without_streams import Stream

from without_asgi import headers
from without_asgi.outbound import Outbound
from without_asgi.outbound import PathSend
from without_asgi.outbound import ResponseBody
from without_asgi.outbound import ResponseStart
from without_asgi.outbound import ZeroCopySend
from without_asgi.routing import HttpMiddleware
from without_asgi.routing import wrap
from without_asgi.scope import HttpScope
from without_asgi.sse import EVENT_STREAM_MEDIA_TYPE
from without_asgi.types import RawHeaders

__all__ = [
    "DEFAULT_COMPRESSORS",
    "DYNAMIC_BROTLI_QUALITY",
    "GZIP_CONTAINER",
    "MAX_RANDOM_BYTES",
    "PADDED_COMPRESSORS",
    "Compressor",
    "OffloadedBodyAfterEncoding",
    "StreamingCompressor",
    "brotli_compressor",
    "compress",
    "gzip_compressor",
    "is_compressible",
    "negotiate_coding",
    "padded_gzip_compressor",
    "padded_zstd_compressor",
    "zstd_compressor",
]

# The `wbits` value that selects the gzip wrapper around DEFLATE, for both
# `zlib.compressobj` and `zlib.decompressobj`. Public because the coding tables take
# factories, so anyone writing a gzip variant (a different level, a tuned memory
# level) needs the same magic offset, and it is the kind of value that is wrong
# silently: raw DEFLATE differs from gzip only in the framing, so a wrong `wbits`
# round-trips against itself and fails only against a real peer.
GZIP_CONTAINER = zlib.MAX_WBITS | 16


class Compressor(Protocol):
    """
    The incremental compressor shape `compress` drives: feed chunks through
    `compress`, then `flush` ends the stream. `zlib.compressobj` and
    `zstd.ZstdCompressor` satisfy it as-is, and a third-party codec plugs in with
    whatever thin adapter its own surface needs.

    This is the lesser of two rungs, and which one a table entry lands on decides
    what happens to a streaming body: a codec that satisfies only this protocol can
    be emptied only by ending the stream, which is enough for a response that arrives
    whole and not for one still being produced. Prefer the `StreamingCompressor` that
    `gzip_compressor`, `zstd_compressor`, and `brotli_compressor` produce over a raw
    codec, which reaches only this rung.

    The same two shapes drive `without-http`'s request-side `compressing`, which
    re-exports both, so one adapter serves both directions.
    """

    def compress(self, data: bytes, /) -> bytes: ...
    def flush(self) -> bytes: ...


@runtime_checkable
class StreamingCompressor(Compressor, Protocol):
    """
    A `Compressor` that can also make what it has swallowed deliverable *without*
    ending the stream.

    Every codec buffers, and what any one `compress` call returns is the codec's
    choice rather than the caller's: fed the small pieces a streaming body arrives
    in, zlib emits its header and then nothing until `flush`, and zstd emits
    nothing at all. That is right for a body held whole, where the buffering is what
    buys the ratio, and wrong for one being streamed, where it converts incremental
    delivery into a single burst at the end and grows the held bytes without bound.
    `flush_block` ends the current block so everything fed so far decodes now, and
    leaves the stream open for what follows.

    A separate protocol rather than a third method on `Compressor`, because a codec
    without one is still perfectly good at the buffered case and there is no reason
    to shut it out of the table. `compress` reads the difference at the point it
    matters: a coding whose factory produces a plain `Compressor` encodes responses
    that arrive whole and leaves streaming ones unencoded, which costs bytes rather
    than latency.
    """

    def flush_block(self) -> bytes: ...


@dataclass(slots=True, eq=False)
class _FlushingCompressor:
    """
    Assemble a `StreamingCompressor` out of the three calls a codec already has.

    `zlib.compressobj` and `zstd.ZstdCompressor` both spell every flush as one
    method, separating "end a block" from "end the stream" by a *mode* argument
    whose constants are each codec's own. Binding the three calls at construction
    keeps that difference in the factory that knows the codec, rather than pushing a
    mode type through an adapter shared by codecs that spell it incompatibly.
    """

    _compress: Callable[[bytes], bytes]
    _flush_block: Callable[[], bytes]
    _flush: Callable[[], bytes]

    def compress(self, data: bytes, /) -> bytes:
        return self._compress(data)

    def flush_block(self) -> bytes:
        return self._flush_block()

    def flush(self) -> bytes:
        return self._flush()


def gzip_compressor(level: int = zlib.Z_DEFAULT_COMPRESSION) -> StreamingCompressor:
    """
    A fresh gzip `StreamingCompressor`, the codec behind `DEFAULT_COMPRESSORS`' `gzip` entry.

    `level` is zlib's compression level, defaulting to zlib's own default. Called with
    no argument it is already the zero-argument factory a table wants.

    Public because `zlib.compressobj` is not a substitute for it: the raw object is a
    `Compressor` and *not* a `StreamingCompressor`, since ending a block is a mode
    argument to its `flush` rather than a method of its own, so a table entry built
    from it directly would silently take the buffered path for every streaming
    response. `without-http`'s request-side `gzip_compress` drives this same factory.
    """
    raw = zlib.compressobj(level, zlib.DEFLATED, GZIP_CONTAINER)
    return _FlushingCompressor(raw.compress, lambda: raw.flush(zlib.Z_SYNC_FLUSH), raw.flush)


def zstd_compressor(level: int | None = None) -> StreamingCompressor:
    """
    A fresh zstd `StreamingCompressor`, the codec behind `DEFAULT_COMPRESSORS`' `zstd` entry.

    `level` is zstd's compression level, defaulting to the library's own. Everything in
    `gzip_compressor` about why the raw `zstd.ZstdCompressor` is not a substitute
    applies here: it spells a block flush as a mode argument too.
    """
    raw = zstd.ZstdCompressor(level)
    return _FlushingCompressor(raw.compress, lambda: raw.flush(raw.FLUSH_BLOCK), raw.flush)


class _RawBrotliCompressor(Protocol):
    """The slice of `brotli.Compressor` the adapter drives (the bindings ship no types)."""

    def process(self, data: bytes, /) -> bytes: ...
    def flush(self) -> bytes: ...
    def finish(self) -> bytes: ...


@dataclass(slots=True, eq=False)
class _BrotliCompressor:
    """
    Adapt `brotli.Compressor` to the `StreamingCompressor` shape.

    Brotli's bindings spell the incremental surface `process`/`flush`/`finish`,
    where `flush` keeps the stream open and `finish` ends it, so the adapter maps
    `compress` to `process`, `flush_block` to `flush`, and `flush` to `finish`.
    """

    _raw: _RawBrotliCompressor

    def compress(self, data: bytes, /) -> bytes:
        return self._raw.process(data)

    def flush_block(self) -> bytes:
        return self._raw.flush()

    def flush(self) -> bytes:
        return self._raw.finish()


# Brotli's own default is 11, the maximum, which is tuned for compressing an asset
# once and serving it many times. A response encoded per request is the other case,
# so the default here is the quality dynamic content is served at. On response-sized
# bodies the ratio does not suffer for it: measured on a 2 KB JSON body and a 2.7 KB
# HTML one, quality 5 matched or beat 11 outright, since 11's larger window and
# costlier search have little to find in a few kilobytes.
DYNAMIC_BROTLI_QUALITY = 5


def brotli_compressor(quality: int = DYNAMIC_BROTLI_QUALITY) -> StreamingCompressor:
    """
    A fresh brotli `Compressor`, the codec behind `DEFAULT_COMPRESSORS`' `br` entry.

    `quality` is brotli's compression quality (0-11), defaulting to
    `DYNAMIC_BROTLI_QUALITY` rather than the bindings' own 11: a table entry encodes
    a response per request, where 11 costs much more CPU without a ratio to show for
    it at response sizes. Raise it for a table serving bodies large enough for the
    wider window to pay, or content compressed once
    (`compress(DEFAULT_COMPRESSORS | {b"br": lambda: brotli_compressor(11)})`).

    Called with no argument it is already the zero-argument factory a table wants.
    `without-http`'s request-side `brotli_compress` drives the same adapter, keeping
    its own default at 11, since a client compressing one upload is the static case
    again.
    """
    return _BrotliCompressor(brotli.Compressor(quality=quality))


# The codings encoded out of the box. `compress`'s default table, and *ordered*:
# iteration order is the server's own preference, which decides a tie between codings
# the client weighted equally. Best ratio first, which is the order to want because a
# client only offers what it can decode: brotli's text dictionary wins on the HTML and
# JSON most responses are, zstd is the cheapest of the three to encode and decode but
# newer on the client, and gzip is what everything understands.
# A proxy rather than a `dict` for two reasons: it cannot be mutated, so a caller
# cannot reach in and change what every other caller's table starts from, and it
# supports `|`, so extending it reads as `DEFAULT_COMPRESSORS | {...}` and yields a
# fresh `dict` rather than touching this one.
DEFAULT_COMPRESSORS: MappingProxyType[bytes, Callable[[], Compressor]] = MappingProxyType(
    {
        b"br": brotli_compressor,
        b"zstd": zstd_compressor,
        b"gzip": gzip_compressor,
    }
)


# The padding budget from the Heal The Breach paper, and the value Django settled on.
# The paper measures the delay it imposes on an attack at a factor of about 500 for a
# 10-byte budget and 500,000 for 100, against a cost of `MAX_RANDOM_BYTES / 2` bytes
# on an average response.
MAX_RANDOM_BYTES = 100

_GZIP_HEADER_BYTES = 10  # the fixed part, before any optional field the flag byte announces
_GZIP_FNAME_FLAG = 0b0000_1000  # FLG.FNAME (RFC 1952 §2.3.1): an original-filename field follows
_ZSTD_SKIPPABLE_FRAME = 0x184D2A50  # RFC 8878 §3.1.2: a frame decoders are required to ignore


def _random_run(max_random_bytes: int) -> bytes:
    """A run of 0 to `max_random_bytes` bytes, safe to place in any container field."""
    length = secrets.randbelow(max_random_bytes + 1)
    return secrets.token_hex(length).encode()[:length]


@dataclass(slots=True, eq=False)
class _PaddedGzipCompressor:
    """
    A gzip `Compressor` that carries a random-length filename nobody will read.

    RFC 1952 §2.3.1 puts an optional NUL-terminated original-filename field right
    after the fixed 10-byte header, announced by a bit in the flag byte, outside the
    DEFLATE stream and discarded by every decoder. Writing a random-length one there
    is Heal The Breach: the response length stops being a function of the content
    alone, so a length oracle has to average the noise away before it can read
    anything (see `PADDED_COMPRESSORS`).

    `zlib` emits that header with the first output it produces, and produces output
    when it feels like it, so the field is spliced in once enough bytes have
    accumulated rather than at a known call.
    """

    _inner: StreamingCompressor
    _filename: bytes
    _held: bytes = b""
    _spliced: bool = False

    def _splice(self, chunk: bytes) -> bytes:
        if self._spliced:
            return chunk
        self._held += chunk
        if len(self._held) < _GZIP_HEADER_BYTES:
            return b""
        self._spliced = True
        header = bytearray(self._held[:_GZIP_HEADER_BYTES])
        header[3] |= _GZIP_FNAME_FLAG
        return bytes(header) + self._filename + b"\x00" + self._held[_GZIP_HEADER_BYTES:]

    def compress(self, data: bytes, /) -> bytes:
        return self._splice(self._inner.compress(data))

    def flush_block(self) -> bytes:
        return self._splice(self._inner.flush_block())

    def flush(self) -> bytes:
        return self._splice(self._inner.flush())


@dataclass(slots=True, eq=False)
class _PaddedZstdCompressor:
    """
    A zstd `Compressor` that closes with a random-length skippable frame.

    zstd's answer to gzip's filename field: RFC 8878 §3.1.2 reserves a frame kind a
    decoder must skip over, so a random-length one carries the same noise into the
    response length. Everything in `_PaddedGzipCompressor` about why applies here.

    The frame *trails* the data rather than leading it, because a decoder is
    entitled to stop at the end of the first frame it reads, and the stdlib's own
    `zstd.ZstdDecompressor` does: given the padding first it returns an empty body
    and reports the entire payload as `unused_data`, leaving recovery to a caller
    that thought to loop. Trailing it hands every decoder the whole payload before
    the frame it is asked to ignore.
    """

    _inner: StreamingCompressor
    _trailer: bytes

    def compress(self, data: bytes, /) -> bytes:
        return self._inner.compress(data)

    def flush_block(self) -> bytes:
        return self._inner.flush_block()

    def flush(self) -> bytes:
        return self._inner.flush() + self._trailer


def padded_gzip_compressor(max_random_bytes: int = MAX_RANDOM_BYTES) -> StreamingCompressor:
    """A gzip `Compressor` whose output length carries up to `max_random_bytes` of noise."""
    return _PaddedGzipCompressor(gzip_compressor(), _random_run(max_random_bytes))


def padded_zstd_compressor(max_random_bytes: int = MAX_RANDOM_BYTES) -> StreamingCompressor:
    """A zstd `Compressor` whose output length carries up to `max_random_bytes` of noise."""
    payload = _random_run(max_random_bytes)
    trailer = struct.pack("<II", _ZSTD_SKIPPABLE_FRAME, len(payload)) + payload
    return _PaddedZstdCompressor(zstd_compressor(), trailer)


# `compress`'s table for a router whose responses mix a secret with text an attacker
# can influence. Compression leaks such a secret through the response *length*, which
# is the BREACH attack; [Heal The Breach](https://ieeexplore.ieee.org/document/9754554)
# answers it by making that length partly random, so an oracle must average the noise
# away first. The mitigation is per *container*, because the padding has to go
# somewhere a decoder ignores, and that is why this table is shorter than
# `DEFAULT_COMPRESSORS` rather than a padded copy of it: gzip has its filename field
# and zstd its skippable frame, while brotli's bindings expose only `process`, `flush`,
# and `finish`, with no metadata block to write into and no concatenation to prepend
# one as. Dropping `br` is the point rather than a gap: a table where one coding
# silently went unpadded would offer a guarantee it does not keep, and brotli is the
# coding a browser picks first *and* the one measured here to leak fastest.
#
# The trade is stated so it can be taken deliberately: this costs the best-compressing
# coding on the routes that use it, so mount it where responses reflect input back
# beside credentials and leave `DEFAULT_COMPRESSORS` everywhere else.
PADDED_COMPRESSORS: MappingProxyType[bytes, Callable[[], Compressor]] = MappingProxyType(
    {
        b"zstd": padded_zstd_compressor,
        b"gzip": padded_gzip_compressor,
    }
)


def _weight(parameters: bytes) -> float | None:
    """
    The `q` value among a codings entry's `parameters`: `1.0` when unstated,
    `None` when it cannot be read as one.

    An unreadable weight is not an error to raise at a client (`accept-encoding`
    is a preference, and refusing a request over a malformed one would be
    hostile), but it is not a preference either, so the entry it decorates is
    dropped rather than guessed at. Dropping is the conservative direction: the
    coding goes unoffered and the response falls back to identity.
    """
    for parameter in parameters.split(b";"):
        name, _, value = parameter.partition(b"=")
        if name.strip().lower() != b"q":
            continue
        try:
            weight = float(value.strip())
        except ValueError:
            return None
        # Bounds are what reject `q=7` and `q=nan` alike, the latter because every
        # comparison against a NaN is false.
        return weight if 0.0 <= weight <= 1.0 else None
    return 1.0


def _weights(accept_encoding: bytes) -> dict[bytes, float]:
    weights: dict[bytes, float] = {}
    for element in accept_encoding.split(b","):
        codings, _, parameters = element.partition(b";")
        token = codings.strip().lower()
        if not token:
            continue
        weight = _weight(parameters)
        if weight is None:
            continue
        weights[token] = weight
    return weights


def negotiate_coding(accept_encoding: bytes | None, available: Sequence[bytes]) -> bytes | None:
    """
    Pick the content coding a response should use, or `None` for no coding.

    The whole of [RFC 9110 §12.5.3](https://www.rfc-editor.org/rfc/rfc9110#section-12.5.3),
    as a pure function of the request's `accept-encoding` and the codings the
    server can produce, so the rules are testable on their own and reusable
    outside the middleware that ships them. `available` is in the server's
    preference order and decides ties.

    Weights are honored, which is the part usually skipped: a coding carries an
    optional `q` weight (§12.4.2) and the acceptable coding with the highest
    non-zero weight wins, so `gzip;q=0.5, zstd;q=1.0` picks zstd however the
    server would rather rank them. `q=0` means *not acceptable*, so `gzip;q=0`
    excludes gzip rather than selecting it, and `*` matches every coding not
    named explicitly.

    `None` comes back in four cases, three of them the spec's and one this
    package's:

    - No `accept-encoding` at all. §12.5.3 rule 1 makes any coding acceptable
      here, so compressing would be legal; it stays identity anyway, because a
      request that never mentions the field is more often a client that does not
      decode than one that quietly would, and an over-eager coding is a broken
      response where a missed one is only a larger one.
    - An `accept-encoding` present but empty, which the spec reads as wanting no
      coding at all. Note the asymmetry with the case above: absent and empty are
      different requests, and only the empty one *says* identity.
    - Nothing in `available` acceptable, which is the spec's own instruction to
      answer without a coding.
    - Identity outranking every available coding, whether named
      (`identity;q=1.0, gzip;q=0.5`) or reached through a wildcard.

    A request that marks identity unacceptable (`identity;q=0`) and accepts no
    coding this server has is answered with identity regardless, rather than the
    `406` the spec leaves open: nothing is served by failing a request over a
    preference, and every real client handles the unencoded body it claimed not
    to want.
    """
    if accept_encoding is None:
        return None
    weights = _weights(accept_encoding)
    wildcard = weights.get(b"*")
    best: bytes | None = None
    best_weight = 0.0
    for coding in available:
        weight = weights.get(coding, wildcard)
        # `<=` is both halves of the rule: it drops a `q=0` coding as unacceptable
        # and keeps the earlier of two equal weights, which is what makes
        # `available`'s order the server's preference.
        if weight is None or weight <= best_weight:
            continue
        best, best_weight = coding, weight
    if best is None:
        return None
    identity = weights.get(b"identity", wildcard)
    if identity is not None and identity > best_weight:
        return None
    return best


# Media types worth compressing that are not `text/*` and do not carry a
# structured suffix. Everything reachable by those two rules is left out.
_COMPRESSIBLE_TYPES = frozenset(
    {
        b"application/javascript",
        b"application/json",
        b"application/json-seq",
        b"application/wasm",
        b"application/x-javascript",
        b"application/x-ndjson",
        b"application/xml",
    }
)
_COMPRESSIBLE_SUFFIXES = (b"+json", b"+json-seq", b"+xml", b"+yaml")


def is_compressible(content_type: bytes | None) -> bool:
    """
    Whether a response of this media type is worth compressing: `compress`'s
    default policy, replaceable through its `compressible` argument.

    An allowlist, the shape nginx's `gzip_types` takes, rather than a list of
    exclusions: `text/*`, the registered structured syntax suffixes worth encoding
    (`+json`, `+json-seq`, `+xml`, `+yaml`, which is how `application/problem+json`
    and every other vendor type arrives), and a short list of the remaining types
    worth the CPU. A type nobody listed is left alone, so the failure mode of an
    unrecognized type is a larger response rather than cycles burnt re-compressing
    a JPEG.

    Two exceptions to the shape. A response with no `content-type` is not
    compressed, since compression here is driven by the declared media type and
    there is nothing to drive it. And `text/event-stream` is excluded despite the
    `text/` prefix, for a reason about the connection rather than the bytes: an
    event stream is the one response held open for as long as the client stays, so
    every event on it is encoded against a window holding every event before it,
    and an attacker who can inject one event reads the length of the next. That is
    BREACH (see `compress`) with as many samples as it cares to take, on a
    connection it never has to re-establish. A deployment that wants them encoded
    anyway passes its own `compressible` to `compress`.

    That last exclusion is keyed on the media type because a media type is all this
    predicate sees, while the exposure belongs to *streaming*: a streamed response
    of a type allowed here carries the same per-chunk length oracle, on a connection
    that ends rather than one held open. `compress`'s warning says what follows for
    such a route.
    """
    if content_type is None:
        return False
    media = content_type.split(b";")[0].strip().lower()
    if media == EVENT_STREAM_MEDIA_TYPE:
        return False
    if media.startswith(b"text/"):
        return True
    if media.endswith(_COMPRESSIBLE_SUFFIXES):
        return True
    return media in _COMPRESSIBLE_TYPES


# Statuses no coding can be applied to, each for its own reason. `204` and `304`
# carry no content by definition, so there is nothing to encode and a
# `content-encoding` on either would describe a body that does not exist. A `206`
# does carry bytes, but they are a range of the *identity* representation and its
# `content-range` names offsets into that representation, which this middleware has
# no way to restate for an encoded one: encoding the range would leave the field
# describing bytes the client no longer holds, and a client reassembling several
# ranges would stitch them at the wrong offsets.
_UNENCODABLE_STATUSES = frozenset({204, 206, 304})

_NOT_MODIFIED = 304


def _accept_encoding(raw: RawHeaders) -> bytes | None:
    # A list-valued field's value is *all* its occurrences joined by commas
    # (RFC 9110 §5.2), so a client that split its offer across two lines is read
    # whole rather than truncated to the first.
    values = headers.get_all(raw, b"accept-encoding")
    if not values:
        return None
    return b",".join(values)


def _varying(raw: RawHeaders) -> RawHeaders:
    """Declare that the response body depends on `accept-encoding`, once."""
    fields = {field.strip().lower() for field in b",".join(headers.get_all(raw, b"vary")).split(b",")}
    if b"*" in fields or b"accept-encoding" in fields:
        return raw
    return headers.add(raw, b"vary", b"accept-encoding")


def _weakened(raw: RawHeaders) -> RawHeaders:
    """
    Downgrade a strong `etag` to a weak one, because the bytes just changed.

    RFC 9110 §8.8.1 defines a strong validator as one that changes whenever the
    content of a `200` would, so carrying the same strong tag on the encoded and
    unencoded bodies would be a lie. Weakening rather than dropping keeps the
    validator useful in exactly the way it is still true: the two representations
    remain semantically equivalent, so a conditional request still matches under
    weak comparison, while a `Range` request, which requires strong comparison,
    correctly stops matching.
    """
    etag = headers.first(raw, b"etag")
    if etag is None or etag.startswith(b"W/"):
        return raw
    return headers.replace(raw, b"etag", b"W/" + etag)


def _encoded(raw: RawHeaders, coding: bytes, length: int | None) -> RawHeaders:
    """Re-describe the head for a body that is now `coding`-encoded."""
    described = headers.replace(_weakened(raw), b"content-encoding", coding)
    if length is None:
        # `content-length` described the unencoded body and nothing yet knows the
        # encoded one, so it goes rather than contradicting the bytes on the wire.
        return headers.remove(described, b"content-length")
    return headers.replace(described, b"content-length", str(length).encode())


def _is_candidate(start: ResponseStart, compressible: Callable[[bytes | None], bool]) -> bool:
    if start.status in _UNENCODABLE_STATUSES:
        return False
    if headers.first(start.headers, b"content-encoding") is not None:
        return False
    return compressible(headers.first(start.headers, b"content-type"))


def _revalidates_encodable(raw: RawHeaders, compressible: Callable[[bytes | None], bool]) -> bool:
    """
    Whether the stored `200` a `304` revalidates is one this middleware would have
    encoded, and so whether this `304` describes a variant that depends on the
    client's `accept-encoding`.

    RFC 9110 §15.4.5's field list omits `content-type` and most `304`s arrive
    without one, so the usual answer is unknowable and the candidate is assumed: a
    cache keyed on a header the representation does not really vary by shares a
    little less, while one missing a header it does vary by hands a client an
    encoding it cannot read.

    A `304` that *does* say what it revalidates settles it, and settling it matters
    in the direction that costs something. A `video/mp4` or an already-encoded body
    is never encoded here, so the stored bytes *are* the identity representation and
    the strong validator the app stated is still true of them; weakening it anyway
    would break every later `If-Range`, which requires strong comparison, back into
    a full response, for a re-encoding that never happened.

    The size a `304` may also state (RFC 9110 §8.6) is deliberately not read as the
    same kind of evidence, though a `200`'s own `content-length` answers the floor.
    It would only prove the stored body went out unencoded where nothing under
    `minimum_size` is ever encoded, which is `weigh_undeclared_bodies` rather than the
    default, and what it buys is a strong validator on a representation too small for
    anyone to ask a range of.
    """
    if headers.first(raw, b"content-encoding") is not None:
        return False
    content_type = headers.first(raw, b"content-type")
    return content_type is None or compressible(content_type)


def _declared_length(raw: RawHeaders) -> int | None:
    """
    The body size the head states, or `None` when it states none or states one that
    cannot be read as a count.

    An unreadable length is left to the bytes to answer rather than raised at: this
    middleware observes a head it did not write, and every decision it makes has an
    answer that works without one.
    """
    length = headers.first(raw, b"content-length")
    if length is None:
        return None
    try:
        return int(length)
    except ValueError:
        return None


def _trailers_need_chunking(http_version: str) -> bool:
    """
    Whether this HTTP version carries trailers only in the chunked coding, so a
    response announcing them cannot also state an exact `content-length`.

    HTTP/1.1 does, and a length there would frame the body by length and strand what
    follows. HTTP/2 and HTTP/3 send trailers as a second HEADERS frame, which sits
    beside a `content-length` rather than displacing it, so a response announcing
    trailers keeps the exact length it could otherwise state. `1.0` is weighed with
    the rest of the 1.x family rather than read as its own case: it carries no
    trailers at all, so the only response the choice touches there is one announcing
    trailers it cannot send.
    """
    return http_version.startswith("1.")


def _deliverable(compressor: StreamingCompressor, chunk: bytes) -> bytes:
    """
    `chunk` encoded into bytes the client can decode *now*.

    A bare `compress` call returns whatever the codec chose to emit, which for the
    small pieces a streaming body arrives in is usually nothing, so the block is
    ended here to push them out. An empty chunk gets no block of its own: ending one
    costs framing bytes and there is nothing behind them to deliver.
    """
    if not chunk:
        return b""
    return compressor.compress(chunk) + compressor.flush_block()


# The events that carry body bytes this middleware never sees. Both extensions hand
# the transfer to the server, which reads the file itself, so there is nothing here
# to feed a compressor.
_OFFLOADED_BODY = (ZeroCopySend, PathSend)


class OffloadedBodyAfterEncoding(Exception):
    """
    Raised when a response offloads the rest of its body after `compress` has already
    committed to a content coding.

    The `http.response.zerocopysend` and `http.response.pathsend` extensions both
    send bytes the middleware never sees, and `zerocopysend` carries `more_body`
    precisely so it can follow the body events an app has already sent. A response
    that has not been committed to a coding yet takes that combination in stride,
    releasing what it held and passing the offload through unencoded. Once the head
    has gone out declaring `content-encoding`, there is no version of it that is
    correct: the offloaded bytes are not encoded, and appending them to the encoded
    stream produces a body no decoder can read.

    So `compress` raises rather than writing that body. What reaches the client is a
    truncated response, which every transport already signals as one, in place of a
    complete-looking response that decodes to garbage.

    The way out, for an app that means to stream a prefix and then offload the rest,
    is to send the whole response through the offload instead: an offload arriving
    before any body event is the case that always passes through.
    """


def _offloaded_after(coding: bytes) -> str:
    return f"a body already committed to content-encoding {coding!r} cannot be offloaded"


def _released(start: ResponseStart, interleaved: Sequence[Outbound]) -> tuple[Outbound, ...]:
    """
    The head as it goes out, followed by whatever arrived behind it while it was held.

    A server push may be sent any time after the head and before the final body event,
    so one can land while the floor is still being weighed. It decides nothing about
    the encoding and cannot go out ahead of the head it must follow, so it waits with
    the prefix and is released in the order the app sent it.
    """
    return (start, *interleaved)


def compress(
    compressors: Mapping[bytes, Callable[[], Compressor]] = DEFAULT_COMPRESSORS,
    *,
    minimum_size: int = 500,
    weigh_undeclared_bodies: bool = False,
    compressible: Callable[[bytes | None], bool] = is_compressible,
) -> HttpMiddleware[object]:
    """
    An `HttpMiddleware` that negotiates and applies a content coding to response bodies.

    The server-side mirror of `without-http`'s client `decompress`, and the same
    mechanism pointed the other way: that one offers `accept-encoding` and decodes
    what comes back, this one reads `accept-encoding` and encodes what goes out.
    Middleware rather than server behavior, so it applies under any transport
    (HTTP/1.1, HTTP/2, and a third-party ASGI server alike) and to any router,
    and so nothing rewrites bytes that was not asked to.

    `compressors` maps each coding to a factory for a fresh `Compressor`,
    defaulting to `DEFAULT_COMPRESSORS` (brotli, zstd, and gzip). What is
    negotiated is *derived from its keys*, so what is advertised and what can be
    produced cannot disagree, and **its order is the server's preference**, which
    settles a tie between codings a client weighted equally. Retune a shipped
    coding, or register one that does not ship, by extending the table with any
    factory whose product satisfies `Compressor`:

    ```python
    compress(DEFAULT_COMPRESSORS | {b"br": lambda: brotli_compressor(11)})
    ```

    Negotiation is `negotiate_coding`, which implements RFC 9110 §12.5.3 whole,
    weights included; read it for what the client can ask for and what comes back
    unencoded.

    Three things decide whether a response is a *candidate* at all, before any
    client preference is consulted, because all three are properties of the
    response rather than the request: the status has to be one a coding can apply
    to (see `_UNENCODABLE_STATUSES`), a response already carrying
    `content-encoding` is already encoded, and the media type has to be worth
    compressing (`compressible`, defaulting to `is_compressible`). Every candidate
    gets `Vary: Accept-Encoding` whether or not this particular client got a
    compressed body, so a shared cache keys on the header that decides the answer;
    a non-candidate gets no `Vary`, since nothing about it varies, except the `304`
    that revalidates one and inherits the field with the rest of its head.

    A body shorter than `minimum_size` is sent unencoded, because gzip's framing
    costs more than a few hundred bytes of text saves. It is the *only* floor, and
    what decides how it is answered is what the head said rather than how the body
    arrives: a declared `content-length` answers it before a single body event is
    read, a body that ends in the events read so far answers it exactly from its own
    bytes, and a body still being produced behind a head that declared no length is
    the one case that cannot be answered without holding bytes the app has already
    made. An empty body is left alone however low the floor: encoding nothing
    produces pure framing, and a head stating *its* length is how a `HEAD` response,
    whose own body is empty while its head describes the body a `GET` would carry,
    comes to answer with the wrong size.

    `weigh_undeclared_bodies` decides that case, and it is a policy rather than a
    second floor because the only two honest answers are to hold or not to. Holding
    means keeping produced bytes until `minimum_size` of them accumulate, so a feed
    emitting a line a second delivers nothing for as many seconds as that takes, and
    how long that is belongs to the app rather than to this middleware. The default
    does not hold: the middleware commits on the first non-empty chunk and streams
    the rest through the compressor, ending a block per chunk so each one reaches
    the client as it is produced rather than accumulating inside the codec until the
    response finishes. That spends framing bytes on a body too small to earn them,
    bounded by the floor, which is the trade a response the app chose to stream
    usually wants. An app that wants both, incremental delivery *and* the floor,
    declares a `content-length`, which buys the answer for nothing;
    `file_response` does.

    Committing needs a `StreamingCompressor`; a coding whose factory produces a
    plain `Compressor` still encodes responses that arrive whole, and leaves
    streaming ones unencoded rather than stalling them. A response that arrives
    whole gets an exact `content-length` for its encoded body, unless it announced
    trailers over HTTP/1.x, which carries them only in the chunked coding; a streamed
    one loses `content-length` and is framed by the transport, as any streaming body
    is. A strong `etag` is weakened when the body is encoded (see `_weakened`).

    !!! warning "Compression and secrets"

        Compressing a response that mixes a secret (a CSRF token, a session
        identifier) with attacker-influenced text leaks the secret through the
        response's *length*: the BREACH attack. Every coding in
        `DEFAULT_COMPRESSORS` leaks that way, brotli fastest of the three, since a
        longer match with the guessed prefix is exactly what each of them is built
        to find.

        Hand `PADDED_COMPRESSORS` to this middleware on the routes where that
        matters. It is Heal The Breach: a random-length run in a part of the
        container the decoder ignores, so the length stops being a function of the
        content alone. It costs brotli, which has nowhere to put one, and about
        `MAX_RANDOM_BYTES / 2` bytes per response.

        Padding raises the number of samples an attack needs rather than removing
        the leak, so it is defense in depth and not a licence to stop thinking
        about which responses reflect input back beside credentials. Middleware
        coverage is decided by *where* it is mounted, so both answers are
        route-scoped: a router that never reflects input can keep the default
        table and its brotli.

        Streaming is its own exposure and neither answer covers it. A committed
        stream ends a block per chunk, so each chunk the app produces carries
        its own observable length: an attacker reads the length of the
        part holding the secret rather than of the whole response, which is a
        cleaner oracle than the buffered case, and padding does not blunt it,
        since a padded container carries one random run for the whole response
        and none of the chunks behind the first. `is_compressible` excludes
        `text/event-stream` for this, but the property is the streaming rather
        than the media type: a streamed `text/html` page or an
        `application/x-ndjson` feed is exposed the same way, and an event stream
        differs only in staying open to be sampled without re-establishing. Where
        a streamed response mixes a secret with reflected input, keep it off this
        middleware or pass a `compressible` that rejects its type.
    """
    table = {coding.lower(): make_compressor for coding, make_compressor in compressors.items()}
    available = tuple(table)

    async def rewritten(events: Stream[Outbound], scope: HttpScope) -> AsyncIterator[Outbound]:
        outbound = aiter(events)

        # Whatever precedes the head (early hints, a server push, debug info) is not
        # the body and passes straight out; an early hint held until the head would
        # be an early hint for nothing.
        start = await anext(outbound, None)
        while start is not None and not isinstance(start, ResponseStart):
            yield start
            start = await anext(outbound, None)
        if start is None:
            return

        if start.status == _NOT_MODIFIED and _revalidates_encodable(start.headers, compressible):
            # A `304` updates the stored `200` it revalidates, and RFC 9110 §15.4.5
            # asks it to carry the header fields that `200` would have, naming
            # `vary` among them because that is how a shared cache picks the stored
            # variant to update. A `304` this middleware could not have encoded takes
            # neither field and falls through to the pass-through below, since its
            # status is one no coding applies to.
            revalidated = _varying(start.headers)
            if negotiate_coding(_accept_encoding(scope.headers), available) is not None:
                # The stored entry this `304` updates is the one its `vary` key
                # selects, so for a client that negotiates a coding it is the encoded
                # variant, and RFC 9111 §4.3.4 has the cache copy these fields onto
                # it. A strong validator copied onto encoded bytes is the lie
                # `_weakened` exists to prevent, and it undoes itself: a later
                # `If-Range` matches under strong comparison, and the `206` this
                # middleware never encodes returns identity bytes to stitch into an
                # encoded body. A client that negotiates nothing holds the identity
                # representation, so its validator is left as the app stated it.
                # Any `content-length` goes with the strong tag: §8.6 permits one on
                # a `304` only where it equals what a `200` to this request would
                # have carried, and that `200` is the encoded variant, so the size
                # the app stated for the identity body is no longer that number.
                revalidated = headers.remove(_weakened(revalidated), b"content-length")
            yield replace(start, headers=revalidated)
            async for event in outbound:
                yield event
            return

        if not _is_candidate(start, compressible):
            yield start
            async for event in outbound:
                yield event
            return

        start = replace(start, headers=_varying(start.headers))
        coding = negotiate_coding(_accept_encoding(scope.headers), available)
        declared = _declared_length(start.headers)
        if coding is None or (declared is not None and declared < minimum_size):
            # Nothing to encode with, or a head naming a body too small to be worth
            # encoding. A declared length answers the floor here, before a byte is
            # held, which is what carries it to a body the app *streams* behind a
            # known size: `file_response` stats the file and then yields it chunk by
            # chunk, and a stylesheet gzip can only grow is weighed all the same.
            yield start
            async for event in outbound:
                yield event
            return

        # How much of a body still being produced may be held while the floor is
        # weighed, and the whole of what the streaming case adds: committing to a
        # `content-encoding` is the one decision that cannot be taken back, so the
        # head waits here until the floor resolves. A declared length already
        # resolved it, and an undeclared body is held only when asked to be, never
        # past the floor, since bytes held beyond it decide nothing.
        hold = minimum_size if declared is None and weigh_undeclared_bodies else 0
        prefix: list[bytes] = []
        interleaved: list[Outbound] = []
        buffered = 0
        async for event in outbound:
            if isinstance(event, _OFFLOADED_BODY):
                # An offloaded body (a path send, a zero-copy send) is bytes this
                # middleware never sees, so there is nothing to encode. Release the
                # prefix only if any body event arrived: an offload that arrives
                # first has none, and an empty body event conjured ahead of it is a
                # message the app never sent.
                for held in _released(start, interleaved):
                    yield held
                if prefix:
                    yield ResponseBody(body=b"".join(prefix), more_body=True)
                yield event
                async for rest in outbound:
                    yield rest
                return
            if not isinstance(event, ResponseBody):
                interleaved.append(event)
                continue
            prefix.append(event.body)
            buffered += len(event.body)

            if not event.more_body:
                body = b"".join(prefix)
                if not buffered or buffered < minimum_size:
                    # Nothing to encode, or too little to be worth encoding. The empty
                    # body is the first case however low the floor, matching the
                    # streaming path below, which will not commit on nothing either.
                    # Encoding it would produce pure framing and then state *its*
                    # length, which is how a `HEAD` response, whose head describes the
                    # body a `GET` would carry and whose own body is empty, comes to
                    # answer with the size of an empty encoded stream.
                    for held in _released(start, interleaved):
                        yield held
                    yield ResponseBody(body=body, more_body=False)
                    async for rest in outbound:
                        yield rest
                    return
                compressor = table[coding]()
                encoded = compressor.compress(body) + compressor.flush()
                # A head that announced trailers gives up the exact length this
                # response could otherwise state, but only where carrying them means
                # staying chunked (see `_trailers_need_chunking`).
                length = None if start.trailers and _trailers_need_chunking(scope.http_version) else len(encoded)
                for held in _released(replace(start, headers=_encoded(start.headers, coding, length)), interleaved):
                    yield held
                yield ResponseBody(body=encoded, more_body=False)
                # Trailers may still follow a finished body.
                async for rest in outbound:
                    if isinstance(rest, _OFFLOADED_BODY):
                        raise OffloadedBodyAfterEncoding(_offloaded_after(coding))
                    yield rest
                return

            if buffered and buffered >= hold:
                # Past the floor with more body coming: commit to encoding, release
                # what was held through the compressor, and stream the rest. An empty
                # chunk cannot carry the response past a hold of zero, since there is
                # nothing yet to encode and nothing to deliver if there were.
                compressor = table[coding]()
                if not isinstance(compressor, StreamingCompressor):
                    # This codec can only make its output deliverable by ending the
                    # stream, so encoding here would hold the whole body inside it
                    # and deliver the response in one burst at the end. Unencoded
                    # instead, which costs bytes rather than the incremental
                    # delivery the app asked for by streaming.
                    for held in _released(start, interleaved):
                        yield held
                    yield ResponseBody(body=b"".join(prefix), more_body=True)
                    async for rest in outbound:
                        yield rest
                    return
                for held in _released(replace(start, headers=_encoded(start.headers, coding, None)), interleaved):
                    yield held
                yield ResponseBody(body=_deliverable(compressor, b"".join(prefix)), more_body=True)
                async for rest in outbound:
                    if isinstance(rest, _OFFLOADED_BODY):
                        raise OffloadedBodyAfterEncoding(_offloaded_after(coding))
                    if not isinstance(rest, ResponseBody):
                        yield rest
                    elif rest.more_body:
                        if encoded := _deliverable(compressor, rest.body):
                            yield ResponseBody(body=encoded, more_body=True)
                    else:
                        # Ending the stream is itself a flush, so the last chunk
                        # rides out on it rather than paying for a block of its own.
                        yield ResponseBody(body=compressor.compress(rest.body) + compressor.flush(), more_body=False)
                return

        # The stream ended without a final body event: the response is already
        # truncated, so release what was held rather than inventing an ending.
        for held in _released(start, interleaved):
            yield held
        if prefix:
            yield ResponseBody(body=b"".join(prefix), more_body=True)

    return wrap(outbound=rewritten)
