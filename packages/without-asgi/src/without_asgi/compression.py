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

import brotli
from without import Stream

from without_asgi import headers
from without_asgi.outbound import Outbound
from without_asgi.outbound import ResponseBody
from without_asgi.outbound import ResponseStart
from without_asgi.routing import HttpMiddleware
from without_asgi.routing import wrap
from without_asgi.scope import HttpScope
from without_asgi.types import RawHeaders

__all__ = [
    "DEFAULT_COMPRESSORS",
    "DYNAMIC_BROTLI_QUALITY",
    "GZIP_CONTAINER",
    "MAX_RANDOM_BYTES",
    "PADDED_COMPRESSORS",
    "Compressor",
    "brotli_compressor",
    "compress",
    "is_compressible",
    "negotiate_coding",
    "padded_gzip_compressor",
    "padded_zstd_compressor",
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
    `zstd.ZstdCompressor` satisfy it as-is; a third-party codec plugs in with
    whatever thin adapter its own surface needs.

    The same shape drives `without-http`'s request-side `compressing`, which
    re-exports this protocol, so one adapter serves both directions.
    """

    def compress(self, data: bytes, /) -> bytes: ...
    def flush(self) -> bytes: ...


def _gzip_compressor() -> Compressor:
    return zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, GZIP_CONTAINER)


class _RawBrotliCompressor(Protocol):
    """The slice of `brotli.Compressor` the adapter drives (the bindings ship no types)."""

    def process(self, data: bytes, /) -> bytes: ...
    def finish(self) -> bytes: ...


@dataclass(slots=True, eq=False)
class _BrotliCompressor:
    """
    Adapt `brotli.Compressor` to the `Compressor` shape.

    Brotli's bindings spell the incremental surface `process`/`finish` (their `flush`
    is a mid-stream flush that keeps the stream open), so the adapter maps `compress`
    to `process` and `flush` to `finish`.
    """

    _raw: _RawBrotliCompressor

    def compress(self, data: bytes, /) -> bytes:
        return self._raw.process(data)

    def flush(self) -> bytes:
        return self._raw.finish()


# Brotli's own default is 11, the maximum, which is tuned for compressing an asset
# once and serving it many times. A response encoded per request is the other case,
# so the default here is the quality dynamic content is served at. On response-sized
# bodies the ratio does not suffer for it: measured on a 2 KB JSON body and a 2.7 KB
# HTML one, quality 5 matched or beat 11 outright, since 11's larger window and
# costlier search have little to find in a few kilobytes.
DYNAMIC_BROTLI_QUALITY = 5


def brotli_compressor(quality: int = DYNAMIC_BROTLI_QUALITY) -> Compressor:
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
        b"zstd": zstd.ZstdCompressor,
        b"gzip": _gzip_compressor,
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

    _inner: Compressor
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

    def flush(self) -> bytes:
        return self._splice(self._inner.flush())


@dataclass(slots=True, eq=False)
class _PaddedZstdCompressor:
    """
    A zstd `Compressor` that leads with a random-length skippable frame.

    zstd's answer to gzip's filename field: RFC 8878 §3.1.2 reserves a frame kind a
    decoder must skip over, so a random-length one carries the same noise into the
    response length. Everything in `_PaddedGzipCompressor` about why applies here.
    """

    _inner: Compressor
    _preamble: bytes

    def _lead(self, chunk: bytes) -> bytes:
        preamble, self._preamble = self._preamble, b""
        return preamble + chunk

    def compress(self, data: bytes, /) -> bytes:
        return self._lead(self._inner.compress(data))

    def flush(self) -> bytes:
        return self._lead(self._inner.flush())


def padded_gzip_compressor(max_random_bytes: int = MAX_RANDOM_BYTES) -> Compressor:
    """A gzip `Compressor` whose output length carries up to `max_random_bytes` of noise."""
    return _PaddedGzipCompressor(_gzip_compressor(), _random_run(max_random_bytes))


def padded_zstd_compressor(max_random_bytes: int = MAX_RANDOM_BYTES) -> Compressor:
    """A zstd `Compressor` whose output length carries up to `max_random_bytes` of noise."""
    payload = _random_run(max_random_bytes)
    preamble = struct.pack("<II", _ZSTD_SKIPPABLE_FRAME, len(payload)) + payload
    return _PaddedZstdCompressor(zstd.ZstdCompressor(), preamble)


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
        b"application/wasm",
        b"application/x-javascript",
        b"application/x-ndjson",
        b"application/xml",
    }
)
_COMPRESSIBLE_SUFFIXES = (b"+json", b"+text", b"+xml")


def is_compressible(content_type: bytes | None) -> bool:
    """
    Whether a response of this media type is worth compressing: `compress`'s
    default policy, replaceable through its `compressible` argument.

    An allowlist, the shape nginx's `gzip_types` takes, rather than a list of
    exclusions: `text/*`, the structured suffixes (`+json`, `+xml`, `+text`, which
    is how `application/problem+json` and every other vendor type arrives), and a
    short list of the remaining types worth the CPU. A type nobody listed is left
    alone, so the failure mode of an unrecognized type is a larger response rather
    than cycles burnt re-compressing a JPEG.

    Two exceptions to the shape. A response with no `content-type` is not
    compressed, since compression here is driven by the declared media type and
    there is nothing to drive it. And `text/event-stream` is excluded despite the
    `text/` prefix: an incremental compressor holds bytes back until it has enough
    to emit a block, which is exactly the latency an event stream exists to avoid,
    so compressing one stalls events rather than shrinking them.
    """
    if content_type is None:
        return False
    media = content_type.split(b";")[0].strip().lower()
    if media == b"text/event-stream":
        return False
    if media.startswith(b"text/"):
        return True
    if media.endswith(_COMPRESSIBLE_SUFFIXES):
        return True
    return media in _COMPRESSIBLE_TYPES


# `204` and `304` carry no content by definition, so there is nothing to encode and
# a `content-encoding` on either would describe a body that does not exist.
_BODYLESS_STATUSES = frozenset({204, 304})


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
    if start.status in _BODYLESS_STATUSES:
        return False
    if headers.first(start.headers, b"content-encoding") is not None:
        return False
    return compressible(headers.first(start.headers, b"content-type"))


def compress(
    compressors: Mapping[bytes, Callable[[], Compressor]] = DEFAULT_COMPRESSORS,
    *,
    minimum_size: int = 500,
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
    response rather than the request: a `204` or `304` carries no content, a
    response already carrying `content-encoding` is already encoded, and the media
    type has to be worth compressing (`compressible`, defaulting to
    `is_compressible`). Every candidate gets `Vary: Accept-Encoding` whether or
    not this particular client got a compressed body, so a shared cache keys on
    the header that decides the answer; a non-candidate gets no `Vary`, since
    nothing about it varies.

    A body shorter than `minimum_size` is sent unencoded, because gzip's framing
    costs more than a few hundred bytes of text saves. The gate is applied to the
    bytes rather than to `content-length`, so it holds for a streaming response
    that never declares one: the middleware buffers up to `minimum_size` of the
    body to find out which side of the gate it falls on, then either releases the
    prefix untouched or commits to encoding and streams the rest through the
    compressor chunk by chunk. A response that arrives whole gets an exact
    `content-length` for its encoded body; one still streaming when it crosses the
    gate loses `content-length` and is framed by the transport, as any streaming
    body is. A strong `etag` is weakened when the body is encoded (see `_weakened`).

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

        if not _is_candidate(start, compressible):
            yield start
            async for event in outbound:
                yield event
            return

        start = replace(start, headers=_varying(start.headers))
        coding = negotiate_coding(_accept_encoding(scope.headers), available)
        if coding is None:
            yield start
            async for event in outbound:
                yield event
            return

        # Hold the head and buffer the body's prefix until the size gate resolves:
        # committing to a `content-encoding` before knowing the body is big enough
        # to deserve one is the one decision that cannot be taken back.
        prefix: list[bytes] = []
        buffered = 0
        async for event in outbound:
            if not isinstance(event, ResponseBody):
                # An offloaded body (a path send, a zero-copy send) is bytes this
                # middleware never sees, so there is nothing to encode. Release the
                # prefix only if there was one: an empty body event before an offload
                # is a message the app never sent.
                yield start
                if prefix:
                    yield ResponseBody(body=b"".join(prefix), more_body=True)
                yield event
                async for rest in outbound:
                    yield rest
                return
            prefix.append(event.body)
            buffered += len(event.body)

            if not event.more_body:
                body = b"".join(prefix)
                if buffered < minimum_size:
                    yield start
                    yield ResponseBody(body=body, more_body=False)
                else:
                    compressor = table[coding]()
                    encoded = compressor.compress(body) + compressor.flush()
                    yield replace(start, headers=_encoded(start.headers, coding, len(encoded)))
                    yield ResponseBody(body=encoded, more_body=False)
                # Trailers may still follow a finished body.
                async for rest in outbound:
                    yield rest
                return

            if buffered >= minimum_size:
                # Past the gate with more body coming: commit to encoding, release
                # the prefix through the compressor, and stream the rest.
                compressor = table[coding]()
                yield replace(start, headers=_encoded(start.headers, coding, None))
                if encoded := compressor.compress(b"".join(prefix)):
                    yield ResponseBody(body=encoded, more_body=True)
                async for rest in outbound:
                    if not isinstance(rest, ResponseBody):
                        yield rest
                        continue
                    if encoded := compressor.compress(rest.body):
                        yield ResponseBody(body=encoded, more_body=True)
                    if not rest.more_body:
                        yield ResponseBody(body=compressor.flush(), more_body=False)
                return

        # The stream ended without a final body event: the response is already
        # truncated, so release what was held rather than inventing an ending.
        yield start
        if prefix:
            yield ResponseBody(body=b"".join(prefix), more_body=True)

    return wrap(outbound=rewritten)
