from __future__ import annotations

import asyncio
import hashlib
import logging
import mimetypes
import os
from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from pathlib import Path
from stat import S_ISREG
from types import MappingProxyType
from typing import assert_never

from without_asgi import headers
from without_asgi.compression import DEFAULT_COMPRESSORS
from without_asgi.compression import Compressor
from without_asgi.compression import is_compressible
from without_asgi.compression import negotiate_coding
from without_asgi.files import DEFAULT_CHUNK_SIZE
from without_asgi.files import no_body
from without_asgi.files import start_for
from without_asgi.files import stream_selection
from without_asgi.outbound import Outbound
from without_asgi.outbound import Response
from without_asgi.outbound import ResponseBody
from without_asgi.outbound import ResponseStart
from without_asgi.outbound import encode_response
from without_asgi.scope import HttpScope
from without_asgi.selection import NotModified
from without_asgi.selection import Selection
from without_asgi.selection import Span
from without_asgi.selection import Unsatisfiable
from without_asgi.selection import Whole
from without_asgi.selection import http_date
from without_asgi.selection import selection_for
from without_asgi.types import RawHeaders

# Serving a tree of static assets by *inventory* rather than by directory traversal.
# The tree is walked once at initialization into a mapping of key to `Asset`, so a
# request is a dictionary lookup and no filesystem path is ever derived from request
# input. That is the whole security argument: a traversal mount has to build a path
# from an untrusted key and then prove the result stayed inside the root, and that
# proof is where CVE-2023-29159 (a character-wise prefix comparison), CVE-2024-23334 (a
# flag that skipped the check), and the Windows drive-letter and reserved-device-name
# escapes all lived. There is no proof to get wrong when there is no derivation.

__all__ = [
    "NOT_FOUND",
    "Asset",
    "AssetChanged",
    "Inventory",
    "Representation",
    "content_hash",
    "inventory",
    "serve_asset",
    "size_and_mtime",
]

logger = logging.getLogger(__name__)

_HASH_CHUNK_SIZE = 1 << 20
_OCTET_STREAM = "application/octet-stream"
_CONTENT_TYPE = b"content-type"
_CONTENT_ENCODING = b"content-encoding"
_ACCEPT_RANGES = b"accept-ranges"
_ACCEPT_ENCODING = b"accept-encoding"
_ETAG = b"etag"
_LAST_MODIFIED = b"last-modified"
_CACHE_CONTROL = b"cache-control"
_VARY = b"vary"
_BYTES = b"bytes"
_LATIN1 = "latin-1"
_ASCII = "ascii"
# RFC 9110 §8.8.3: etagc is %x21 / %x23-7E, i.e. visible ASCII without the DQUOTE that
# delimits the tag. obs-text is permitted by the grammar and deliberately not accepted
# here, so a token cannot carry bytes that need a second opinion about encoding.
_ETAGC = frozenset(range(0x21, 0x7F)) - {0x22}
# The filename conventions nginx's `gzip_static`/`brotli_static` and WhiteNoise already
# use, so a build system that emits sidecars for either is understood as-is. A coding
# outside this table falls back to a dot plus its own name.
_SIDECAR_SUFFIXES: Mapping[bytes, str] = MappingProxyType({b"gzip": ".gz", b"zstd": ".zst", b"br": ".br"})
_NAMED_IN_WARNING = 5

# What `serve_asset` answers for a key the inventory does not hold. Public and
# injectable: the policy is the caller's, and `static_files` passes it straight through
# rather than keeping a second default in step with this one.
NOT_FOUND = Response(
    status=404,
    headers=((_CONTENT_TYPE, b"text/plain; charset=utf-8"),),
    body=b"not found\n",
)
_NO_ENCODINGS: Mapping[bytes, Representation] = MappingProxyType({})

type EtagFor = Callable[[str, Path, os.stat_result], bytes]


class AssetChanged(Exception):
    """
    An asset's bytes changed after the inventory was built.

    The inventory's contract is that nothing writes into the tree while the app runs.
    This is raised when that is observably false, before any `ResponseStart`, rather
    than framing a body whose length and validator describe different bytes.
    """


@dataclass(frozen=True, slots=True)
class Representation:
    """
    One selectable form of an asset: the identity bytes on disk, or a content coding.

    Each carries its **own** strong `etag`. Sharing one tag across codings is a real
    bug rather than an untidiness: a client holding the gzip copy would send
    `If-None-Match`, receive a `304`, and go on using bytes that are a different
    representation entirely.
    """

    size: int
    etag: bytes
    described: RawHeaders
    """What a `200` or `206` says about this representation."""
    revalidation: RawHeaders
    """Only what RFC 9110 §15.4.5 requires a `304` to repeat."""
    body: bytes | None = None
    """The encoded bytes, held in memory; `None` for the identity form, read from disk."""


@dataclass(frozen=True, slots=True)
class Asset:
    """One file in an `Inventory`, with every response header already computed."""

    path: Path
    last_modified: datetime
    identity: Representation
    encodings: Mapping[bytes, Representation] = field(default=_NO_ENCODINGS)


@dataclass(frozen=True, slots=True)
class Inventory:
    """A mapping of request key to `Asset`, built once by `inventory`."""

    assets: Mapping[str, Asset]

    def get(self, key: str) -> Asset | None:
        return self.assets.get(key)


def content_hash(key: str, path: Path, stat: os.stat_result) -> bytes:
    """
    A digest of the file's bytes: the default, and a validator strong on its own merits.

    Unlike a timestamp-derived tag it does not change when a rebuild rewrites an
    unchanged file, so clients do not refetch a bundle that did not change, and it is
    identical across replicas and machines.
    """
    digest = hashlib.blake2b(digest_size=16)
    with path.open("rb") as handle:
        while chunk := handle.read(_HASH_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest().encode(_ASCII)


def size_and_mtime(key: str, path: Path, stat: os.stat_result) -> bytes:
    """
    A validator from the `stat` alone, for a tree too large to read at startup.

    It is published as a *strong* tag, which rests entirely on the inventory's
    no-writes contract rather than on the bytes: a filesystem's timestamp granularity
    can be coarser than the interval between two writes, so this could not distinguish
    two versions of a file that the contract says cannot exist. `content_hash` needs no
    such assumption. Never `st_ino`, which would leak a filesystem internal into every
    response (Apache's FileETag default, CVE-2003-1418).
    """
    return b"%x-%x" % (stat.st_size, stat.st_mtime_ns)


def inventory(
    root: Path,
    *,
    etag_for: EtagFor = content_hash,
    index: str | None = None,
    cache_control: bytes | None = None,
    charset: str | None = "utf-8",
    encodings: Mapping[bytes, Callable[[], Compressor]] = DEFAULT_COMPRESSORS,
    compressible: Callable[[bytes | None], bool] = is_compressible,
) -> Inventory:
    """
    Walk `root` once and build the mapping `serve_asset` answers from.

    Every decision that could involve an attacker in a traversal design is made here
    instead, once, over a tree the operator assembled:

    - Only regular files are admitted, so a directory, fifo, or device is absent rather
      than a failure discovered mid-response.
    - Each entry is resolved and confirmed to be inside `root`; one that escapes
      **raises**, naming both ends. No flag relaxes this, because that flag is
      precisely aiohttp's CVE-2024-23334.
    - Symlinked *directories* are not descended into, so a cycle cannot hang the walk.
    - Content type, validators, and response headers are computed now, so serving hands
      an immutable tuple through rather than rebuilding one per request.

    Keys are relative POSIX paths with no leading slash (`"css/app.css"`). `index`
    installs an alias from a directory's key to the index file inside it, so `/guide/`
    reaches `guide/index.html`; every other directory key is simply absent, which is a
    `404` by omission. There is no directory listing, and none behind a flag.

    `etag_for` returns the opaque *token*; the quoting is added here, and a token
    holding characters illegal in an entity-tag is rejected, so a caller cannot emit a
    malformed validator.

    **Pre-compression.** For each asset whose media type `compressible` allows, a
    variant is built per coding in `encodings`, preferring a sidecar file the build
    system already produced (`app.css.br`, `app.css.gz`, `app.css.zst`, the convention
    nginx's `brotli_static` and WhiteNoise use) and compressing in memory only when one
    is missing or older than the asset it encodes. A missing sidecar is logged, because
    the level worth using for bytes compressed once and served forever is far slower
    than one worth paying per process start: brotli quality 11 runs at roughly a
    megabyte per second, so it belongs in the build, not in every replica's startup.
    Encoded bytes are held in memory, which also makes a `Range` over a compressed
    asset work correctly, something on-the-fly compression cannot do at all.

    Static assets are safe to compress: BREACH needs a response that both reflects
    attacker-controlled input and carries a secret, and a stylesheet does neither. That
    is why this uses `DEFAULT_COMPRESSORS` rather than the padded table this package
    ships for credential-bearing responses.

    This does blocking I/O, deliberately: it is assembly, not request handling. From an
    async lifespan, `await asyncio.to_thread(inventory, root)`. The result is a value,
    so a development loop that wants to pick up edits rebuilds one and swaps it, on a
    timer or a filesystem watch, rather than putting the walk on the request path.
    """
    base = root.resolve(strict=True)
    if not base.is_dir():
        raise NotADirectoryError(f"{base} is not a directory")
    found = _regular_files(base)
    compressed_here: list[str] = []
    assets = {
        key: _asset(key, path, stat, etag_for, cache_control, charset, encodings, compressible, compressed_here)
        for key, (path, stat) in _without_sidecars(found, encodings).items()
    }
    _report(compressed_here)
    return Inventory(assets=_with_index_aliases(assets, index))


def _regular_files(base: Path) -> dict[str, tuple[Path, os.stat_result]]:
    found: dict[str, tuple[Path, os.stat_result]] = {}
    for parent, _directories, names in base.walk():
        for name in names:
            path = parent / name
            resolved = path.resolve()
            if not resolved.is_relative_to(base):
                raise ValueError(f"{path} leaves the asset root: it resolves to {resolved}, outside {base}")
            try:
                stat = resolved.stat()
            except OSError as error:
                # A dangling symlink is the usual cause, and the bare error would name
                # the target rather than the link, leaving nothing to go and fix.
                raise ValueError(f"{path} cannot be read: it resolves to {resolved}, which {error.strerror}") from error
            # A fifo, socket, or device has no length to declare and no bytes to seek in,
            # so it is left out of the keyspace rather than failing on the request that
            # happens to name it.
            if S_ISREG(stat.st_mode):
                found[path.relative_to(base).as_posix()] = (resolved, stat)
    return found


def _without_sidecars(
    found: dict[str, tuple[Path, os.stat_result]],
    encodings: Mapping[bytes, Callable[[], Compressor]],
) -> dict[str, tuple[Path, os.stat_result]]:
    """
    Drop `app.css.br` when `app.css` is present: it is that asset's encoded form, not an
    asset of its own, and indexing it too would serve compressed bytes under a media
    type guessed from the wrong suffix.
    """
    suffixes = tuple(_sidecar_suffix(coding) for coding in encodings)
    return {
        key: entry
        for key, entry in found.items()
        if not any(key.endswith(suffix) and key[: -len(suffix)] in found for suffix in suffixes)
    }


def _sidecar_suffix(coding: bytes) -> str:
    return _SIDECAR_SUFFIXES.get(coding, f".{coding.decode(_ASCII)}")


def _asset(
    key: str,
    path: Path,
    stat: os.stat_result,
    etag_for: EtagFor,
    cache_control: bytes | None,
    charset: str | None,
    encodings: Mapping[bytes, Callable[[], Compressor]],
    compressible: Callable[[bytes | None], bool],
    compressed_here: list[str],
) -> Asset:
    token = _valid_token(etag_for(key, path, stat), key)
    content_type = _content_type(path, charset)
    modified = datetime.fromtimestamp(stat.st_mtime, UTC)
    encoded = (
        _encodings(key, path, stat, token, content_type, modified, cache_control, encodings, compressed_here)
        if compressible(content_type)
        else {}
    )
    # Vary only where the body genuinely depends on the request: stamping it on an
    # already-compressed image fragments every downstream cache key for nothing, which
    # is the bug filed against ngx_brotli as #97.
    identity = _representation(
        size=stat.st_size,
        token=token,
        content_type=content_type,
        coding=None,
        modified=modified,
        cache_control=cache_control,
        varies=bool(encoded),
    )
    return Asset(
        path=path,
        last_modified=modified,
        identity=identity,
        encodings=MappingProxyType(encoded) if encoded else _NO_ENCODINGS,
    )


def _encodings(
    key: str,
    path: Path,
    stat: os.stat_result,
    token: bytes,
    content_type: bytes,
    modified: datetime,
    cache_control: bytes | None,
    encodings: Mapping[bytes, Callable[[], Compressor]],
    compressed_here: list[str],
) -> dict[bytes, Representation]:
    if not encodings:
        return {}
    built: dict[bytes, Representation] = {}
    identity: bytes | None = None
    for coding, make in encodings.items():
        body = _sidecar(path, coding, stat)
        built_here = body is None
        if body is None:
            if identity is None:
                identity = path.read_bytes()
            body = _compressed(identity, make)
        # A coding that made the body no smaller costs the client a decode for nothing.
        if len(body) >= stat.st_size:
            continue
        # Reported only once the variant is kept, so the warning names work a sidecar
        # would actually have saved rather than encodings that were discarded anyway.
        if built_here:
            compressed_here.append(f"{key} ({coding.decode(_ASCII)})")
        built[coding] = _representation(
            size=len(body),
            token=token,
            content_type=content_type,
            coding=coding,
            modified=modified,
            cache_control=cache_control,
            varies=True,
            body=body,
        )
    return built


def _sidecar(path: Path, coding: bytes, stat: os.stat_result) -> bytes | None:
    candidate = path.with_name(path.name + _sidecar_suffix(coding))
    try:
        sidecar = candidate.stat()
    except OSError:
        return None
    # A sidecar older than what it encodes describes bytes the asset no longer has.
    # Serving it under a validator derived from the *current* bytes would hand the
    # client stale content beneath a tag claiming otherwise, so it is recompressed.
    if not S_ISREG(sidecar.st_mode) or sidecar.st_mtime_ns < stat.st_mtime_ns:
        return None
    return candidate.read_bytes()


def _compressed(data: bytes, make: Callable[[], Compressor]) -> bytes:
    compressor = make()
    return compressor.compress(data) + compressor.flush()


def _representation(
    *,
    size: int,
    token: bytes,
    content_type: bytes,
    coding: bytes | None,
    modified: datetime,
    cache_control: bytes | None,
    varies: bool,
    body: bytes | None = None,
) -> Representation:
    # Each coding is its own representation, so it gets its own strong tag rather than
    # sharing the identity one.
    etag = b'"%s"' % token if coding is None else b'"%s-%s"' % (token, coding)
    caching: RawHeaders = () if cache_control is None else ((_CACHE_CONTROL, cache_control),)
    vary: RawHeaders = ((_VARY, _ACCEPT_ENCODING),) if varies else ()
    encoding: RawHeaders = () if coding is None else ((_CONTENT_ENCODING, coding),)
    validators: RawHeaders = ((_ETAG, etag), (_LAST_MODIFIED, http_date(modified)))
    return Representation(
        size=size,
        etag=etag,
        described=((_CONTENT_TYPE, content_type), *encoding, *validators, (_ACCEPT_RANGES, _BYTES), *caching, *vary),
        revalidation=(*validators, *caching, *vary),
        body=body,
    )


def _content_type(path: Path, charset: str | None) -> bytes:
    guessed = mimetypes.guess_file_type(path)[0] or _OCTET_STREAM
    # A textual asset with no charset leaves the encoding to the recipient's guess,
    # which is how a UTF-8 stylesheet ends up rendering as mojibake.
    if charset is not None and guessed.startswith("text/"):
        guessed = f"{guessed}; charset={charset}"
    return guessed.encode(_LATIN1)


def _valid_token(token: bytes, key: str) -> bytes:
    if not token or not _ETAGC.issuperset(token):
        raise ValueError(f"the entity-tag token for {key!r} is not a valid etag: {token!r}")
    return token


def _with_index_aliases(assets: dict[str, Asset], index: str | None) -> Mapping[str, Asset]:
    if index is None:
        return assets
    for key, asset in list(assets.items()):
        directory, separator, name = key.rpartition("/")
        # A root-level index has no directory key to alias; it is reached by naming it,
        # which is what a router `fallback` serving a single-page app does.
        if name == index and separator and directory not in assets:
            assets[directory] = asset
    return assets


def _report(compressed_here: list[str]) -> None:
    if not compressed_here:
        return
    named = ", ".join(compressed_here[:_NAMED_IN_WARNING])
    rest = len(compressed_here) - _NAMED_IN_WARNING
    logger.warning(
        f"Compressed {len(compressed_here)} asset representation(s) at startup because no sidecar "
        f"file was found beside them ({named}{f', and {rest} more' if rest > 0 else ''}). "
        "Emit these from the build system, a pre-commit hook, or any other step that runs before "
        "startup: a level worth using for bytes compressed once and served forever is far slower "
        "than one worth paying on every process start, and this cost is paid again by every replica."
    )


async def serve_asset(
    scope: HttpScope,
    assets: Inventory,
    key: str,
    *,
    not_found: Response = NOT_FOUND,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> AsyncIterator[Outbound]:
    """
    Answer a request for `key` out of `assets`: `200`, `206`, `304`, `416`, or `404`.

    `key` is used only to look up an entry, never to build a path, so every traversal
    payload (`..`, a decoded `%2F` or `%00`, an absolute path, a Windows drive letter,
    a reserved device name) is simply a key that is not present.

    The content coding is negotiated against the variants the inventory holds, and the
    conditional and range rules are then applied to *that* representation: its size, its
    own strong validator. A `304` or `416` is decided from the inventory alone and
    touches no file at all, which is what makes revalidation, the common request for a
    cached asset, cost a dictionary lookup and a byte comparison.

    When identity bytes are owed the `stat` runs before any `ResponseStart`, and its
    size is checked against the inventory's. A disagreement means the tree was written
    to while the app was running, which the inventory's contract forbids, and raises
    `AssetChanged` while nothing is committed rather than framing a response whose
    length and validator describe different bytes.
    """
    asset = assets.get(key)
    if asset is None:
        return _replay(encode_response(not_found))
    chosen = _negotiated(asset, scope.headers)
    selection = selection_for(
        size=chosen.size,
        method=scope.method,
        request_headers=scope.headers,
        etag=chosen.etag,
        last_modified=asset.last_modified,
    )
    if chosen.body is not None:
        return _from_memory(chosen, selection, chunk_size)
    if isinstance(selection, Whole | Span):
        stat = await asyncio.to_thread(asset.path.stat)
        if stat.st_size != chosen.size:
            raise AssetChanged(
                f"{asset.path} is {stat.st_size} bytes but the inventory recorded {chosen.size}; "
                "something wrote into the asset root while the app was running"
            )
    return stream_selection(asset.path, selection, chosen.size, chosen.described, chosen.revalidation, chunk_size)


def _negotiated(asset: Asset, request_headers: RawHeaders) -> Representation:
    if not asset.encodings:
        return asset.identity
    # A list-valued field's value is all its occurrences joined (RFC 9110 §5.2), and the
    # mapping's order is the server's preference order.
    offered = headers.get_all(request_headers, _ACCEPT_ENCODING)
    coding = negotiate_coding(b",".join(offered) if offered else None, tuple(asset.encodings))
    return asset.identity if coding is None else asset.encodings[coding]


def _from_memory(chosen: Representation, selection: Selection, chunk_size: int) -> AsyncIterator[Outbound]:
    body = chosen.body
    if body is None:  # pragma: no cover - only reached with an identity representation
        raise AssertionError
    start = start_for(selection, chosen.size, chosen.described, chosen.revalidation)
    match selection:
        case Whole():
            return _stream_bytes(start, body, chunk_size)
        case Span(first, last):
            return _stream_bytes(start, body[first : last + 1], chunk_size)
        case NotModified() | Unsatisfiable():
            return no_body(start)
        case _ as unreachable:
            assert_never(unreachable)


async def _stream_bytes(start: ResponseStart, body: bytes, chunk_size: int) -> AsyncIterator[Outbound]:
    yield start
    for offset in range(0, len(body), chunk_size):
        yield ResponseBody(body=body[offset : offset + chunk_size], more_body=True)
    yield ResponseBody(body=b"", more_body=False)  # pragma: no mutate - values equal the field defaults


async def _replay(events: tuple[Outbound, ...]) -> AsyncIterator[Outbound]:
    for event in events:
        yield event
