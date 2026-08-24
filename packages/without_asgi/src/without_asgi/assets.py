from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from pathlib import Path
from stat import S_ISDIR
from stat import S_ISREG
from types import MappingProxyType
from typing import NoReturn
from typing import assert_never
from urllib.parse import quote

from without import stream_from_iterable

from without_asgi.compression import DEFAULT_COMPRESSORS
from without_asgi.compression import Compressor
from without_asgi.compression import _accept_encoding
from without_asgi.compression import is_compressible
from without_asgi.compression import negotiate_coding
from without_asgi.files import _CONTENT_LENGTH
from without_asgi.files import _CONTENT_TYPE
from without_asgi.files import DEFAULT_CHUNK_SIZE
from without_asgi.files import describing
from without_asgi.files import guessed_type
from without_asgi.files import no_body
from without_asgi.files import size_and_mtime_token
from without_asgi.files import start_for
from without_asgi.files import stream_selection
from without_asgi.outbound import Outbound
from without_asgi.outbound import Response
from without_asgi.outbound import ResponseBody
from without_asgi.outbound import ResponseStart
from without_asgi.outbound import encode_response
from without_asgi.scope import HttpScope
from without_asgi.selection import Head
from without_asgi.selection import NotModified
from without_asgi.selection import Selection
from without_asgi.selection import Span
from without_asgi.selection import Unsatisfiable
from without_asgi.selection import Whole
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
    "IMMUTABLE_CACHE_CONTROL",
    "NOT_FOUND",
    "REVALIDATE_CACHE_CONTROL",
    "STATIC_ASSET_HEADERS",
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

_CACHE_CONTROL = b"cache-control"
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
_LOCATION = b"location"
# Neither `301` nor a cached one: the inventory does not know what prefix it is mounted
# under, so the URL shape it is redirecting *to* is not its own to make permanent. This
# is why WhiteNoise's `redirect` is a `302` as well.
_MOVED = 302

# What `serve_asset` answers for a key the inventory does not hold. Public and
# injectable: the policy is the caller's, and `static_files` passes it straight through
# rather than keeping a second default in step with this one.
_NOT_FOUND_BODY = b"not found\n"
NOT_FOUND = Response(
    status=404,
    headers=(
        (_CONTENT_TYPE, b"text/plain; charset=utf-8"),
        (_CONTENT_LENGTH, b"%d" % len(_NOT_FOUND_BODY)),
    ),
    body=_NOT_FOUND_BODY,
)
# Store the response, but revalidate before every reuse. Correct for any tree, whatever
# its filenames, which is why it is the default. It is not the obvious performance loss
# it looks like *here*: an inventory answers a conditional request from memory, with no
# syscall, so the cost is one round trip rather than a read. Stating it also beats saying
# nothing, since a response carrying no `cache-control` falls to the recipient's heuristic
# freshness (RFC 9111 §4.2.2), which invents a staleness window out of `Last-Modified`
# that nobody chose and no test exercises.
REVALIDATE_CACHE_CONTROL = b"public, no-cache"

# Cache for a year and never revalidate (RFC 8246). Opt in with `headers.replace` *only*
# for a tree of fingerprinted filenames, ones carrying their content hash
# (`app.a1b2c3d4.css`), where a new build writes a new URL and the old entry is simply
# never requested again. On stable names it pins a stale copy in every browser that saw
# it, for a year, with no way to reach those clients, because `immutable` stops the client
# sending even the conditional request a `304` would answer. That failure is invisible
# until someone ships a fix nobody receives, which is why it is not the default.
IMMUTABLE_CACHE_CONTROL = b"public, max-age=31536000, immutable"

# What `inventory` applies to every asset unless told otherwise: the caching policy that
# is right whatever the tree looks like, plus `nosniff`, which stops a browser
# second-guessing the media type the inventory computed and is what turns a file served
# as `text/plain` into script. Exported so a caller extends or amends it with the ordinary
# `headers` helpers rather than retyping it.
STATIC_ASSET_HEADERS: RawHeaders = (
    (_CACHE_CONTROL, REVALIDATE_CACHE_CONTROL),
    (b"x-content-type-options", b"nosniff"),
)

_NO_ENCODINGS: Mapping[bytes, Representation] = MappingProxyType({})

type EtagFor = Callable[[str, Path, os.stat_result], bytes]


@dataclass(frozen=True, slots=True)
class _Policy:
    """
    What `inventory`'s keyword arguments settle, parsed once and threaded as one value.

    Every builder below reads the same record, so a knob added to `inventory` reaches
    them without editing four signatures, and the sidecar filter and the asset builder
    cannot drift on what counts as encodable.
    """

    etag_for: EtagFor
    headers: RawHeaders
    charset: str | None
    encodings: Mapping[bytes, Callable[[], Compressor]]
    compressible: Callable[[bytes | None], bool]

    def encodable(self, path: Path) -> bool:
        content_type, stored = guessed_type(path, self.charset)
        # Bytes already in a content coding are not encoded again: stacking gzip on
        # `logo.svgz` costs the client two unwraps to reach the same SVG.
        return stored is None and self.compressible(content_type)


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
    """
    What a `304` repeats, as `describing` assembles it: RFC 9110 §15.4.5's required
    fields, plus the `content-type` and `content-encoding` naming *which* stored variant
    is being revalidated. The coding settles that for an already-encoded variant; the
    type settles it for a representation with no variants at all, a PNG or a font or a
    video, which carries no coding to read.
    """
    body: bytes | None = None
    """The encoded bytes, held in memory; `None` for the identity form, read from disk."""


@dataclass(frozen=True, slots=True)
class Asset:
    """One file in an `Inventory`, with every response header already computed."""

    path: Path
    last_modified: datetime
    identity: Representation
    encodings: Mapping[bytes, Representation] = _NO_ENCODINGS
    codings: tuple[bytes, ...] = ()
    """
    `encodings`' keys in the server's preference order, which is what
    `negotiate_coding` takes. Held rather than derived per request: it is a constant of
    the asset, and rebuilding it on every request for every compressible asset is an
    allocation on the hot path buying nothing.
    """
    needs_trailing_slash: bool = False
    """
    Whether this key reaches a directory's index under a URL that is missing the
    trailing slash, and so should be redirected rather than answered. See
    `_slash_redirect`.
    """


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
    with path.open("rb") as handle:
        digest = hashlib.file_digest(handle, lambda: hashlib.blake2b(digest_size=16))
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
    return size_and_mtime_token(stat.st_size, stat.st_mtime_ns)


def inventory(
    root: Path,
    *,
    etag_for: EtagFor = content_hash,
    index: str | None = None,
    headers: RawHeaders = STATIC_ASSET_HEADERS,
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
    - A directory that cannot be read **raises**, rather than contributing nothing and
      leaving the inventory silently short every asset beneath it.
    - Each entry is resolved and confirmed to be inside `root`; one that escapes
      **raises**, naming both ends. No flag relaxes this, because that flag is
      precisely aiohttp's CVE-2024-23334.
    - A symlinked *directory* **raises**. Descending it could cycle and hang the walk,
      and skipping it is the silent shortfall again, since `Path.walk` reports one among
      the filenames rather than the directories.
    - Content type, validators, and response headers are computed now, so serving hands
      an immutable tuple through rather than rebuilding one per request.

    Keys are relative POSIX paths with no leading slash (`"css/app.css"`). `index`
    installs an alias from a directory's key to the index file inside it, under both
    `"guide"` and `"guide/"` so the keyspace does not depend on whether the shell above
    strips a trailing slash; `/guide/` reaches `guide/index.html`, and the slash-less
    `/guide` gets a `302` to it rather than the document itself (see `_slash_redirect`).
    Every other directory key is simply absent, which is a `404` by omission. There is
    no directory listing, and none behind a flag.

    A file whose suffixes name a content coding as well as a media type (`logo.svgz`,
    `bundle.tar.gz`) is served with that `content-encoding` and is not encoded again.

    `etag_for` returns the opaque *token*; the quoting is added here, and a token
    holding characters illegal in an entity-tag is rejected, so a caller cannot emit a
    malformed validator.

    `headers` are prepended to every asset's response, on both what a `200` announces
    and what a `304` repeats, since a policy header is needed by the browser reading the
    response back out of cache too. They default to `STATIC_ASSET_HEADERS`:
    `REVALIDATE_CACHE_CONTROL` plus `x-content-type-options: nosniff`. That caching
    policy is correct whatever the tree's filenames look like, and cheap here, since a
    revalidation is answered from memory with no syscall at all.

    Where the tree holds **fingerprinted** filenames, ones carrying a content hash
    (`app.a1b2c3d4.css`), a new build writes a new URL and the old entry is never
    requested again, so the round trip buys nothing and `IMMUTABLE_CACHE_CONTROL` is
    worth opting into. Do not reach for it otherwise: on stable names it pins a stale
    copy in every browser that saw it, for a year, with no way to reach those clients.
    Amend or extend with the `headers` module's ordinary helpers, rather than retyping:

    ```python
    inventory(root, headers=headers.replace(
        STATIC_ASSET_HEADERS, b"cache-control", IMMUTABLE_CACHE_CONTROL))
    inventory(root, headers=headers.add(
        STATIC_ASSET_HEADERS, b"cross-origin-resource-policy", b"same-origin"))
    ```

    **Pre-compression.** For each asset whose media type `compressible` allows, a
    variant is built per coding in `encodings`, preferring a sidecar file the build
    system already produced (`app.css.br`, `app.css.gz`, `app.css.zst`, the convention
    nginx's `brotli_static` and WhiteNoise use) and compressing in memory only when one
    is missing or older than the asset it encodes. A sidecar is recognized as one only
    beside an asset that is *itself* encoded, so a `data.tar.gz` published alongside its
    own `data.tar` keeps its URL rather than disappearing into a variant that a
    non-compressible media type never builds. A missing sidecar is logged, because
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
    policy = _Policy(
        etag_for=etag_for,
        headers=headers,
        charset=charset,
        encodings=encodings,
        compressible=compressible,
    )
    built = {
        key: _asset(key, path, stat, policy)
        for key, (path, stat) in _without_sidecars(_regular_files(base), policy).items()
    }
    _report([named for (_, compressed) in built.values() for named in compressed])
    assets = {key: asset for key, (asset, _) in built.items()}
    return Inventory(assets=MappingProxyType(dict(_with_index_aliases(assets, index))))


def _unreadable(error: OSError) -> NoReturn:
    # `Path.walk` ignores a failed `scandir` by default, so an unreadable directory
    # yields nothing and says nothing: the inventory is quietly short every asset
    # beneath it and answers `404` for them in production. The walk raises on a dangling
    # symlink and on one escaping the root; a directory it cannot open is the same class
    # of tree problem and gets the same treatment.
    raise error


def _regular_files(base: Path) -> dict[str, tuple[Path, os.stat_result]]:
    found: dict[str, tuple[Path, os.stat_result]] = {}
    for parent, _directories, names in base.walk(on_error=_unreadable):
        for name in names:
            path = parent / name
            admitted = _admitted(path, base)
            if admitted is not None:
                found[path.relative_to(base).as_posix()] = admitted
    return found


def _admitted(path: Path, base: Path) -> tuple[Path, os.stat_result] | None:
    """
    The resolved path and `stat` of one walked entry, or `None` for one not served.

    Raises for a tree problem an operator has to fix, and returns `None` only for an
    entry that is fine to leave out of the keyspace.
    """
    resolved = path.resolve()
    if not resolved.is_relative_to(base):
        raise ValueError(f"{path} leaves the asset root: it resolves to {resolved}, outside {base}")
    try:
        stat = resolved.stat()
    except OSError as error:
        # A dangling symlink is the usual cause, and the bare error would name the target
        # rather than the link, leaving nothing to go and fix.
        raise ValueError(f"{path} cannot be read: it resolves to {resolved}, which {error.strerror}") from error
    if S_ISREG(stat.st_mode):
        return resolved, stat
    if S_ISDIR(stat.st_mode):
        # `Path.walk` reports a symlinked directory among the *filenames* rather than
        # descending it, so this is the only way a directory reaches here. Dropping it
        # with the fifos would leave the inventory silently short every asset beneath it,
        # which is what `_unreadable` refuses for a directory that cannot be opened.
        raise ValueError(
            f"{path} is a symlink to the directory {resolved}, which the walk does not descend into, "
            "so every asset beneath it would be missing. Replace the link with a real directory, or "
            "move what it points at into the root."
        )
    # A fifo, socket, or device has no length to declare and no bytes to seek in, so it is
    # left out of the keyspace rather than failing on the request that happens to name it.
    return None


def _without_sidecars(
    found: dict[str, tuple[Path, os.stat_result]],
    policy: _Policy,
) -> dict[str, tuple[Path, os.stat_result]]:
    """
    Drop `app.css.br` when `app.css` is present: it is that asset's encoded form, not an
    asset of its own, and publishing it too would hand the same bytes a second URL,
    outside negotiation, under a validator unrelated to the asset's.

    Only where the asset beside it is one `_encodings` will actually build variants for.
    A media type this never compresses has no variant for a sidecar to *become*, so
    suppressing it would take those bytes out of the keyspace and put them back nowhere:
    a `data.tar.gz` published beside its own `data.tar` would be a `404`, and a silent
    one, since nothing about a tarball is wrong enough to say anything about at startup.
    On its own that file is already an asset in good standing, `application/x-tar` with
    `content-encoding: gzip`, so the presence of a second file is the last thing that
    should decide whether it has a URL.

    Where the asset *is* encoded, the suffixes are a **fixed** set, deliberately not the
    active `encodings` table. Key the suppression on the table and a coding's absence
    exposes its sidecars: with `encodings={b"gzip": ...}` an `app.css.br` beside
    `app.css` becomes an asset in its own right, and with `encodings={}` every sidecar in
    the tree does. `.zst` is worse still, because `mimetypes` has no entry for it, so
    `app.css.zst` would go out as `application/zstd` at a URL the build system's naming
    makes guessable. This is why WhiteNoise's `is_compressed_variant` tests a literal
    `(".gz", ".br")` rather than anything configurable. A coding outside the table still
    suppresses its own sidecars, so a custom one loses nothing.
    """
    suffixes = frozenset(_SIDECAR_SUFFIXES.values()) | {_sidecar_suffix(coding) for coding in policy.encodings}
    return {
        key: entry
        for key, entry in found.items()
        if not any(key.endswith(suffix) and _has_variants(found, key[: -len(suffix)], policy) for suffix in suffixes)
    }


def _has_variants(found: Mapping[str, tuple[Path, os.stat_result]], key: str, policy: _Policy) -> bool:
    """Whether `found` holds `key` and `_encodings` will build variants for it."""
    entry = found.get(key)
    if entry is None:
        return False
    path, _ = entry
    return policy.encodable(path)


def _sidecar_suffix(coding: bytes) -> str:
    return _SIDECAR_SUFFIXES.get(coding, f".{coding.decode(_ASCII)}")


def _asset(key: str, path: Path, stat: os.stat_result, policy: _Policy) -> tuple[Asset, list[str]]:
    """The asset for one file, and the names of any variants compressed here rather than read."""
    token = _valid_token(policy.etag_for(key, path, stat), key)
    content_type, stored = guessed_type(path, policy.charset)
    modified = datetime.fromtimestamp(stat.st_mtime, UTC)
    encoded, compressed_here = (
        _encodings(key, path, stat, token, content_type, modified, policy) if policy.encodable(path) else ({}, [])
    )
    identity = _representation(
        size=stat.st_size,
        etag=_etag(token),
        content_type=content_type,
        coding=stored,
        modified=modified,
        headers=policy.headers,
        varies=bool(encoded),
    )
    asset = Asset(
        path=path,
        last_modified=modified,
        identity=identity,
        encodings=MappingProxyType(encoded),
        codings=tuple(encoded),
    )
    return asset, compressed_here


def _encodings(
    key: str,
    path: Path,
    stat: os.stat_result,
    token: bytes,
    content_type: bytes,
    modified: datetime,
    policy: _Policy,
) -> tuple[dict[bytes, Representation], list[str]]:
    built: dict[bytes, Representation] = {}
    compressed_here: list[str] = []
    identity: bytes | None = None
    for coding, make in policy.encodings.items():
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
            etag=_etag(token, coding),
            content_type=content_type,
            coding=coding,
            modified=modified,
            headers=policy.headers,
            varies=True,
            body=body,
        )
    return built, compressed_here


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


def _etag(token: bytes, coding: bytes | None = None) -> bytes:
    """
    Quote `token` as an entity-tag, suffixed for a negotiated variant.

    Each coding is its own representation, so it gets its own strong tag rather than
    sharing the identity one. A file *stored* in a coding is not a variant of anything:
    it is the only representation there is, so it keeps the bare tag.
    """
    return b'"%s"' % token if coding is None else b'"%s-%s"' % (token, coding)


def _representation(
    *,
    size: int,
    etag: bytes,
    content_type: bytes,
    coding: bytes | None,
    modified: datetime,
    headers: RawHeaders,
    varies: bool,
    body: bytes | None = None,
) -> Representation:
    described, revalidation = describing(
        headers=headers,
        content_type=content_type,
        coding=coding,
        etag=etag,
        modified=modified,
        varies=varies,
    )
    return Representation(size=size, etag=etag, described=described, revalidation=revalidation, body=body)


def _valid_token(token: bytes, key: str) -> bytes:
    if not token or not _ETAGC.issuperset(token):
        raise ValueError(f"the entity-tag token for {key!r} is not a valid etag: {token!r}")
    return token


def _with_index_aliases(assets: dict[str, Asset], index: str | None) -> Mapping[str, Asset]:
    """
    Alias a directory's key to the index file inside it, in **both** spellings.

    `"guide"` and `"guide/"` are registered together so the keyspace does not depend on
    how the shell above happens to normalize a path. `without-web`'s `split_path` strips
    a trailing slash, so `/assets/guide/` arrives as `"guide"`; a plain-ASGI shell
    slicing off a prefix keeps it, so the same URL arrives as `"guide/"`. Keying only
    the slash-less form would make the inventory silently correct under one shell and a
    `404` under the other.

    Only the slash-less key is marked for redirection: a request that already carried
    the slash is at the canonical URL and is simply answered.
    """
    if index is None:
        return assets
    for key, asset in list(assets.items()):
        directory, separator, name = key.rpartition("/")
        # A root-level index has no directory key to alias; it is reached by naming it,
        # which is what a router `fallback` serving a single-page app does.
        if name == index and separator and directory not in assets:
            assets[directory] = replace(asset, needs_trailing_slash=True)
            assets[f"{directory}/"] = asset
    return assets


def _slash_redirect(key: str, query_string: bytes) -> Response:
    """
    Send `/assets/guide?theme=dark` to `/assets/guide/?theme=dark`.

    Serving the index at the slash-less URL would resolve every relative link and asset
    reference in that document one level too high, against `/assets/` instead of
    `/assets/guide/`, which is why static servers redirect rather than answer at both.

    The target is **relative**: an inventory does not know what prefix it was mounted
    under, so it names the final segment and lets the client resolve it against the
    request URI (RFC 9110 §10.2.2). The segment is percent-encoded, which also keeps a
    name containing a colon from being read as a scheme.

    The query is carried across explicitly, because a relative reference that states no
    query does not inherit the base URI's (RFC 3986 §5.3): dropping it would answer a
    search or a tracking parameter with the unparameterized page, and it is what nginx
    and WhiteNoise both preserve.
    """
    _, _, name = key.rpartition("/")
    query = b"" if not query_string else b"?" + query_string
    return Response(
        status=_MOVED,
        headers=(
            (_LOCATION, quote(name, safe="").encode(_ASCII) + b"/" + query),
            # A redirect carries no body, and a head that states no length is framed by
            # the transport instead: h11 answers a bodyless `302` with a chunked
            # encoding and a terminating chunk nobody needs.
            (_CONTENT_LENGTH, b"0"),
        ),
    )


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
    Answer a request for `key` out of `assets`: `200`, `206`, `302`, `304`, `416`, or `404`.

    `key` is used only to look up an entry, never to build a path, so every traversal
    payload (`..`, a decoded `%2F` or `%00`, an absolute path, a Windows drive letter,
    a reserved device name) is simply a key that is not present.

    The content coding is negotiated against the variants the inventory holds, and the
    conditional and range rules are then applied to *that* representation: its size, its
    own strong validator. Every answer that owes no bytes (a `302`, a `304`, a `416`,
    and any `HEAD`) is settled from the inventory alone and touches no file at all,
    which is what makes revalidation, the common request for a cached asset, cost a
    dictionary lookup and a byte comparison, and a `curl -I` cost nothing at all.

    When identity bytes are owed the `stat` runs before any `ResponseStart`, and its
    size is checked against the inventory's. A disagreement means the tree was written
    to while the app was running, which the inventory's contract forbids, and raises
    `AssetChanged` while nothing is committed rather than framing a response whose
    length and validator describe different bytes.
    """
    asset = assets.get(key)
    if asset is None:
        return stream_from_iterable(encode_response(not_found))
    if asset.needs_trailing_slash and not scope.path.endswith("/"):
        return stream_from_iterable(encode_response(_slash_redirect(key, scope.query_string)))
    chosen = _negotiated(asset, scope.headers)
    selection = selection_for(
        size=chosen.size,
        method=scope.method,
        request_headers=scope.headers,
        etag=chosen.etag,
        last_modified=asset.last_modified,
    )
    if (body := chosen.body) is not None:
        return _from_memory(chosen, body, selection, chunk_size)
    if isinstance(selection, Whole | Span):
        stat = await asyncio.to_thread(asset.path.stat)
        if stat.st_size != chosen.size:
            raise AssetChanged(
                f"{asset.path} is {stat.st_size} bytes but the inventory recorded {chosen.size}; "
                "something wrote into the asset root while the app was running"
            )
    return stream_selection(asset.path, selection, chosen.size, chosen.described, chosen.revalidation, chunk_size)


def _negotiated(asset: Asset, request_headers: RawHeaders) -> Representation:
    if not asset.codings:
        return asset.identity
    coding = negotiate_coding(_accept_encoding(request_headers), asset.codings)
    return asset.identity if coding is None else asset.encodings[coding]


def _from_memory(chosen: Representation, body: bytes, selection: Selection, chunk_size: int) -> AsyncIterator[Outbound]:
    start = start_for(selection, chosen.size, chosen.described, chosen.revalidation)
    match selection:
        case Whole():
            return _stream_bytes(start, body, 0, len(body), chunk_size)
        case Span() as span:
            # Bounds rather than a slice: slicing the span out and then chunking the
            # copy would move every byte of a large range twice.
            return _stream_bytes(start, body, span.first, span.last + 1, chunk_size)
        case Head() | NotModified() | Unsatisfiable():
            return no_body(start)
        case _ as unreachable:
            assert_never(unreachable)


async def _stream_bytes(
    start: ResponseStart, body: bytes, first: int, end: int, chunk_size: int
) -> AsyncIterator[Outbound]:
    yield start
    for offset in range(first, end, chunk_size):
        yield ResponseBody(body=body[offset : min(offset + chunk_size, end)], more_body=True)
    yield ResponseBody(body=b"", more_body=False)  # pragma: no mutate - values equal the field defaults
