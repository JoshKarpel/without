from __future__ import annotations

import gzip
import hashlib
import logging
import os
import re
import sys
from collections.abc import Callable
from datetime import UTC
from errno import EACCES
from pathlib import Path

import brotli
import pytest
from pytest_mock import MockerFixture
from without_asgi import DEFAULT_CHUNK_SIZE
from without_asgi import IMMUTABLE_CACHE_CONTROL
from without_asgi import NOT_FOUND
from without_asgi import REVALIDATE_CACHE_CONTROL
from without_asgi import STATIC_ASSET_HEADERS
from without_asgi import Response
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_asgi.assets import AssetChanged
from without_asgi.assets import Inventory
from without_asgi.assets import content_hash
from without_asgi.assets import inventory
from without_asgi.assets import serve_asset
from without_asgi.assets import size_and_mtime
from without_asgi.compression import DEFAULT_COMPRESSORS
from without_asgi.compression import Compressor
from without_asgi.compression import brotli_compressor
from without_asgi.compression import gzip_compressor
from without_asgi.compression import zstd_compressor
from without_asgi.headers import add
from without_asgi.headers import first
from without_asgi.headers import replace
from without_asgi.selection import http_date
from without_asgi.types import RawHeaders
from without_streams import collect

from .helpers import a_scope

_STYLESHEET = ("body { color: rebeccapurple; }\n" * 40).encode()
_LOGO = bytes(range(256)) * 12  # not a real PNG, but named one, so it is incompressible by type
# gzip framing around *stored* bytes: already in a content coding, yet still very
# compressible, so a build that re-encoded it would visibly produce a smaller variant.
_STORED_SVGZ = gzip.compress(b"<svg></svg>\n" * 500, compresslevel=0, mtime=0)


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    (tmp_path / "css").mkdir()
    (tmp_path / "css" / "app.css").write_bytes(_STYLESHEET)
    (tmp_path / "logo.png").write_bytes(_LOGO)
    (tmp_path / "guide").mkdir()
    (tmp_path / "guide" / "index.html").write_bytes(b"<p>guide</p>\n")
    return tmp_path


async def _answer(
    assets: Inventory,
    key: str,
    *,
    headers: RawHeaders = (),
    method: str = "GET",
    not_found: Response = NOT_FOUND,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    path: str | None = None,
    query_string: bytes = b"",
) -> tuple[ResponseStart, bytes]:
    scope = a_scope(
        path=path if path is not None else f"/{key}",
        headers=headers,
        method=method,
        query_string=query_string,
    )
    served = await serve_asset(scope, assets, key, not_found=not_found, chunk_size=chunk_size)
    events = await collect(served)
    start = events[0]
    assert isinstance(start, ResponseStart)
    body = b"".join(event.body for event in events[1:] if isinstance(event, ResponseBody))
    return start, body


def _header(start: ResponseStart, name: bytes) -> bytes | None:
    return first(start.headers, name)


def _header_of(assets: Inventory, key: str, name: bytes) -> bytes | None:
    asset = assets.assets[key]
    return first(asset.identity.described, name)


class TestInventoryBuild:
    def test_every_regular_file_is_keyed_by_its_relative_posix_path(self, tree: Path) -> None:
        assets = inventory(tree, encodings={})

        assert set(assets.assets) == {"css/app.css", "logo.png", "guide/index.html"}

    def test_a_missing_root_fails_at_build_rather_than_on_the_first_request(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            inventory(tmp_path / "absent")

    def test_a_root_that_is_a_file_is_rejected(self, tree: Path) -> None:
        with pytest.raises(NotADirectoryError, match="is not a directory") as raised:
            inventory(tree / "logo.png")

        assert "logo.png" in str(raised.value)

    def test_a_directory_is_not_an_asset(self, tree: Path) -> None:
        assets = inventory(tree, encodings={})

        assert assets.get("css") is None
        assert assets.get("guide") is None

    def test_an_index_aliases_the_directory_that_holds_it(self, tree: Path) -> None:
        assets = inventory(tree, index="index.html", encodings={})

        alias, direct = assets.get("guide"), assets.get("guide/index.html")
        assert alias is not None
        assert direct is not None
        assert alias.path == direct.path
        assert alias.identity == direct.identity
        # The slash-less key is the one whose URL still needs canonicalizing.
        assert (alias.needs_trailing_slash, direct.needs_trailing_slash) == (True, False)

    def test_an_index_is_aliased_under_both_spellings_of_the_directory_key(self, tree: Path) -> None:
        # A shell that strips the trailing slash and one that keeps it must both reach
        # the index, so the keyspace cannot depend on which one is above.
        assets = inventory(tree, index="index.html", encodings={})

        slashless, slashed = assets.get("guide"), assets.get("guide/")
        assert slashless is not None
        assert slashed is not None
        assert slashed.path == slashless.path
        # A request that already carried the slash is canonical, so it is answered.
        assert slashed.needs_trailing_slash is False

    async def test_a_key_keeping_its_trailing_slash_serves_the_index_without_redirecting(self, tree: Path) -> None:
        assets = inventory(tree, index="index.html", encodings={})

        start, body = await _answer(assets, "guide/", path="/guide/")

        assert start.status == 200
        assert body == b"<p>guide</p>\n"

    def test_without_an_index_the_directory_key_stays_absent(self, tree: Path) -> None:
        assert inventory(tree, encodings={}).get("guide") is None

    def test_an_index_several_directories_deep_aliases_its_own_directory(self, tmp_path: Path) -> None:
        nested = tmp_path / "docs" / "guide"
        nested.mkdir(parents=True)
        (nested / "index.html").write_bytes(b"<p>nested</p>\n")

        assets = inventory(tmp_path, index="index.html", encodings={})

        alias, direct = assets.get("docs/guide"), assets.get("docs/guide/index.html")
        assert alias is not None
        assert direct is not None
        assert alias.path == direct.path
        assert assets.get("docs") is None

    def test_a_file_that_is_not_the_index_does_not_alias_its_directory(self, tree: Path) -> None:
        assets = inventory(tree, index="index.html", encodings={})

        assert assets.get("css") is None

    def test_the_modification_time_is_carried_as_an_aware_utc_value(self, tree: Path) -> None:
        modified = inventory(tree, encodings={}).assets["css/app.css"].last_modified

        assert modified.tzinfo is UTC

    def test_a_content_type_is_guessed_and_given_a_charset(self, tree: Path) -> None:
        assets = inventory(tree, encodings={})

        assert _header_of(assets, "css/app.css", b"content-type") == b"text/css; charset=utf-8"

    def test_a_binary_type_gets_no_charset(self, tree: Path) -> None:
        assets = inventory(tree, encodings={})

        assert _header_of(assets, "logo.png", b"content-type") == b"image/png"

    def test_the_charset_can_be_turned_off(self, tree: Path) -> None:
        assets = inventory(tree, charset=None, encodings={})

        assert _header_of(assets, "css/app.css", b"content-type") == b"text/css"

    def test_a_content_hash_validator_is_strong_and_stable_across_rebuilds(self, tree: Path) -> None:
        before = inventory(tree, encodings={}).assets["css/app.css"].identity.etag
        os.utime(tree / "css" / "app.css", (0, 0))  # a rebuild that did not change the bytes
        after = inventory(tree, encodings={}).assets["css/app.css"].identity.etag

        assert before == after
        assert not before.startswith(b"W/")

    def test_a_stat_derived_validator_moves_when_the_timestamp_does(self, tree: Path) -> None:
        before = inventory(tree, etag_for=size_and_mtime, encodings={}).assets["css/app.css"].identity.etag
        os.utime(tree / "css" / "app.css", (0, 0))
        after = inventory(tree, etag_for=size_and_mtime, encodings={}).assets["css/app.css"].identity.etag

        assert before != after

    @pytest.mark.parametrize(
        "token",
        [
            pytest.param(b'has"quote', id="a-dquote-would-end-the-tag-early"),
            pytest.param(b"", id="an-empty-token"),
            pytest.param(b"has space", id="a-space-is-below-the-etagc-range"),
            pytest.param(b"has\ttab", id="a-control-character"),
            pytest.param(b"has\x7fdelete", id="delete-is-above-the-etagc-range"),
            pytest.param(b"obs\xe9text", id="obs-text-is-deliberately-not-accepted"),
        ],
    )
    def test_a_token_that_is_not_a_valid_entity_tag_is_rejected(self, tmp_path: Path, token: bytes) -> None:
        # One file, so the key named in the message is not a guess about walk order.
        (tmp_path / "only.css").write_bytes(_STYLESHEET)

        with pytest.raises(ValueError, match="not a valid etag") as raised:
            inventory(tmp_path, etag_for=lambda key, path, stat: token, encodings={})

        # Naming the key is what makes a tree of hundreds actionable.
        assert "only.css" in str(raised.value)

    @pytest.mark.parametrize(
        "token",
        [
            pytest.param(b"!", id="the-bottom-of-the-etagc-range"),
            pytest.param(b"~", id="the-top-of-the-etagc-range"),
            pytest.param(b"a1b2-c3d4/e5", id="an-ordinary-digest-shaped-token"),
        ],
    )
    def test_a_valid_token_is_quoted_as_given(self, tree: Path, token: bytes) -> None:
        assets = inventory(tree, etag_for=lambda key, path, stat: token, encodings={})

        assert assets.assets["css/app.css"].identity.etag == b'"%s"' % token

    def test_the_default_headers_are_applied_to_every_asset(self, tree: Path) -> None:
        assets = inventory(tree, encodings={})

        assert _header_of(assets, "css/app.css", b"cache-control") == REVALIDATE_CACHE_CONTROL
        assert _header_of(assets, "css/app.css", b"x-content-type-options") == b"nosniff"

    def test_given_headers_replace_the_defaults_rather_than_adding_to_them(self, tree: Path) -> None:
        # A value distinct from the default, so this cannot pass by the argument being
        # ignored and the default answering in its place.
        assets = inventory(tree, headers=((b"cache-control", b"private, max-age=30"),), encodings={})

        assert _header_of(assets, "css/app.css", b"cache-control") == b"private, max-age=30"
        assert _header_of(assets, "css/app.css", b"x-content-type-options") is None

    def test_immutable_caching_is_opted_into_rather_than_defaulted(self, tree: Path) -> None:
        # Only correct for fingerprinted filenames, so it is reachable but never applied
        # on the caller's behalf: a stale copy pinned for a year cannot be recalled.
        assert _header_of(inventory(tree, encodings={}), "css/app.css", b"cache-control") != IMMUTABLE_CACHE_CONTROL

        opted_in = replace(STATIC_ASSET_HEADERS, b"cache-control", IMMUTABLE_CACHE_CONTROL)
        assets = inventory(tree, headers=opted_in, encodings={})

        assert _header_of(assets, "css/app.css", b"cache-control") == IMMUTABLE_CACHE_CONTROL
        assert _header_of(assets, "css/app.css", b"x-content-type-options") == b"nosniff"

    def test_the_defaults_extend_through_the_ordinary_header_helpers(self, tree: Path) -> None:
        extended = add(STATIC_ASSET_HEADERS, b"cross-origin-resource-policy", b"same-origin")
        assets = inventory(tree, headers=extended, encodings={})

        assert _header_of(assets, "css/app.css", b"cross-origin-resource-policy") == b"same-origin"
        assert _header_of(assets, "css/app.css", b"cache-control") == REVALIDATE_CACHE_CONTROL

    def test_headers_are_repeated_on_a_revalidation(self, tree: Path) -> None:
        # A browser applies a policy header to the response it reads back out of cache,
        # so a 304 that drops it silently weakens the policy it was set for.
        assets = inventory(tree, encodings={})

        revalidation = assets.assets["css/app.css"].identity.revalidation
        assert first(revalidation, b"cache-control") == REVALIDATE_CACHE_CONTROL
        assert first(revalidation, b"x-content-type-options") == b"nosniff"

    def test_headers_can_be_turned_off_entirely(self, tree: Path) -> None:
        assert _header_of(inventory(tree, headers=(), encodings={}), "css/app.css", b"cache-control") is None

    def test_an_unreadable_directory_raises_rather_than_going_silently_missing(
        self, tree: Path, mocker: MockerFixture
    ) -> None:
        # `Path.walk` ignores a failed `scandir` by default, which would leave the
        # inventory short every asset under the directory and answer 404 for them
        # forever.
        #
        # Windows's `chmod` sets only a read-only bit, which does not stop a directory
        # being read, and coverage runs on every platform, so the refusal is simulated
        # here and the POSIX test below is the control confirming that a really
        # unreadable directory takes the same path.
        mocker.patch("os.scandir", side_effect=PermissionError(EACCES, "Permission denied", str(tree)))

        with pytest.raises(PermissionError):
            inventory(tree, encodings={})

    # The control exists only where permissions stop a read. Excluded from the coverage
    # gate because no single platform exercises both arms.
    if sys.platform != "win32":  # pragma: no cover

        def test_a_really_unreadable_directory_raises(self, tree: Path) -> None:
            locked = tree / "locked"
            locked.mkdir()
            (locked / "hidden.css").write_bytes(_STYLESHEET)
            locked.chmod(0o000)
            try:
                with pytest.raises(PermissionError):
                    inventory(tree, encodings={})
            finally:
                locked.chmod(0o755)


class TestStoredContentCodings:
    """A file whose suffixes name a coding as well as a media type is already encoded."""

    @pytest.mark.parametrize(
        ("name", "content_type", "coding"),
        [
            pytest.param("logo.svgz", b"image/svg+xml", b"gzip", id="svgz-is-a-gzipped-svg"),
            pytest.param("bundle.tar.gz", b"application/x-tar", b"gzip", id="tar-gz-is-a-gzipped-tar"),
            pytest.param("bundle.tgz", b"application/x-tar", b"gzip", id="tgz-is-the-same-thing-abbreviated"),
        ],
    )
    def test_the_coding_the_suffix_names_is_declared(
        self, tmp_path: Path, name: str, content_type: bytes, coding: bytes
    ) -> None:
        # Dropping the coding is how gzip bytes come to be labelled image/svg+xml, which
        # a browser renders as SVG rather than decompressing.
        (tmp_path / name).write_bytes(gzip.compress(b"<svg></svg>\n", mtime=0))

        assets = inventory(tmp_path, encodings={})

        assert _header_of(assets, name, b"content-type") == content_type
        assert _header_of(assets, name, b"content-encoding") == coding

    def test_a_suffix_naming_only_a_coding_is_an_opaque_archive(self, tmp_path: Path) -> None:
        # `guess_file_type("archive.gz")` is `(None, "gzip")`: no media type came with
        # the coding, so this is a file to hand over whole, not one to unwrap.
        (tmp_path / "archive.gz").write_bytes(gzip.compress(b"payload\n", mtime=0))

        assets = inventory(tmp_path, encodings={})

        assert _header_of(assets, "archive.gz", b"content-type") == b"application/octet-stream"
        assert _header_of(assets, "archive.gz", b"content-encoding") is None

    def test_already_encoded_bytes_are_not_encoded_again(self, tmp_path: Path) -> None:
        # Stored rather than deflated, so re-encoding *would* shrink it and the
        # no-smaller check cannot be what keeps the variant out.
        (tmp_path / "logo.svgz").write_bytes(_STORED_SVGZ)

        assets = inventory(tmp_path)

        assert assets.assets["logo.svgz"].encodings == {}

    def test_a_stored_coding_negotiates_nothing_so_it_does_not_vary(self, tmp_path: Path) -> None:
        (tmp_path / "logo.svgz").write_bytes(_STORED_SVGZ)

        assets = inventory(tmp_path)

        assert _header_of(assets, "logo.svgz", b"vary") is None

    def test_the_only_representation_keeps_the_bare_entity_tag(self, tmp_path: Path) -> None:
        # The `-gzip` suffix distinguishes negotiated variants of one asset. A stored
        # coding is not a variant of anything, so suffixing it would be a lie.
        (tmp_path / "logo.svgz").write_bytes(gzip.compress(b"<svg></svg>\n", mtime=0))

        etag = inventory(tmp_path, encodings={}).assets["logo.svgz"].identity.etag

        assert not etag.endswith(b'-gzip"')

    async def test_a_304_for_a_stored_coding_repeats_it(self, tmp_path: Path) -> None:
        (tmp_path / "logo.svgz").write_bytes(gzip.compress(b"<svg></svg>\n", mtime=0))
        assets = inventory(tmp_path, encodings={})
        etag = assets.assets["logo.svgz"].identity.etag

        start, body = await _answer(assets, "logo.svgz", headers=((b"if-none-match", etag),))

        assert (start.status, body) == (304, b"")
        assert _header(start, b"content-encoding") == b"gzip"


class TestSymlinks:
    @pytest.mark.security("a symlink pointing outside the asset root is refused at build time")
    def test_a_symlink_escaping_the_root_raises_when_the_inventory_is_built(self, tree: Path, tmp_path: Path) -> None:
        outside = tmp_path.parent / "outside-the-root.txt"
        outside.write_bytes(b"secret\n")
        (tree / "escape.txt").symlink_to(outside)

        with pytest.raises(ValueError, match="leaves the asset root"):
            inventory(tree, encodings={})

    def test_a_symlink_inside_the_root_is_served(self, tree: Path) -> None:
        (tree / "alias.css").symlink_to(tree / "css" / "app.css")

        assert inventory(tree, encodings={}).get("alias.css") is not None

    def test_a_symlinked_directory_raises_rather_than_leaving_the_inventory_short(self, tree: Path) -> None:
        # `Path.walk` reports a symlinked directory among the *filenames* and does not
        # descend it, so dropping it with the fifos would leave every asset beneath it
        # out of the keyspace and answer 404 for them in production, with a clean startup.
        (tree / "alias").symlink_to(tree / "css", target_is_directory=True)

        with pytest.raises(ValueError, match=r"alias is a symlink to the directory"):
            inventory(tree, encodings={})

    def test_a_dangling_symlink_names_the_link_rather_than_its_target(self, tree: Path) -> None:
        (tree / "dangling.css").symlink_to(tree / "never-written.css")

        with pytest.raises(ValueError, match=r"dangling\.css cannot be read"):
            inventory(tree, encodings={})


class TestNonRegularFiles:
    """
    A fifo, socket, or device has no length to declare and no bytes to seek in, so it is
    left out of the keyspace rather than failing on the request that names it.

    Windows can make none of those, and coverage runs on every platform, so the branch
    is exercised by simulating a non-regular mode everywhere and the POSIX test below is
    the control confirming that a real fifo takes the same path.
    """

    def test_an_entry_that_is_not_a_regular_file_is_left_out(self, tree: Path, mocker: MockerFixture) -> None:
        mocker.patch("without_asgi.assets.S_ISREG", return_value=False)

        assert inventory(tree, encodings={}).assets == {}

    # The control exists only where a fifo does. Branching on the platform rather than
    # skipping on `hasattr(os, "mkfifo")` is what the Windows leg's type check reads:
    # the stubs declare `mkfifo` on POSIX only, so a call it can reach there is an
    # attribute error. Excluded from the coverage gate because no single platform
    # exercises both arms.
    if sys.platform != "win32":  # pragma: no cover

        def test_a_real_fifo_is_left_out_while_its_siblings_are_kept(self, tree: Path) -> None:
            os.mkfifo(tree / "pipe")

            assets = inventory(tree, encodings={})

            assert "pipe" not in assets.assets
            assert "css/app.css" in assets.assets


class TestTraversalPayloadsSimplyMiss:
    """
    Every historical static-file escape, as a key that is not in the mapping.

    These read as near-tautologies, which is the point: the inventory never builds a
    path from request input, so the containment proof that these CVEs all lived inside
    does not exist to get wrong.
    """

    @pytest.mark.security("a request key is never joined onto the asset root", cve="CVE-2024-23334")
    @pytest.mark.parametrize(
        "key",
        [
            pytest.param("../logo.png", id="parent-traversal"),
            pytest.param("css/../../logo.png", id="traversal-through-a-real-directory"),
            pytest.param("/etc/passwd", id="an-absolute-posix-path"),
            pytest.param("etc/passwd", id="a-rooted-looking-relative-path"),
            pytest.param("....//logo.png", id="a-doubled-dot-segment"),
            pytest.param("css/app.css\x00.txt", id="a-decoded-null-byte"),
            pytest.param("css%2Fapp.css", id="an-encoding-the-server-already-decoded"),
            pytest.param("c:/windows/win.ini", id="a-windows-drive-letter"),
            pytest.param("\\\\host\\share\\x", id="a-windows-unc-path"),
            pytest.param("CON", id="a-windows-reserved-device-name"),
            pytest.param("CON.txt", id="a-reserved-device-name-with-an-extension"),
            pytest.param("NUL", id="the-windows-null-device"),
            pytest.param("", id="the-empty-key"),
            pytest.param("css//app.css", id="a-doubled-separator"),
        ],
    )
    async def test_a_traversal_payload_is_a_key_that_is_not_present(self, tree: Path, key: str) -> None:
        assets = inventory(tree, encodings={})

        start, body = await _answer(assets, key)

        assert start.status == 404
        assert body == b"not found\n"

    @pytest.mark.security(
        "a sibling sharing the root's name prefix is not reachable through the mount",
        cve="CVE-2023-29159",
    )
    async def test_a_sibling_named_like_the_root_is_not_in_the_inventory(self, tmp_path: Path) -> None:
        root = tmp_path / "static"
        root.mkdir()
        (root / "app.css").write_bytes(_STYLESHEET)
        (tmp_path / "static1.txt").write_bytes(b"not public\n")

        assets = inventory(root, encodings={})

        assert set(assets.assets) == {"app.css"}
        assert (await _answer(assets, "../static1.txt"))[0].status == 404


class TestServing:
    async def test_a_hit_is_a_200_carrying_the_bytes(self, tree: Path) -> None:
        start, body = await _answer(inventory(tree, encodings={}), "css/app.css")

        assert start.status == 200
        assert body == _STYLESHEET
        assert _header(start, b"content-length") == b"%d" % len(_STYLESHEET)
        assert _header(start, b"accept-ranges") == b"bytes"

    async def test_a_matching_validator_is_a_bodyless_304(self, tree: Path) -> None:
        assets = inventory(tree, encodings={})
        etag = assets.assets["css/app.css"].identity.etag

        start, body = await _answer(assets, "css/app.css", headers=((b"if-none-match", etag),))

        assert start.status == 304
        assert body == b""
        assert _header(start, b"content-length") is None
        assert _header(start, b"etag") == etag

    async def test_a_timestamp_condition_revalidates_too(self, tree: Path) -> None:
        assets = inventory(tree, encodings={})
        modified = assets.assets["css/app.css"].last_modified

        start, body = await _answer(assets, "css/app.css", headers=((b"if-modified-since", http_date(modified)),))

        assert (start.status, body) == (304, b"")

    async def test_a_range_is_a_206_framing_only_the_span(self, tree: Path) -> None:
        start, body = await _answer(inventory(tree, encodings={}), "css/app.css", headers=((b"range", b"bytes=10-19"),))

        assert start.status == 206
        assert body == _STYLESHEET[10:20]
        assert _header(start, b"content-length") == b"10"
        assert _header(start, b"content-range") == b"bytes 10-19/%d" % len(_STYLESHEET)

    async def test_an_unsatisfiable_range_is_a_416_naming_the_size(self, tree: Path) -> None:
        start, body = await _answer(
            inventory(tree, encodings={}), "css/app.css", headers=((b"range", b"bytes=999999-"),)
        )

        assert start.status == 416
        assert body == b""
        assert _header(start, b"content-range") == b"bytes */%d" % len(_STYLESHEET)

    async def test_a_head_request_is_described_like_the_get(self, tree: Path) -> None:
        start, _body = await _answer(inventory(tree, encodings={}), "css/app.css", method="HEAD")

        assert start.status == 200
        assert _header(start, b"content-length") == b"%d" % len(_STYLESHEET)

    async def test_a_head_request_carries_no_body_bytes_at_all(self, tree: Path) -> None:
        # Answering HEAD as `Whole` reads the whole file and streams it for the
        # transport to drop, so `curl -I` costs a full read of whatever it names.
        _start, body = await _answer(inventory(tree, encodings={}), "css/app.css", method="HEAD")

        assert body == b""

    async def test_a_head_request_never_opens_the_file(self, tree: Path, mocker: MockerFixture) -> None:
        assets = inventory(tree, encodings={})  # the walk reads every file; the request must not
        opened = mocker.patch.object(Path, "open", autospec=True)

        await _answer(assets, "css/app.css", method="HEAD")

        opened.assert_not_called()

    async def test_a_head_for_an_in_memory_variant_carries_no_body_either(self, tree: Path) -> None:
        assets = inventory(tree, encodings={b"gzip": gzip_compressor})

        start, body = await _answer(assets, "css/app.css", method="HEAD", headers=((b"accept-encoding", b"gzip"),))

        assert (start.status, body) == (200, b"")
        assert _header(start, b"content-encoding") == b"gzip"
        assert _header(start, b"content-length") == b"%d" % assets.assets["css/app.css"].encodings[b"gzip"].size

    async def test_a_304_for_a_negotiated_variant_names_the_coding_it_revalidates(self, tree: Path) -> None:
        # Without it a downstream `compress()` reads the 304 as one it would itself have
        # encoded and weakens a validator that is still exactly true of the stored bytes.
        assets = inventory(tree, encodings={b"gzip": gzip_compressor})
        etag = assets.assets["css/app.css"].encodings[b"gzip"].etag

        start, body = await _answer(
            assets,
            "css/app.css",
            headers=((b"accept-encoding", b"gzip"), (b"if-none-match", etag)),
        )

        assert (start.status, body) == (304, b"")
        assert _header(start, b"content-encoding") == b"gzip"

    async def test_a_304_for_an_asset_with_no_variants_names_the_type_it_revalidates(self, tree: Path) -> None:
        # An asset with no variants carries no coding for a downstream `compress()` to
        # read, so the type is the only evidence that it was never a candidate. Without
        # it the strong tag is weakened and the client's next `If-Range` refetches.
        assets = inventory(tree)
        etag = assets.assets["logo.png"].identity.etag

        start, body = await _answer(assets, "logo.png", headers=((b"if-none-match", etag),))

        assert (start.status, body) == (304, b"")
        assert _header(start, b"content-type") == b"image/png"


class TestDirectoryIndexes:
    @pytest.fixture
    def assets(self, tree: Path) -> Inventory:
        return inventory(tree, index="index.html", encodings={})

    async def test_the_trailing_slash_form_serves_the_index(self, assets: Inventory) -> None:
        start, body = await _answer(assets, "guide", path="/guide/")

        assert (start.status, body) == (200, b"<p>guide</p>\n")

    async def test_the_slashless_form_redirects_rather_than_serving_the_document(self, assets: Inventory) -> None:
        # Served here, every relative link in the document resolves against `/` instead
        # of `/guide/`, i.e. one level too high. `split_path` has already dropped the
        # trailing slash, so this is the only place the two URLs are still distinct.
        start, body = await _answer(assets, "guide", path="/guide")

        assert (start.status, body) == (302, b"")
        assert _header(start, b"location") == b"guide/"

    async def test_the_redirect_carries_the_query_across(self, assets: Inventory) -> None:
        # A relative reference stating no query does not inherit the base URI's
        # (RFC 3986 §5.3), so the search would land on the unparameterized page.
        start, _body = await _answer(assets, "guide", path="/guide", query_string=b"theme=dark")

        assert _header(start, b"location") == b"guide/?theme=dark"

    async def test_the_redirect_states_a_zero_length_rather_than_being_framed_by_the_transport(
        self, assets: Inventory
    ) -> None:
        start, _body = await _answer(assets, "guide", path="/guide")

        assert _header(start, b"content-length") == b"0"

    async def test_the_redirect_target_is_relative_so_the_mount_prefix_is_irrelevant(self, tmp_path: Path) -> None:
        nested = tmp_path / "docs" / "guide"
        nested.mkdir(parents=True)
        (nested / "index.html").write_bytes(b"<p>nested</p>\n")
        assets = inventory(tmp_path, index="index.html", encodings={})

        start, _body = await _answer(assets, "docs/guide", path="/assets/docs/guide")

        # Resolved against the request URI (RFC 9110 §10.2.2) this is /assets/docs/guide/.
        assert _header(start, b"location") == b"guide/"

    @pytest.mark.parametrize(
        ("name", "location"),
        [
            pytest.param("a b", b"a%20b/", id="a-space"),
            # A bare colon in the first segment of a relative reference reads as a scheme,
            # so it is escaped too. Windows serves no such directory, in a test or in an
            # asset tree: the character is illegal in a filename there.
            *([] if sys.platform == "win32" else [pytest.param("a b:c", b"a%20b%3Ac/", id="a-colon")]),
        ],
    )
    async def test_a_directory_whose_name_needs_encoding_is_quoted(
        self, tmp_path: Path, name: str, location: bytes
    ) -> None:
        nested = tmp_path / name
        nested.mkdir()
        (nested / "index.html").write_bytes(b"<p>odd</p>\n")
        assets = inventory(tmp_path, index="index.html", encodings={})

        start, _body = await _answer(assets, name, path=f"/{name}")

        assert _header(start, b"location") == location

    async def test_naming_the_index_directly_still_serves_it(self, assets: Inventory) -> None:
        start, body = await _answer(assets, "guide/index.html", path="/guide/index.html")

        assert (start.status, body) == (200, b"<p>guide</p>\n")

    async def test_an_ordinary_asset_is_never_redirected(self, assets: Inventory) -> None:
        start, body = await _answer(assets, "css/app.css", path="/css/app.css")

        assert (start.status, body) == (200, _STYLESHEET)

    async def test_the_default_not_found_states_its_own_length(self, tree: Path) -> None:
        start, body = await _answer(inventory(tree, encodings={}), "absent")

        assert (start.status, _header(start, b"content-length")) == (404, b"%d" % len(body))

    async def test_a_custom_not_found_response_is_used(self, tree: Path) -> None:
        teapot = Response(status=418, body=b"nope\n")

        start, body = await _answer(inventory(tree, encodings={}), "absent", not_found=teapot)

        assert (start.status, body) == (418, b"nope\n")

    @pytest.mark.security("an asset rewritten after the inventory was built is refused, not mis-framed")
    async def test_a_file_that_changed_since_the_walk_raises_before_anything_is_sent(self, tree: Path) -> None:
        assets = inventory(tree, encodings={})
        (tree / "css" / "app.css").write_bytes(b"much shorter\n")

        with pytest.raises(AssetChanged, match="wrote into the asset root") as raised:
            await _answer(assets, "css/app.css")

        assert "app.css" in str(raised.value)

    async def test_a_revalidation_of_a_changed_file_never_touches_the_disk(self, tree: Path) -> None:
        # The 304 path is decided from the inventory alone, so it does not reach the
        # drift check at all: no file is opened and none is stat'd.
        assets = inventory(tree, encodings={})
        etag = assets.assets["css/app.css"].identity.etag
        (tree / "css" / "app.css").unlink()

        start, _body = await _answer(assets, "css/app.css", headers=((b"if-none-match", etag),))

        assert start.status == 304


class TestPreCompression:
    async def test_a_compressible_asset_gains_a_variant_per_coding(self, tree: Path) -> None:
        asset = inventory(tree).assets["css/app.css"]

        assert set(asset.encodings) == set(DEFAULT_COMPRESSORS)

    async def test_an_incompressible_type_gains_none(self, tree: Path) -> None:
        assert inventory(tree).assets["logo.png"].encodings == {}

    async def test_a_client_offering_a_coding_gets_it(self, tree: Path) -> None:
        start, body = await _answer(inventory(tree), "css/app.css", headers=((b"accept-encoding", b"gzip"),))

        assert start.status == 200
        assert _header(start, b"content-encoding") == b"gzip"
        assert gzip.decompress(body) == _STYLESHEET
        assert _header(start, b"content-length") == b"%d" % len(body)

    async def test_an_encoded_variant_still_names_the_underlying_media_type(self, tree: Path) -> None:
        # `Content-Encoding` describes the transformation; `Content-Type` still has to
        # name what the client gets after undoing it.
        start, _body = await _answer(inventory(tree), "css/app.css", headers=((b"accept-encoding", b"gzip"),))

        assert _header(start, b"content-type") == b"text/css; charset=utf-8"

    async def test_an_encoded_body_spanning_several_chunks_is_reassembled(self, tree: Path) -> None:
        assets = inventory(tree)
        encoded = assets.assets["css/app.css"].encodings[b"gzip"]
        assert encoded.body is not None
        assert encoded.size > 32  # so the chunk size below really does split it

        _start, body = await _answer(assets, "css/app.css", headers=((b"accept-encoding", b"gzip"),), chunk_size=16)

        assert body == encoded.body

    async def test_every_encoded_chunk_but_the_last_says_more_is_coming(self, tree: Path) -> None:
        # Joining the bodies passes even if the stream tells the transport to stop after
        # the first chunk, so the framing flags are asserted rather than the bytes.
        assets = inventory(tree)
        scope = a_scope(path="/css/app.css", headers=((b"accept-encoding", b"gzip"),))

        events = await collect(await serve_asset(scope, assets, "css/app.css", chunk_size=16))
        bodies = [event for event in events if isinstance(event, ResponseBody)]

        assert len(bodies) > 2
        assert all(event.more_body for event in bodies[:-1])
        assert bodies[-1] == ResponseBody(body=b"", more_body=False)

    async def test_an_encoded_variant_carries_the_caching_policy_and_varies(self, tree: Path) -> None:
        start, _body = await _answer(inventory(tree), "css/app.css", headers=((b"accept-encoding", b"gzip"),))

        assert _header(start, b"cache-control") == REVALIDATE_CACHE_CONTROL
        assert _header(start, b"vary") == b"accept-encoding"

    async def test_an_offer_split_across_two_header_fields_is_read_whole(self, tree: Path) -> None:
        # RFC 9110 §5.2: a list-valued field's value is all of its occurrences joined.
        start, _body = await _answer(
            inventory(tree),
            "css/app.css",
            headers=((b"accept-encoding", b"identity;q=0"), (b"accept-encoding", b"gzip")),
        )

        assert _header(start, b"content-encoding") == b"gzip"

    async def test_a_client_offering_nothing_gets_the_identity_bytes(self, tree: Path) -> None:
        start, body = await _answer(inventory(tree), "css/app.css")

        assert _header(start, b"content-encoding") is None
        assert body == _STYLESHEET

    async def test_brotli_is_preferred_over_gzip_when_both_are_offered(self, tree: Path) -> None:
        start, body = await _answer(inventory(tree), "css/app.css", headers=((b"accept-encoding", b"gzip, br"),))

        assert _header(start, b"content-encoding") == b"br"
        assert brotli.decompress(body) == _STYLESHEET

    async def test_an_asset_with_variants_varies_on_accept_encoding(self, tree: Path) -> None:
        start, _body = await _answer(inventory(tree), "css/app.css")

        assert _header(start, b"vary") == b"accept-encoding"

    async def test_an_asset_without_variants_does_not_vary(self, tree: Path) -> None:
        # Stamping Vary on an incompressible asset fragments every downstream cache key
        # for nothing; this is ngx_brotli issue #97.
        start, _body = await _answer(inventory(tree), "logo.png")

        assert _header(start, b"vary") is None

    @pytest.mark.security("each content coding carries its own strong validator")
    async def test_each_coding_has_a_distinct_etag(self, tree: Path) -> None:
        # One tag shared across codings lets a client holding the gzip copy revalidate
        # into a 304 and keep bytes from a different representation.
        asset = inventory(tree).assets["css/app.css"]
        tags = [asset.identity.etag, *(variant.etag for variant in asset.encodings.values())]

        assert len(set(tags)) == len(tags)
        assert all(not tag.startswith(b"W/") for tag in tags)

    async def test_a_conditional_request_matches_only_the_coding_it_was_issued_for(self, tree: Path) -> None:
        assets = inventory(tree)
        identity_tag = assets.assets["css/app.css"].identity.etag

        start, _body = await _answer(
            assets,
            "css/app.css",
            headers=((b"accept-encoding", b"gzip"), (b"if-none-match", identity_tag)),
        )

        assert start.status == 200
        assert _header(start, b"content-encoding") == b"gzip"

    async def test_a_range_over_an_encoded_variant_is_framed_against_that_variant(self, tree: Path) -> None:
        # On-the-fly compression cannot answer this at all: it must skip every 206
        # because it has no way to restate a content-range computed over identity bytes.
        assets = inventory(tree)
        encoded = assets.assets["css/app.css"].encodings[b"gzip"]
        assert encoded.body is not None

        start, body = await _answer(
            assets,
            "css/app.css",
            headers=((b"accept-encoding", b"gzip"), (b"range", b"bytes=0-9")),
        )

        assert start.status == 206
        assert body == encoded.body[:10]
        assert _header(start, b"content-range") == b"bytes 0-9/%d" % encoded.size

    async def test_a_coding_that_does_not_shrink_the_body_is_dropped(self, tmp_path: Path) -> None:
        (tmp_path / "tiny.css").write_bytes(b"a")  # smaller than any container's header

        assert inventory(tmp_path).assets["tiny.css"].encodings == {}

    async def test_a_dropped_coding_does_not_stop_the_ones_after_it(self, tmp_path: Path) -> None:
        # Twenty repeated bytes are larger under gzip's container and much smaller under
        # brotli's, so the first coding in the table is discarded and the second is not.
        (tmp_path / "small.css").write_bytes(b"a" * 20)

        encodings = (
            inventory(tmp_path, encodings={b"gzip": gzip_compressor, b"br": brotli_compressor})
            .assets["small.css"]
            .encodings
        )

        assert set(encodings) == {b"br"}

    async def test_encodings_can_be_turned_off(self, tree: Path) -> None:
        assert inventory(tree, encodings={}).assets["css/app.css"].encodings == {}

    async def test_a_variant_can_be_revalidated_into_a_bodyless_304(self, tree: Path) -> None:
        assets = inventory(tree)
        gzipped = assets.assets["css/app.css"].encodings[b"gzip"].etag

        start, body = await _answer(
            assets,
            "css/app.css",
            headers=((b"accept-encoding", b"gzip"), (b"if-none-match", gzipped)),
        )

        assert (start.status, body) == (304, b"")
        assert _header(start, b"etag") == gzipped

    async def test_a_range_past_the_end_of_a_variant_is_a_416(self, tree: Path) -> None:
        assets = inventory(tree)
        encoded = assets.assets["css/app.css"].encodings[b"gzip"]

        start, body = await _answer(
            assets,
            "css/app.css",
            headers=((b"accept-encoding", b"gzip"), (b"range", b"bytes=999999-")),
        )

        assert (start.status, body) == (416, b"")
        assert _header(start, b"content-range") == b"bytes */%d" % encoded.size


class TestSidecars:
    def _with_sidecar(self, tree: Path, body: bytes) -> Path:
        sidecar = tree / "css" / "app.css.gz"
        sidecar.write_bytes(body)
        return sidecar

    @pytest.mark.parametrize(
        ("coding", "suffix", "compressor"),
        [
            pytest.param(b"gzip", ".gz", gzip_compressor, id="gzip-uses-the-dot-gz-suffix"),
            pytest.param(b"br", ".br", brotli_compressor, id="brotli-uses-the-dot-br-suffix"),
            pytest.param(b"zstd", ".zst", zstd_compressor, id="zstd-uses-the-dot-zst-suffix"),
        ],
    )
    async def test_each_coding_looks_for_the_conventional_sidecar_name(
        self,
        tree: Path,
        coding: bytes,
        suffix: str,
        compressor: Callable[[], Compressor],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # The names nginx's gzip_static/brotli_static and WhiteNoise already write, so a
        # build system emitting either is understood without configuration.
        prebuilt = b"pretend this is a smaller encoding of the stylesheet"
        (tree / "css" / f"app.css{suffix}").write_bytes(prebuilt)

        with caplog.at_level(logging.WARNING, logger="without_asgi.assets"):
            assets = inventory(tree, encodings={coding: compressor})

        assert assets.assets["css/app.css"].encodings[coding].body == prebuilt
        assert "css/app.css" not in caplog.text

    async def test_a_sidecar_is_used_instead_of_compressing_at_startup(self, tree: Path) -> None:
        prebuilt = gzip.compress(_STYLESHEET, compresslevel=9, mtime=0)
        self._with_sidecar(tree, prebuilt)

        assets = inventory(tree, encodings={b"gzip": gzip_compressor})

        assert assets.assets["css/app.css"].encodings[b"gzip"].body == prebuilt

    async def test_a_coding_with_no_convention_falls_back_to_its_own_name(self, tree: Path) -> None:
        prebuilt = b"a shorter encoding under a made-up coding name"
        (tree / "css" / "app.css.rot13").write_bytes(prebuilt)

        assets = inventory(tree, encodings={b"rot13": gzip_compressor})

        assert assets.assets["css/app.css"].encodings[b"rot13"].body == prebuilt

    async def test_a_sidecar_is_not_itself_an_asset(self, tree: Path) -> None:
        self._with_sidecar(tree, gzip.compress(_STYLESHEET, mtime=0))

        assets = inventory(tree, encodings={b"gzip": gzip_compressor})

        assert "css/app.css.gz" not in assets.assets

    @pytest.mark.security("a sidecar is never published under the media type of the asset it encodes")
    @pytest.mark.parametrize(
        "suffix",
        [
            pytest.param(".gz", id="gzip"),
            pytest.param(".br", id="brotli"),
            pytest.param(".zst", id="zstd"),
        ],
    )
    @pytest.mark.parametrize(
        "encodings",
        [
            pytest.param({}, id="with-no-codings-configured-at-all"),
            pytest.param({b"gzip": gzip_compressor}, id="with-only-one-coding-configured"),
        ],
    )
    async def test_a_sidecar_is_dropped_whether_or_not_its_coding_is_configured(
        self,
        tree: Path,
        suffix: str,
        encodings: dict[bytes, Callable[[], Compressor]],
    ) -> None:
        # Keying suppression on the active table is how `app.css.br` becomes an asset of
        # its own: brotli bytes labelled `text/css`, with no content-encoding, at a URL
        # the build system's own naming makes guessable.
        (tree / "css" / f"app.css{suffix}").write_bytes(b"encoded bytes that are not css")

        assets = inventory(tree, encodings=encodings)

        assert f"css/app.css{suffix}" not in assets.assets

    @pytest.mark.parametrize(
        ("name", "content_type"),
        [
            pytest.param("data.tar", b"application/x-tar", id="a-tarball-and-its-gzip"),
            pytest.param("manual.pdf", b"application/pdf", id="a-pdf-and-its-gzip"),
        ],
    )
    async def test_a_sidecar_beside_an_asset_that_is_never_encoded_keeps_its_own_url(
        self, tree: Path, name: str, content_type: bytes
    ) -> None:
        # A media type this never compresses has no variant for the sidecar to become, so
        # suppressing it would take those bytes out of the keyspace and put them back
        # nowhere: a silent 404 for a second deliverable the operator published, which on
        # its own is an asset in good standing.
        payload = b"pretend this is the uncompressed form\n"
        (tree / name).write_bytes(payload)
        (tree / f"{name}.gz").write_bytes(gzip.compress(payload, mtime=0))

        assets = inventory(tree)

        assert assets.assets[name].encodings == {}
        start, _body = await _answer(assets, f"{name}.gz")
        assert start.status == 200
        assert _header(start, b"content-type") == content_type
        assert _header(start, b"content-encoding") == b"gzip"

    async def test_a_sidecar_with_no_asset_beside_it_is_served_as_itself(self, tree: Path) -> None:
        (tree / "orphan.gz").write_bytes(gzip.compress(b"lonely", mtime=0))

        assets = inventory(tree, encodings={b"gzip": gzip_compressor})

        assert "orphan.gz" in assets.assets

    @pytest.mark.security("a sidecar older than the asset it encodes is not served under a fresh validator")
    async def test_a_stale_sidecar_is_recompressed(self, tree: Path) -> None:
        stale = self._with_sidecar(tree, gzip.compress(b"the previous build\n", mtime=0))
        os.utime(stale, (0, 0))

        assets = inventory(tree, encodings={b"gzip": gzip_compressor})

        assert gzip.decompress(assets.assets["css/app.css"].encodings[b"gzip"].body or b"") == _STYLESHEET

    async def test_compressing_at_startup_is_reported(self, tree: Path, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="without_asgi.assets"):
            inventory(tree, encodings={b"gzip": gzip_compressor})

        assert "css/app.css (gzip)" in caplog.text
        assert "build system" in caplog.text

    @pytest.mark.parametrize(
        ("files", "expected"),
        [
            pytest.param(5, None, id="exactly-the-naming-cap-counts-nothing-extra"),
            pytest.param(6, "and 1 more", id="one-past-the-cap-counts-one"),
            pytest.param(8, "and 3 more", id="several-past-the-cap"),
        ],
    )
    async def test_the_report_counts_only_the_files_it_did_not_name(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture, files: int, expected: str | None
    ) -> None:
        for index in range(files):
            (tmp_path / f"a{index}.css").write_bytes(_STYLESHEET)

        with caplog.at_level(logging.WARNING, logger="without_asgi.assets"):
            inventory(tmp_path, encodings={b"gzip": gzip_compressor})

        assert f"Compressed {files} asset representation(s)" in caplog.text
        if expected is None:
            assert "more)" not in caplog.text
        else:
            assert expected in caplog.text

    async def test_a_coding_that_is_discarded_is_not_reported(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        # A one-byte stylesheet is larger under every container, so no variant is kept
        # and there is no sidecar a build step could usefully have produced.
        (tmp_path / "tiny.css").write_bytes(b"a")

        with caplog.at_level(logging.WARNING, logger="without_asgi.assets"):
            inventory(tmp_path)

        assert caplog.text == ""

    async def test_a_complete_set_of_sidecars_reports_nothing(
        self, tree: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        self._with_sidecar(tree, gzip.compress(_STYLESHEET, mtime=0))
        (tree / "guide" / "index.html.gz").write_bytes(gzip.compress(b"<p>guide</p>\n" * 40, mtime=0))

        with caplog.at_level(logging.WARNING, logger="without_asgi.assets"):
            inventory(tree, encodings={b"gzip": gzip_compressor})

        assert caplog.text == ""


class TestEtagFor:
    def test_the_key_and_stat_are_both_available_to_a_custom_validator(self, tree: Path) -> None:
        seen: list[tuple[str, int]] = []

        def from_key(key: str, path: Path, stat: os.stat_result) -> bytes:
            seen.append((key, stat.st_size))
            return key.replace("/", "-").encode()

        assets = inventory(tree, etag_for=from_key, encodings={})

        assert assets.assets["css/app.css"].identity.etag == b'"css-app.css"'
        assert ("css/app.css", len(_STYLESHEET)) in seen

    def test_a_content_hash_is_a_128_bit_digest_in_hex(self, tree: Path) -> None:
        # The digest width is on the wire in every `ETag`, so it is pinned rather than
        # left to whatever the constructor happened to be called with.
        etag = inventory(tree, encodings={}).assets["css/app.css"].identity.etag

        assert re.fullmatch(rb'"[0-9a-f]{32}"', etag)

    def test_a_file_larger_than_one_read_hashes_to_its_whole_contents(self, tmp_path: Path) -> None:
        payload = bytes(range(256)) * 8192  # 2 MB, more than `file_digest` reads at once
        path = tmp_path / "a.css"
        path.write_bytes(payload)

        expected = hashlib.blake2b(payload, digest_size=16).hexdigest().encode()

        assert content_hash("a.css", path, path.stat()) == expected

    def test_size_and_mtime_is_lowercase_hex_joined_by_a_dash(self, tree: Path) -> None:
        # The exact spelling is on the wire in every `ETag`, so it is pinned rather than
        # left to whatever `%` format happened to be written.
        path = tree / "css" / "app.css"
        stat = path.stat()

        token = size_and_mtime("css/app.css", path, stat)

        assert token == b"%x-%x" % (stat.st_size, stat.st_mtime_ns)
        assert re.fullmatch(rb"[0-9a-f]+-[0-9a-f]+", token)

    def test_content_hash_and_size_and_mtime_disagree_on_a_touched_file(self, tree: Path) -> None:
        path = tree / "css" / "app.css"
        stat = path.stat()

        assert content_hash("css/app.css", path, stat) != size_and_mtime("css/app.css", path, stat)
