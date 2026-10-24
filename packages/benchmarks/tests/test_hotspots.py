from __future__ import annotations

import json

import pytest
from benchmarks.hotspots import Frame
from benchmarks.hotspots import package
from benchmarks.hotspots import self_samples
from benchmarks.hotspots import self_time

# The classifier keys off the module *after* the install/source marker, so the
# most important case is the regression: the repository root is itself named
# `without/`, so a naive `/without/` substring match would misfile every installed
# package (which lives under `…/without/.venv/…`) as the `without` core package.


@pytest.mark.parametrize(
    ("file", "expected"),
    [
        # Installed third-party packages, classified by their top-level module even
        # though the venv path runs through the repo root `…/without/.venv/…`.
        ("/home/j/without/.venv/lib/python3.14/site-packages/uvicorn/protocols/http/httptools_impl.py", "uvicorn"),
        ("/home/j/without/.venv/lib/python3.14/site-packages/h11/_connection.py", "h11"),
        ("/home/j/without/.venv/lib/python3.14/site-packages/h2/connection.py", "h2"),
        ("/home/j/without/.venv/lib/python3.14/site-packages/hpack/hpack.py", "hpack"),
        ("/home/j/without/.venv/lib/python3.14/site-packages/hyperframe/frame.py", "hyperframe"),
        ("/home/j/without/.venv/lib/python3.14/site-packages/fastapi/routing.py", "fastapi"),
        ("/home/j/without/.venv/lib/python3.14/site-packages/pydantic/type_adapter.py", "pydantic"),
        # First-party packages, classified from the `/src/` marker.
        ("/home/j/without/packages/without_streams/src/without_streams/wiring.py", "without-streams"),
        ("/home/j/without/packages/without_http/src/without_http/server.py", "without-http"),
        ("/home/j/without/packages/without_asgi/src/without_asgi/h11_wire.py", "without-asgi"),
        # Interpreter internals and stdlib, split so the event-loop cost is visible.
        ("/usr/lib/python3.14/asyncio/selector_events.py", "stdlib:asyncio"),
        ("/usr/lib/python3.14/json/encoder.py", "stdlib:json"),
        ("<frozen importlib._bootstrap>", "stdlib:other"),
        # An unmapped installed package keeps its top-level name so it is legible.
        ("/home/j/without/.venv/lib/python3.14/site-packages/anyio/_backends/_asyncio.py", "other:anyio"),
    ],
)
def test_package_classifies_by_top_level_module(file: str, expected: str) -> None:
    assert package(file) == expected


RUNNERS = "/usr/lib/python3.15/asyncio/runners.py"
SELECTOR = "/usr/lib/python3.15/asyncio/selector_events.py"
ENCODER = "/usr/lib/python3.15/json/encoder.py"


def jsonl(*records: dict[str, object]) -> str:
    return "\n".join(json.dumps({"v": 0, "run_id": "r", **record}) for record in records)


# The shape of a `replay --jsonl`: strings, then frames referencing them, then the
# per-frame aggregate. Frames 2 and 3 are the same line at two columns.
PROFILE = jsonl(
    {"type": "meta", "sample_interval_usec": 100, "mode": "gil"},
    {
        "type": "string_table",
        "strings": [
            {"str_id": 0, "value": "run"},
            {"str_id": 1, "value": RUNNERS},
            {"str_id": 2, "value": "write"},
            {"str_id": 3, "value": SELECTOR},
            {"str_id": 4, "value": "iterencode"},
            {"str_id": 5, "value": ENCODER},
        ],
    },
    {
        "type": "frame_table",
        "frames": [
            {"frame_id": 0, "path_str_id": 1, "func_str_id": 0, "line": 127, "col": 8},
            {"frame_id": 1, "path_str_id": 3, "func_str_id": 2, "line": 1071, "col": 12},
            {"frame_id": 2, "path_str_id": 5, "func_str_id": 4, "line": 263, "col": 15},
            {"frame_id": 3, "path_str_id": 5, "func_str_id": 4, "line": 263, "col": 30},
        ],
    },
    {
        "type": "agg",
        "kind": "frame",
        "scope": "final",
        "samples_total": 13,
        "entries": [
            {"frame_id": 0, "self": 0, "cumulative": 13},
            {"frame_id": 1, "self": 7, "cumulative": 7},
            {"frame_id": 2, "self": 2, "cumulative": 2},
            {"frame_id": 3, "self": 4, "cumulative": 4},
        ],
    },
    {"type": "end", "samples_total": 13},
)


def test_self_samples_sums_the_columns_of_one_line() -> None:
    samples = self_samples(PROFILE)

    assert samples[Frame(file=ENCODER, function="iterencode", line=263)] == 6
    assert samples[Frame(file=SELECTOR, function="write", line=1071)] == 7


def test_self_samples_gives_a_root_frame_no_self_time() -> None:
    assert self_samples(PROFILE)[Frame(file=RUNNERS, function="run", line=127)] == 0


def test_self_samples_rejects_an_unknown_format_version() -> None:
    with pytest.raises(ValueError, match="version 1"):
        self_samples(json.dumps({"v": 1, "type": "meta"}))


def test_self_time_attributes_samples_to_package_and_frame() -> None:
    by_package, by_frame, total = self_time(self_samples(PROFILE))

    assert total == 13
    assert by_package == {"stdlib:asyncio": 7, "stdlib:json": 6}
    assert by_frame["write  (selector_events.py:1071)"] == 7
    assert by_frame["iterencode  (encoder.py:263)"] == 6
