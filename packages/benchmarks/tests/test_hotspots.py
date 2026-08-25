from __future__ import annotations

import pytest
from benchmarks.hotspots import package
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
    assert package({"file": file, "name": "whatever", "line": 42}) == expected


def test_self_time_attributes_weight_to_leaf_frames() -> None:
    profile = {
        "shared": {
            "frames": [
                {"name": "run", "file": "/usr/lib/python3.14/asyncio/runners.py", "line": 127},
                {"name": "write", "file": "/usr/lib/python3.14/asyncio/selector_events.py", "line": 1071},
                {"name": "iterencode", "file": "/usr/lib/python3.14/json/encoder.py", "line": 263},
            ],
        },
        "profiles": [
            {
                # Stack A leafs in the socket write, stack B in the JSON encoder;
                # the shared `run` root frame is never a leaf, so it earns no self-time.
                "samples": [[0, 1], [0, 2]],
                "weights": [7, 3],
            }
        ],
    }

    by_package, by_frame, total = self_time(profile)

    assert total == 10
    assert by_package == {"stdlib:asyncio": 7, "stdlib:json": 3}
    assert by_frame["write  (selector_events.py:1071)"] == 7
    assert by_frame["iterencode  (encoder.py:263)"] == 3
    assert "run  (runners.py:127)" not in by_frame
