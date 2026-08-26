from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Mapping
from pathlib import Path

# Summarize where a saturated server's CPU goes, from the austin speedscope
# profiles the harness writes under `--profile`. austin CPU-mode samples are
# on-CPU stacks; self-time is the weight landing on a stack's *leaf* frame, which
# this attributes to a package (by the frame's top-level module) and to the frame
# itself. Like `plot.py` this reads results the harness produced and is run by
# hand, so it stays out of the unit-test path; the pure classification is tested.
#
# Reading the numbers: for the uvloop-backed cells (uvicorn, without-http-uvloop)
# the event loop is C, so its socket I/O and scheduling surface at the nearest
# Python frame, `asyncio.runners.Runner.run`. That large `stdlib:asyncio` block is
# the C loop, not a pure-Python hotspot; the pure-Python selector transport's
# `_SelectorSocketTransport.write` only shows up on the plain (stdlib-asyncio)
# `without-http` cell.

_TOP_LEVEL = {
    "without_http": "without-http",
    "without_asgi": "without-asgi",
    "without_web": "without-web",
    "without_env": "without-env",
    "without_configmap": "without-configmap",
    "without_streams": "without-streams",
    "without_async": "without-async",
    "h11": "h11",
    "h2": "h2",
    "hpack": "hpack",
    "hyperframe": "hyperframe",
    "httptools": "httptools",
    "uvloop": "uvloop",
    "uvicorn": "uvicorn",
    "starlette": "starlette",
    "fastapi": "fastapi",
    "pydantic_core": "pydantic-core",
    "pydantic": "pydantic",
    "integration": "integration(domain)",
}


def package(frame: Mapping[str, object]) -> str:
    """
    Classify a speedscope frame into a package label by its top-level module.

    The module is taken from the path *after* the install/source marker
    (`site-packages/` or `/src/`), so the repository root (which is itself named
    `without/`) cannot be read as a top-level module. Frames without such a
    marker are interpreter internals or the stdlib.
    """
    raw = frame.get("file")
    path = (raw if isinstance(raw, str) else "").replace("\\", "/")
    for marker in ("/site-packages/", "/src/"):
        index = path.find(marker)
        if index != -1:
            top = path[index + len(marker) :].split("/")[0].removesuffix(".py")
            return _TOP_LEVEL.get(top, f"other:{top}")
    if "/asyncio/" in path:
        return "stdlib:asyncio"
    if "/json/" in path:
        return "stdlib:json"
    return "stdlib:other"


def self_time(profile: Mapping[str, object]) -> tuple[Counter[str], Counter[str], int]:
    """
    Aggregate leaf-frame self-time from a parsed speedscope document.

    Returns `(by_package, by_frame, total_weight)`, where each counter maps a label
    to summed sample weight over the stacks whose leaf it is.
    """
    shared = profile["shared"]
    frames = shared["frames"]  # type: ignore[index]
    first = profile["profiles"][0]  # type: ignore[index]
    samples, weights = first["samples"], first["weights"]

    by_package: Counter[str] = Counter()
    by_frame: Counter[str] = Counter()
    for stack, weight in zip(samples, weights, strict=True):
        if not stack:
            continue
        leaf = frames[stack[-1]]
        by_package[package(leaf)] += weight
        name, file, line = leaf.get("name"), leaf.get("file"), leaf.get("line")
        by_frame[f"{name}  ({Path(file if isinstance(file, str) else '').name}:{line})"] += weight
    return by_package, by_frame, sum(weights)


def report(path: Path, top: int) -> str:
    by_package, by_frame, total = self_time(json.loads(path.read_text()))
    cell = path.stem.removesuffix(".speedscope")
    lines = [f"===== {cell}  ({total} samples) =====", "  self-time by package:"]
    lines += [f"    {100 * weight / total:5.1f}%  {label}" for label, weight in by_package.most_common(top)]
    lines.append("  top self-time frames:")
    lines += [f"    {100 * weight / total:5.1f}%  {label}" for label, weight in by_frame.most_common(top)]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Summarize per-package and per-frame self-time from speedscope profiles."
    )
    parser.add_argument(
        "profiles",
        nargs="*",
        type=Path,
        help="speedscope profile files (default: every *-prof.speedscope.json under --results)",
    )
    parser.add_argument("--results", type=Path, default=Path(__file__).parent.parent.parent / "results")
    parser.add_argument("--top", type=int, default=12, help="how many packages and frames to show per profile")
    args = parser.parse_args()

    profiles = args.profiles or sorted(args.results.glob("*-prof.speedscope.json"))
    if not profiles:
        raise SystemExit(f"no profiles given and none found in {args.results}; run `just bench ... --profile` first")
    for path in profiles:
        print(report(path, args.top))
        print()


if __name__ == "__main__":
    main()
