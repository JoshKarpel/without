from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

# Summarize where a saturated server's CPU goes, from the Tachyon profiles the
# harness writes under `--profile`. Tachyon GIL-mode samples are stacks of a thread
# running Python; self-time is the samples landing on a stack's *leaf* frame, which
# this attributes to a package (by the frame's top-level module) and to the frame
# itself. Like `plot.py` this reads results the harness produced and is run by
# hand, so it stays out of the unit-test path; the pure parsing and classification
# are tested.
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


def package(file: str) -> str:
    """
    Classify a frame's source file into a package label by its top-level module.

    The module is taken from the path *after* the install/source marker
    (`site-packages/` or `/src/`), so the repository root (which is itself named
    `without/`) cannot be read as a top-level module. Frames without such a
    marker are interpreter internals or the stdlib.
    """
    path = file.replace("\\", "/")
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


@dataclass(frozen=True, slots=True)
class Frame:
    """
    A sampled source line. Tachyon's frame table also keys frames by column, so one
    line can be several of its frames; equal `Frame`s are the same line, and their
    samples sum.
    """

    file: str
    function: str
    line: int

    @property
    def label(self) -> str:
        return f"{self.function}  ({Path(self.file).name}:{self.line})"


def self_samples(jsonl: str) -> Counter[Frame]:
    """
    Parse Tachyon's `--jsonl` output into the samples each line took as a stack's leaf.

    Raises `ValueError` on a format version other than 0, whose records this reads.
    Record types other than the string table, the frame table, and the final per-frame
    aggregate carry nothing self-time needs, so they are skipped.
    """
    strings: dict[int, str] = {}
    frames: dict[int, Frame] = {}
    samples: Counter[Frame] = Counter()
    for raw in jsonl.splitlines():
        record = json.loads(raw)
        if record["v"] != 0:
            raise ValueError(f"unsupported Tachyon JSONL version {record['v']!r}")
        match record["type"]:
            case "string_table":
                strings.update((entry["str_id"], entry["value"]) for entry in record["strings"])
            case "frame_table":
                frames.update(
                    (
                        entry["frame_id"],
                        Frame(
                            file=strings[entry["path_str_id"]],
                            function=strings[entry["func_str_id"]],
                            line=entry["line"],
                        ),
                    )
                    for entry in record["frames"]
                )
            case "agg" if record["kind"] == "frame" and record["scope"] == "final":
                for entry in record["entries"]:
                    samples[frames[entry["frame_id"]]] += entry["self"]
            case _:
                pass
    return samples


def self_time(samples: Counter[Frame]) -> tuple[Counter[str], Counter[str], int]:
    """
    Aggregate leaf-frame self-time by package and by frame label.

    Returns `(by_package, by_frame, total_samples)`.
    """
    by_package: Counter[str] = Counter()
    by_frame: Counter[str] = Counter()
    for frame, count in samples.items():
        by_package[package(frame.file)] += count
        by_frame[frame.label] += count
    return by_package, by_frame, samples.total()


def replay_jsonl(profile: Path) -> str:
    """Replay a binary Tachyon profile to JSONL through Tachyon's own converter."""
    with TemporaryDirectory() as directory:
        out = Path(directory) / "profile.jsonl"
        subprocess.run(
            [sys.executable, "-m", "profiling.sampling", "replay", "--jsonl", "-o", str(out), str(profile)],
            check=True,
            capture_output=True,
        )
        return out.read_text()


def report(path: Path, top: int) -> str:
    by_package, by_frame, total = self_time(self_samples(replay_jsonl(path)))
    lines = [f"===== {path.stem}  ({total} samples) =====", "  self-time by package:"]
    lines += [f"    {100 * weight / total:5.1f}%  {label}" for label, weight in by_package.most_common(top)]
    lines.append("  top self-time frames:")
    lines += [f"    {100 * weight / total:5.1f}%  {label}" for label, weight in by_frame.most_common(top)]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize per-package and per-frame self-time from Tachyon profiles.")
    parser.add_argument(
        "profiles",
        nargs="*",
        type=Path,
        help="binary Tachyon profiles (default: every *-prof.tachyon under --results)",
    )
    parser.add_argument("--results", type=Path, default=Path(__file__).parent.parent.parent / "results")
    parser.add_argument("--top", type=int, default=12, help="how many packages and frames to show per profile")
    args = parser.parse_args()
    if importlib.util.find_spec("profiling.sampling") is None:
        parser.error("replaying a Tachyon profile needs Python 3.15: rerun with UV_PYTHON=3.15")

    profiles = args.profiles or sorted(args.results.glob("*-prof.tachyon"))
    if not profiles:
        raise SystemExit(f"no profiles given and none found in {args.results}; run `just bench ... --profile` first")
    for path in profiles:
        print(report(path, args.top))
        print()


if __name__ == "__main__":
    main()
