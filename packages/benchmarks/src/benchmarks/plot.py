from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

# Read the per-rate vegeta results the harness left in `results/` and draw the
# latency-vs-rate and throughput-vs-rate curves for each stack, so the knee (where
# achieved throughput falls below the target arrival rate and the tail latency
# blows up) is visible at a glance. Like the harness, this shells out to vegeta and
# is run by hand, so it stays out of the unit-test path. Plot *unprofiled* runs:
# austin sampling inflates latency, so a profiled `.bin` would misrepresent it.

# Matches a clean run's stem, e.g. `without+without-http-list-r1500-d8s`. Profiled
# runs end in `-prof`, so the trailing `$` after the duration excludes them (and the
# `-d*s.bin` glob never reaches them either).
_POINT = re.compile(r"-r(\d+)-d(\d+)s$")

# The matrix cells, each keyed by the harness's `{framework}+{server}` result-file
# prefix but *ordered* and *labelled* server-first, so the plot groups by server
# then framework: the two pure-Python `without-http` cells lead, the uvloop variant
# bridges, then the two C-stack `uvicorn` cells. Reading top to bottom then walks
# the server axis from slowest transport to fastest. A cell with no results is
# skipped, so a plain two-server run still draws four series.
_STACKS = (
    "without+without-http",
    "fastapi+without-http",
    "without+without-http-uvloop",
    "without+uvicorn",
    "fastapi+uvicorn",
)

# Five validated CVD-safe hues from the data-viz palette (worst adjacent ΔE 47.2 in
# this order). Colour follows the cell, never its position, so a missing cell never
# repaints the others. Aqua and yellow are sub-3:1 on the light surface, so the
# legend supplies the visible labels the relief rule requires; identity never rests
# on colour alone.
_COLOR = {
    "without+without-http": "#2a78d6",
    "fastapi+without-http": "#008300",
    "without+without-http-uvloop": "#4a3aa7",
    "without+uvicorn": "#eda100",
    "fastapi+uvicorn": "#1baf7a",
}
_LABEL = {
    "without+without-http": "without-http + without-web",
    "fastapi+without-http": "without-http + fastapi",
    "without+without-http-uvloop": "without-http (uvloop) + without-web",
    "without+uvicorn": "uvicorn + without-web",
    "fastapi+uvicorn": "uvicorn + fastapi",
}
_SURFACE = "#fcfcfb"
_INK = "#0b0b0b"
_INK_2 = "#52514e"
_MUTED = "#898781"
_GRID = "#e1e0d9"
_AXIS = "#c3c2b7"


def _series(results: Path, stack: str, endpoint: str) -> list[tuple[int, float, float, float, float]]:
    """Per rate: (target rate, p50 ms, p99 ms, achieved throughput, CPU cores), sorted by rate."""
    # If the same rate was run at several durations, keep the longest (most samples).
    chosen: dict[int, tuple[int, Path]] = {}
    for path in results.glob(f"{stack}-{endpoint}-r*-d*s.bin"):
        match = _POINT.search(path.stem)
        if match is None:
            continue
        rate, duration = int(match.group(1)), int(match.group(2))
        if rate not in chosen or duration > chosen[rate][0]:
            chosen[rate] = (duration, path)

    rows: list[tuple[int, float, float, float, float]] = []
    for rate in sorted(chosen):
        bin_path = chosen[rate][1]
        report = subprocess.run(
            ["vegeta", "report", "-type=json", str(bin_path)], capture_output=True, text=True, check=True
        ).stdout
        data = json.loads(report)
        latencies = data["latencies"]
        cpu = bin_path.with_suffix(".cpu")
        cores = float(cpu.read_text()) if cpu.exists() else float("nan")
        rows.append((rate, latencies["50th"] / 1e6, latencies["99th"] / 1e6, data["throughput"], cores))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot latency and throughput vs target rate from vegeta results.")
    parser.add_argument("--endpoint", default="list")
    parser.add_argument("--results", type=Path, default=Path(__file__).parent.parent.parent / "results")
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    data = {stack: _series(args.results, stack, args.endpoint) for stack in _STACKS}
    data = {stack: rows for stack, rows in data.items() if rows}
    if not data:
        raise SystemExit(f"no clean results for /{args.endpoint} in {args.results}; run `just bench ...` first")
    max_rate = max(row[0] for rows in data.values() for row in rows)

    plt.switch_backend("Agg")
    # Three data panels in a 2x2 grid; the fourth cell (bottom-right) holds the
    # legend, so every panel stays clean and the shared identity lives in one place.
    figure, ((latency, throughput), (cpu, legend_ax)) = plt.subplots(2, 2, figsize=(13, 9))
    figure.patch.set_facecolor(_SURFACE)

    for axis in (latency, throughput, cpu):
        axis.set_facecolor(_SURFACE)
        axis.spines["top"].set_visible(False)
        axis.spines["right"].set_visible(False)
        axis.spines["left"].set_color(_AXIS)
        axis.spines["bottom"].set_color(_AXIS)
        axis.tick_params(colors=_MUTED, labelcolor=_INK_2)
        axis.grid(visible=True, color=_GRID, linewidth=0.8)
        axis.set_axisbelow(True)
        axis.set_xlim(0, max_rate * 1.03)
        axis.set_xlabel("target arrival rate (req/s)", color=_INK_2)

    for stack, rows in data.items():
        rates = [row[0] for row in rows]
        color = _COLOR[stack]
        # Only the p99 tail: it is what knees as the server saturates, and plotting
        # p50 too would double the lines to no gain (the median stays flat).
        latency.plot(rates, [row[2] for row in rows], color=color, linewidth=2, marker="o", markersize=6)
        throughput.plot(rates, [row[3] for row in rows], color=color, linewidth=2, marker="o", markersize=6)
        cpu.plot(rates, [row[4] for row in rows], color=color, linewidth=2, marker="o", markersize=6)

    throughput.plot([0, max_rate], [0, max_rate], linestyle=":", color=_MUTED, linewidth=1.5)
    throughput.annotate(
        "ideal (achieved = target)",
        (max_rate, max_rate),
        xytext=(-4, -10),
        textcoords="offset points",
        color=_MUTED,
        ha="right",
    )
    cpu.axhline(1.0, linestyle=":", color=_MUTED, linewidth=1.5)
    cpu.annotate("one core", (0, 1.0), xytext=(4, 4), textcoords="offset points", color=_MUTED)

    latency.set_yscale("log")
    latency.set_ylabel("p99 latency (ms, log scale)", color=_INK_2)
    throughput.set_ylabel("achieved throughput (req/s)", color=_INK_2)
    cpu.set_ylabel("server CPU (cores)", color=_INK_2)
    cpu.set_ylim(bottom=0)
    figure.suptitle(
        f"todos  GET /{args.endpoint}  —  same core, server x framework matrix",
        color=_INK,
        weight="bold",
        x=0.02,
        ha="left",
        fontsize=14,
    )

    # The fourth grid cell holds the one legend: colour is each cell's identity
    # across all three panels, with the label beside it in ink (never in the series
    # colour), so no panel needs its own key.
    legend_ax.axis("off")
    cells = [
        Line2D([], [], color=_COLOR[stack], lw=2.5, marker="o", markersize=7, label=_LABEL[stack]) for stack in data
    ]
    cell_legend = legend_ax.legend(
        handles=cells,
        title="server + framework",
        loc="center",
        frameon=False,
        labelcolor=_INK_2,
        fontsize=11,
    )
    cell_legend.get_title().set_color(_INK_2)

    out = args.out or args.results / f"{args.endpoint}-latency-throughput.png"
    figure.tight_layout(rect=(0, 0.01, 1, 0.96))
    figure.savefig(out, dpi=140)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
