from __future__ import annotations

import argparse
import cProfile
import gc
import pstats
import statistics
import time
from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from functools import partial

from without_html import render

from benchmarks.render.workloads import Workload
from benchmarks.render.workloads import runs
from benchmarks.render.workloads import table_htpy
from benchmarks.render.workloads import table_tree_htpy
from benchmarks.render.workloads import table_tree_without
from benchmarks.render.workloads import table_without
from benchmarks.render.workloads import workloads

# An in-process CPU benchmark, which is a different shape from this package's whole-stack
# load benchmarks and so does not reuse their harness. There is no server, no socket, and
# no load generator: the thing under test is a pure function from data to a string, and
# the only honest way to time one is to call it many times in a tight loop and take the
# best result.
#
# Three decisions carry the methodology:
#
# - **Byte-identical output is checked before timing.** Four renderers that disagree about
#   their output are not comparable at any speed, so `compare` fails rather than reports.
# - **The minimum sample, not the mean.** Noise on a machine that is also doing other
#   things is one-sided: it can only make a run slower. The minimum of many repeats is
#   therefore the closest estimate of the true cost, and the spread is reported beside it
#   so a run polluted by something else is visible rather than silently averaged in.
# - **The garbage collector stays on.** `timeit` disables it, which is right for comparing
#   unrelated snippets and wrong here: building a tree allocates thousands of objects, so
#   collection is part of what the approach costs. `--gc-off` measures the other way, and
#   the difference is itself a result.


@dataclass(frozen=True, slots=True)
class Timing:
    """The distribution of per-call times for one renderer, in nanoseconds."""

    samples: tuple[float, ...]

    @property
    def best(self) -> float:
        return min(self.samples)

    @property
    def median(self) -> float:
        return statistics.median(self.samples)

    @property
    def spread(self) -> float:
        """How far the median sits above the best, as a fraction: a noise indicator."""
        return (self.median - self.best) / self.best


def measure(call: Callable[[], object], *, number: int, repeat: int, collect: bool = True) -> Timing:
    """Time `call` over `repeat` batches of `number` calls, returning per-call nanoseconds."""
    call()  # warm up: first-call imports, memoized tag markup, template globals
    samples = []
    was_enabled = gc.isenabled()
    if not collect:
        gc.disable()
    try:
        for _ in range(repeat):
            start = time.perf_counter_ns()
            for _ in range(number):
                call()
            samples.append((time.perf_counter_ns() - start) / number)
    finally:
        if was_enabled:
            gc.enable()
    return Timing(tuple(samples))


def agreed_output(workload: Workload) -> str:
    """
    The output every renderer produces, having proven they all produce the same one.

    A mismatch is a broken benchmark rather than an interesting finding, so it stops the
    run and shows where the two diverge instead of being reported as a difference in
    speed between two different jobs.
    """
    outputs = {name: render() for name, render in workload.renderers.items()}
    reference_name, reference = next(iter(outputs.items()))
    for name, output in outputs.items():
        if output != reference:
            index = next(
                (i for i, (left, right) in enumerate(zip(reference, output, strict=False)) if left != right),
                min(len(reference), len(output)),
            )
            raise SystemExit(
                f"{workload.name}: {name} disagrees with {reference_name} at byte {index}\n"
                f"  {reference_name}: ...{reference[max(0, index - 40) : index + 40]!r}\n"
                f"  {name}: ...{output[max(0, index - 40) : index + 40]!r}"
            )
    return reference


def compare(workload: Workload, *, number: int, repeat: int, collect: bool) -> None:
    """Time every renderer for one workload and print the comparison."""
    output = agreed_output(workload)
    size = len(output.encode())
    print(f"\n{workload.name}: {workload.description}")
    print(f"  {workload.elements:,} elements, {size:,} bytes of output, identical from every renderer")
    timings = {
        name: measure(render, number=number, repeat=repeat, collect=collect)
        for name, render in workload.renderers.items()
    }
    floor = timings["f-string"].best
    print(f"  {'renderer':<14}{'best':>12}{'ns/element':>12}{'MB/s':>10}{'vs floor':>10}{'spread':>9}")
    for name, timing in sorted(timings.items(), key=lambda item: item[1].best):
        print(
            f"  {name:<14}{timing.best / 1e6:>10.3f}ms{timing.best / workload.elements:>12.0f}"
            f"{size / (timing.best / 1e9) / 1e6:>10.0f}{timing.best / floor:>9.2f}x{timing.spread:>8.1%}"
        )


def phases(*, rows: int, number: int, repeat: int, collect: bool) -> None:
    """
    Split the tree renderers into their build and render halves.

    Only the tree-based contenders have two phases to separate, and the split is the whole
    reason to care: they are different code doing different work, and an optimization that
    helps one does nothing for the other. It is also the half of the design that the other
    two contenders have no equivalent of, since a template and an f-string go from data to
    a string in one pass and leave nothing behind.
    """
    rows_data = runs(rows)
    print(f"\nphases: {rows}-row table, build and render timed separately")
    print(f"  {'renderer':<14}{'build':>12}{'render':>12}{'total':>12}{'build share':>13}")
    for name, build, emit in (
        ("without-html", partial(table_tree_without, rows_data), render),
        ("htpy", partial(table_tree_htpy, rows_data), str),
    ):
        tree = build()
        build_timing = measure(build, number=number, repeat=repeat, collect=collect)
        render_timing = measure(partial(emit, tree), number=number, repeat=repeat, collect=collect)
        total = build_timing.best + render_timing.best
        print(
            f"  {name:<14}{build_timing.best / 1e6:>10.3f}ms{render_timing.best / 1e6:>10.3f}ms"
            f"{total / 1e6:>10.3f}ms{build_timing.best / total:>12.0%}"
        )


def scaling(*, sizes: Sequence[int], number: int, repeat: int, collect: bool) -> None:
    """
    Time the table at several sizes, to show whether cost is linear in elements.

    A per-element figure that holds steady across two orders of magnitude says the cost is
    per-node and there is no hidden quadratic; one that climbs says there is.
    """
    print("\nscaling: the same table at several row counts")
    print(f"  {'rows':>8}{'without-html':>16}{'ns/element':>12}{'htpy':>14}{'ns/element':>12}")
    for size in sizes:
        rows_data = runs(size)
        elements = 2 + 4 + size * 5
        without_timing = measure(partial(table_without, rows_data), number=number, repeat=repeat, collect=collect)
        htpy_timing = measure(partial(table_htpy, rows_data), number=number, repeat=repeat, collect=collect)
        print(
            f"  {size:>8,}{without_timing.best / 1e6:>14.3f}ms{without_timing.best / elements:>12.0f}"
            f"{htpy_timing.best / 1e6:>12.3f}ms{htpy_timing.best / elements:>12.0f}"
        )


def profile(*, rows: int, number: int) -> None:
    """
    Attribute time within `without-html` by function, using a deterministic profiler.

    Its absolute numbers are inflated by per-call instrumentation, and it charges that
    overhead in proportion to call count, so a function called once per element looks
    worse here than it is. Read the *call counts* and the ordering, not the times.
    """
    rows_data = runs(rows)
    profiler = cProfile.Profile()
    profiler.enable()
    for _ in range(number):
        table_without(rows_data)
    profiler.disable()
    print(f"\nprofile: {number} renders of a {rows}-row table (times inflated by instrumentation)")
    pstats.Stats(profiler).sort_stats("tottime").print_stats(12)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1000, help="rows in the table workload")
    parser.add_argument("--cards", type=int, default=200, help="cards in the page workload")
    parser.add_argument("--fragment-rows", type=int, default=20, help="rows in the fragment workload")
    parser.add_argument("--depth", type=int, default=300, help="nesting depth in the deep workload")
    parser.add_argument("--number", type=int, default=20, help="calls per timed batch")
    parser.add_argument("--repeat", type=int, default=9, help="timed batches per renderer")
    parser.add_argument("--gc-off", action="store_true", help="measure with the collector disabled")
    parser.add_argument("--phases", action="store_true", help="split build from render")
    parser.add_argument("--scaling", action="store_true", help="sweep the table size")
    parser.add_argument("--profile", action="store_true", help="attribute time by function")
    arguments = parser.parse_args()

    collect = not arguments.gc_off
    print(f"garbage collection {'off' if arguments.gc_off else 'on'}, best of {arguments.repeat} batches")
    for workload in workloads(
        rows=arguments.rows,
        cards=arguments.cards,
        fragment_rows=arguments.fragment_rows,
        depth=arguments.depth,
    ):
        compare(workload, number=arguments.number, repeat=arguments.repeat, collect=collect)
    if arguments.phases:
        phases(rows=arguments.rows, number=arguments.number, repeat=arguments.repeat, collect=collect)
    if arguments.scaling:
        scaling(sizes=(10, 100, 1_000, 10_000), number=max(1, arguments.number // 4), repeat=5, collect=collect)
    if arguments.profile:
        profile(rows=arguments.rows, number=arguments.number)


if __name__ == "__main__":
    main()
