from __future__ import annotations

import pytest
from benchmarks.render.bench import agreed_output
from benchmarks.render.bench import measure
from benchmarks.render.workloads import Workload
from benchmarks.render.workloads import workloads

# The benchmark's one correctness property, run at a size that costs nothing: every
# renderer produces the same bytes. Without it a "result" can be a renderer quietly doing
# less work than the others, which is the way a benchmark most often lies.

TINY = workloads(rows=3, cards=2, fragment_rows=2, depth=4)


@pytest.mark.parametrize("workload", TINY, ids=lambda workload: workload.name)
def test_every_renderer_agrees_byte_for_byte(workload: Workload) -> None:
    assert agreed_output(workload)


@pytest.mark.parametrize("workload", TINY, ids=lambda workload: workload.name)
def test_every_renderer_produces_well_formed_markup(workload: Workload) -> None:
    output = agreed_output(workload)
    assert output.count("<") == output.count(">")


def test_a_disagreeing_renderer_stops_the_run() -> None:
    # The guard has to fail loudly, so prove it does rather than trusting that it would.
    broken = Workload(
        name="broken",
        description="one renderer that disagrees",
        elements=1,
        renderers={"a": lambda: "<p>x</p>", "b": lambda: "<p>y</p>"},
    )
    with pytest.raises(SystemExit, match="disagrees with"):
        agreed_output(broken)


def test_measure_reports_one_sample_per_batch() -> None:
    timing = measure(lambda: None, number=2, repeat=3)

    assert len(timing.samples) == 3
    assert timing.best <= timing.median
