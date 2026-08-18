from __future__ import annotations

import asyncio
import gc
import sys
from collections.abc import Callable

import pytest
from hypothesis import is_hypothesis_test

# Shared across every workspace package: pytest loads this conftest for all tests
# under `packages/**/tests`, so the whole suite runs under every event loop the
# project supports. `without` never pins a loop: the app chooses one at its
# entrypoint (stdlib `asyncio.run`, or uvloop), so the tests mirror that choice
# instead of privileging one, and prove behaviour holds on each. pytest-asyncio
# calls this hook to discover the loop factories it parametrizes each async test
# over; with `asyncio_mode = "auto"` that covers every async test in the workspace.

type LoopFactory = Callable[[], asyncio.AbstractEventLoop]

_LOOP_FACTORIES: dict[str, LoopFactory] = {"asyncio": asyncio.new_event_loop}

# uvloop ships no Windows wheels, so it is a non-Windows dependency (see pyproject). Branch
# on the platform, not a failed import: a missing uvloop where it *should* exist then fails
# loudly rather than silently degrading the suite to asyncio-only. The branch is excluded from
# the coverage gate because no single platform exercises both arms.
if sys.platform != "win32":  # pragma: no cover
    import uvloop

    _LOOP_FACTORIES["uvloop"] = uvloop.new_event_loop


def pytest_asyncio_loop_factories() -> dict[str, LoopFactory]:
    return _LOOP_FACTORIES


def pytest_runtest_teardown() -> None:
    # A leaked socket or transport is only reported when its deallocator runs, and a
    # transport sits in a reference cycle with its protocol, so refcounting never frees
    # it: the `ResourceWarning` waits for a collection and lands on whichever test the
    # collector happened to interrupt, often in another file entirely. Collecting here
    # pins it to the test that leaked. pytest's own unraisable plugin drains the queue
    # `trylast` in this same phase, so what this frees is reported against this test.
    #
    # Generation 1, not a full collection: an object abandoned by the test that just ran
    # has survived at most a couple of collections, so it is young, and asking for every
    # generation costs the suite about 2.5x while this costs nothing measurable.
    gc.collect(1)


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # The global timeout is there to catch a deadlock, which is a property of one call:
    # a server that never replies hangs whether it is asked once or a hundred times. A
    # `@given` test is the case the bound does not describe, since its duration is the
    # example count times the body, so what it measures is how fast the machine is. Left
    # under the bound, the slowest property test in the suite crosses five seconds on a
    # loaded macOS runner, and `timeout_method = "thread"` kills the process rather than
    # failing the test, so the overrun reads as `node down: Not properly terminated` and
    # costs the whole xdist worker's output. Hypothesis has its own per-example
    # `deadline` for the thing a timeout would have caught here.
    #
    # `is_hypothesis_test` rather than the `hypothesis` marker: that marker is added by
    # Hypothesis's own plugin from this same hook, and a conftest runs before the plugins
    # registered by entry point, so it is not there to read yet.
    for item in items:
        if isinstance(item, pytest.Function) and is_hypothesis_test(item.obj):
            item.add_marker(pytest.mark.timeout(0))

    # Enforce the convention that every `@pytest.mark.security` test names the gap it
    # closes as its first positional argument (see the marker note in pyproject). The
    # guard arms are excluded from coverage because a healthy suite never trips them:
    # they exist to make a *future* undocumented mark fail loudly at collection.
    undocumented: list[str] = []
    for item in items:
        marker = item.get_closest_marker("security")
        if marker is None:
            continue
        gap = marker.args[0] if marker.args else None
        if not (isinstance(gap, str) and gap.strip()):  # pragma: no cover - the suite keeps every mark documented
            undocumented.append(item.nodeid)
    if undocumented:  # pragma: no cover - unreachable while the convention holds
        raise pytest.UsageError(
            "every @pytest.mark.security test must name the gap it closes as its first argument; missing on:\n  "
            + "\n  ".join(undocumented)
        )
