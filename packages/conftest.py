from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable

import pytest

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


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
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
