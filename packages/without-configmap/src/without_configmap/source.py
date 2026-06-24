# A behavior source backed by a Kubernetes ConfigMap mount. Watches the mount
# directory (not a file) because a projected ConfigMap swaps an atomic ..data
# symlink rather than rewriting files in place, so a per-file watch can miss
# updates. On each change the whole mount is reparsed to its desired state
# (declarative) instead of applying deltas. Feed the result to without.sample.

from __future__ import annotations

from collections.abc import AsyncIterator
from collections.abc import Callable
from pathlib import Path

from watchfiles import awatch
from without.contracts import Stream

type Changes = Callable[[Path], AsyncIterator[object]]


async def _awatch_changes(mount: Path) -> AsyncIterator[object]:
    async for batch in awatch(mount):
        yield batch


def watch_config[T](
    mount: Path,
    parse: Callable[[Path], T],
    *,
    changes: Changes = _awatch_changes,
) -> Stream[T]:
    """The parsed config now, and a freshly parsed value on every mount change.

    `parse` is the boundary: it turns the mount directory into a validated
    value. `changes` is the source of reload signals, injectable so tests can
    drive reloads deterministically without real filesystem events; it defaults
    to watching the mount directory with `watchfiles`.
    """

    async def source() -> AsyncIterator[T]:
        yield parse(mount)
        async for _change in changes(mount):
            yield parse(mount)

    return source()
