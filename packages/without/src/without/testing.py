# A helper for testing time-dependent processors by nudging the event loop.
# Building a `Stream` from an iterable and draining one back to a list are not
# test-only, so they live in `without` itself as `stream` and `collect`.

from __future__ import annotations

import asyncio


async def tick() -> None:
    """Let the event loop run ready tasks one step, e.g. to nudge a `sample` drain.

    TODO: this advances the loop only a single step, so it relies on the source
    under test draining in one activation. Replace callers with an explicit
    "await next update" signal on the sampled context so tests assert on
    post-update state deterministically instead of by yielding once.
    """
    await asyncio.sleep(0)
