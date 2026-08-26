from __future__ import annotations

from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass

from without_http import ConnectionPool
from without_streams import Sample
from without_streams import Stream
from without_streams import sample

# Can a connection pool's config be reloaded live? Yes, and without any
# resizable primitive. A pool bound (`max_connections_per_host`) is enforced by a
# fixed-size `asyncio.Semaphore` built once, which cannot be resized. Rather than
# reach for a mutable, resizable gate, take the values-over-places move to its
# conclusion: the pool *is* a value derived from config, so a config change
# rebuilds the whole pool instead of mutating one field of a live one. Readers
# sample the latest pool through a `Context[ConnectionPool]` and always see the
# newest. This generalizes for free: because the entire pool is rebuilt, *any*
# field (timeouts, socket options, http2 policy) becomes live-reloadable with no
# per-field machinery.


@dataclass(frozen=True, slots=True)
class PoolConfig:
    """
    The subset of `ConnectionPool` construction that a reload drives.

    Only fields that a deployment retunes live belong here; the rest keep their
    `ConnectionPool` defaults. Every field composes automatically, since `build`
    hands the whole value to a fresh pool.
    """

    max_connections_per_host: int | None = None
    max_keepalive_per_host: int | None = None


def build_pool(config: PoolConfig) -> ConnectionPool:
    return ConnectionPool(
        max_connections_per_host=config.max_connections_per_host,
        max_keepalive_per_host=config.max_keepalive_per_host,
    )


@asynccontextmanager
async def reloading_pool(
    configs: Stream[PoolConfig],
    *,
    build: Callable[[PoolConfig], ConnectionPool] = build_pool,
) -> AsyncIterator[Sample[ConnectionPool]]:
    """
    Sample a `ConnectionPool` that is rebuilt whenever config changes.

    Feed it a `Stream[PoolConfig]` (from `without_configmap.watch_config` in a
    real deployment, or any config source): each value builds a fresh pool with
    `build`, and readers see the newest through `current()`. Wait for a reload to
    land with `updated()`, the behavior edge's "await next value" signal.

    A superseded pool is closed once its successor becomes current. The handoff
    is graceful for HTTP/1.1: `ConnectionPool.aclose` reaps only *idle*
    connections, so a request already checked out on the old pool finishes on its
    connection and closes it on check-in, while new requests use the new pool. A
    request captures `current()` once and holds that pool for its duration, so a
    reload mid-request never pulls the pool out from under it.

    A caveat inherited from teardown ordering: if the `configs` stream is still
    live when the block exits (a real file watcher never ends), the background
    drain is cancelled and a just-superseded pool may skip its close. A source
    that ends (as a test's does) closes cleanly.
    """

    async def pools() -> AsyncIterator[ConnectionPool]:
        superseded: ConnectionPool | None = None
        async for config in configs:
            pool = build(config)
            yield pool
            # Reached only once the drain pulls the *next* config, i.e. after the
            # successor is already current, so the old pool is closed strictly
            # after nothing new will be routed to it.
            if superseded is not None:
                await superseded.aclose()
            superseded = pool

    async with sample(pools()) as pool:
        try:
            yield pool
        finally:
            await pool.current().aclose()
