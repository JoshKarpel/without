from __future__ import annotations

import os
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis
from without_durability import Durable
from without_durability import MemoryCheckpointer
from without_durability import MemoryScheduler
from without_durability import SplitDurable
from without_durability_postgres import PostgresCheckpointer
from without_durability_postgres import PostgresDurable
from without_durability_postgres import PostgresScheduler
from without_durability_postgres import migrate as migrate_postgres
from without_durability_redis import RedisCheckpointer
from without_durability_redis import RedisSetScheduler
from without_durability_sqlite import SqliteCheckpointer
from without_durability_sqlite import SqliteDurable
from without_durability_sqlite import SqliteScheduler
from without_durability_sqlite import connect
from without_durability_sqlite import migrate as migrate_sqlite

# Every `Durable` this repo ships, behind one fixture, so a workflow-level test is written
# once and run against all of them. That is what the seam is *for*, and a suite that
# proves it is worth more than four suites that each prove it for one store.
#
# The two that need a server skip when `just test` has not published its address; the
# other two always run, which keeps the shape of these tests honest on a machine with no
# container runtime.

STORES = ("memory", "redis", "postgres", "sqlite")


def published(variable: str) -> tuple[str, int]:
    """The host and port `just test` published for a compose service, or a skip."""
    address = os.environ.get(variable)
    if not address:  # pragma: no cover - the arm that runs is the one where this file is uncovered
        pytest.skip(f"{variable} is unset: run `just test`, which starts the services in compose.yaml")
    # podman-compose reports the published port alone, docker compose the bind address it
    # is published on (`0.0.0.0:32768`), which is a wildcard a client cannot dial. Either
    # way the loopback address is where the port is reachable.
    host, _, port = address.strip().rpartition(":")
    return host or "127.0.0.1", int(port)


@pytest.fixture(params=STORES)
async def durable(request: pytest.FixtureRequest, tmp_path: Path, workflow: str) -> AsyncIterator[Durable]:
    """
    One `Durable` per store, namespaced per test so parallel runs do not collide.

    A deployment would share one namespace, because sharing the queue *is* how work
    spreads between workers; a suite running against one server cannot.
    """
    match request.param:
        case "memory":
            yield SplitDurable(MemoryCheckpointer(), MemoryScheduler())
        case "redis":
            host, port = published("WITHOUT_TESTS_REDIS")
            # `decode_responses=True` is the app's call to make, and this app makes it: it
            # owns both ends of every key it touches, so nothing downstream has to ask
            # whether a value came back as bytes.
            client = Redis(host=host, port=port, decode_responses=True)
            try:
                yield SplitDurable(
                    RedisCheckpointer(redis=client),
                    RedisSetScheduler(redis=client, namespace=workflow),
                )
            finally:
                await client.aclose()
        case "postgres":
            host, port = published("WITHOUT_TESTS_POSTGRES")
            pool = AsyncConnectionPool(
                f"postgresql://postgres:without@{host}:{port}/without",
                min_size=1,
                max_size=8,
                open=False,
            )
            await pool.open(wait=True)
            try:
                await migrate_postgres(pool)
                yield PostgresDurable(
                    PostgresCheckpointer(pool=pool),
                    PostgresScheduler(pool=pool, namespace=workflow),
                )
            finally:
                await pool.close()
        case _:
            database = connect(tmp_path / "workflows.db")
            try:
                await migrate_sqlite(database)
                yield SqliteDurable(
                    SqliteCheckpointer(database=database),
                    SqliteScheduler(database=database, namespace=workflow),
                )
            finally:
                database.connection.close()
