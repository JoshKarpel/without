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
from without_durability_redis import RedisStreamScheduler
from without_durability_sqlite import SqliteCheckpointer
from without_durability_sqlite import SqliteDurable
from without_durability_sqlite import SqliteScheduler
from without_durability_sqlite import connect
from without_durability_sqlite import migrate as migrate_sqlite

# Every `Durable` this repo ships, behind one fixture, so a workflow-level test is written
# once and run against all of them. That is what the interface is *for*, and a suite that
# proves it is worth more than four suites that each prove it for one store.
#
# The two that need a server skip when `just test` has not published its address; the
# other two always run, which keeps the shape of these tests honest on a machine with no
# container runtime.

# Both Redis queues, because they are not one store with a knob: the stream carries a
# consumer group, a pending list, an acknowledgement, and a Lua script that moves a
# workflow from the sleepers to the queue, where the sorted set answers all four by
# scoring visibility and has `wake_due`, `reclaim`, and `prepare` do nothing at all. So
# the stream is the only parameter here that exercises the worker's timer and its
# takeover arm end to end, and running the same workflows over both is what says the
# `Scheduler` interface holds across that difference rather than across two spellings of one
# design.
STORES = ("memory", "redis-stream", "redis-set", "postgres", "sqlite")


def published(variable: str) -> tuple[str, int]:
    """The host and port `just test` published for a compose service, or a skip."""
    address = os.environ.get(variable)
    if not address:  # pragma: no cover - the arm that runs is the one where this file is uncovered
        pytest.skip(f"{variable} is unset: run `just test`, which starts the services in compose.yaml")
    # podman-compose reports the published port alone, docker compose the bind address it
    # is published on (`0.0.0.0:32768`), which is a wildcard a client cannot dial. Either
    # way the loopback address is where the port is reachable, so only the port is taken.
    return "127.0.0.1", int(address.strip().rpartition(":")[2])


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
        case "redis-stream":
            host, port = published("WITHOUT_TESTS_REDIS")
            client = Redis(host=host, port=port, decode_responses=True)
            try:
                scheduler = RedisStreamScheduler(redis=client, namespace=workflow)
                # The one queue here with something to create. `work` calls this itself and
                # it is idempotent, but a test that only submits (and never runs a worker)
                # would otherwise leave a stream with no group for the *next* reader to
                # find, so the fixture hands out a store that is ready to be read from.
                await scheduler.prepare()
                yield SplitDurable(RedisCheckpointer(redis=client), scheduler)
            finally:
                await client.aclose()
        case "redis-set":
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
        case "sqlite":
            database = connect(tmp_path / "workflows.db")
            try:
                await migrate_sqlite(database)
                yield SqliteDurable(
                    SqliteCheckpointer(database=database),
                    SqliteScheduler(database=database, namespace=workflow),
                )
            finally:
                # `aclose`, never `connection.close()`: this test's worker task is
                # cancelled just before teardown, which leaves the statement it was
                # running still in flight on a thread, and closing on top of that
                # segfaults rather than raising.
                await database.aclose()
        case unknown:  # pragma: no cover - unreachable while `STORES` and the arms above agree, which is the point
            # Not a `case _` falling through to one of the stores: that would make a typo
            # in `STORES` run some other store twice under the wrong name, and a suite
            # claiming four-store coverage while proving three. `request.param` is a plain
            # `str` to the type checker, so this is the runtime form of `assert_never`.
            raise ValueError(f"{unknown!r} names no store in this fixture")
