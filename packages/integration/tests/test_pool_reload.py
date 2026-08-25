from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from integration.pool_reload import PoolConfig
from integration.pool_reload import reloading_pool
from without_asgi import ASGIApp
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Outbound
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_asgi import make_asgi_app
from without_http import ConnectionPool
from without_http import request
from without_http import serving
from without_streams import Sample
from without_streams import Stream

# The proof for issue #27: a connection pool whose per-host concurrency bound is
# reloaded live, demonstrated end-to-end over without-http. The pool is rebuilt
# from config on each change (see `pool_reload`), so the assertion is that the
# *server-observed* concurrency ceiling changes across a reload while the same
# origin is being driven by the same sampled pool.
#
# Concurrency is measured deterministically, with no sleeps. The server holds
# every in-flight request at an `asyncio.Barrier` sized to the phase's expected
# bound and fires more requests than the bound: they clear in waves of exactly
# the bound (each wave trips the barrier and frees pool permits for the next), so
# the peak in-flight count settles at the bound itself. An over-admitting pool
# drives the peak *above* the bound; an under-admitting one never fills the
# barrier and deadlocks, which the suite's global timeout turns into a failure.


@dataclass(slots=True)
class Concurrency:
    """Server-side probe: how many requests are held in flight at once."""

    barrier: asyncio.Barrier
    in_flight: int = 0
    peak: int = 0

    @asynccontextmanager
    async def track(self) -> AsyncIterator[None]:
        self.in_flight += 1
        self.peak = max(self.peak, self.in_flight)
        try:
            await self.barrier.wait()
            yield
        finally:
            self.in_flight -= 1


@dataclass(slots=True)
class Phase:
    """A mutable holder so the test swaps the probe between phases, one server throughout."""

    concurrency: Concurrency


def probe_app(phase: Phase) -> ASGIApp:
    @asynccontextmanager
    async def lifespan() -> AsyncIterator[None]:
        yield None

    async def serve(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
        async with phase.concurrency.track():
            yield ResponseStart(status=200, headers=((b"content-type", b"text/plain; charset=utf-8"),))
            yield ResponseBody(body=b"ok", more_body=False)

    def router(state: None, scope: HttpScope) -> HttpHandler:
        return serve

    return make_asgi_app(lifespan, http=router)


async def fire(pool: Sample[ConnectionPool], url: str, count: int) -> list[int]:
    async def one() -> int:
        connection_pool = pool.current()
        async with request(connection_pool, "GET", url) as (head, body):
            await body.read()
            return head.status

    return await asyncio.gather(*(one() for _ in range(count)))


async def test_per_host_bound_tracks_a_live_config_reload() -> None:
    reload_gate = asyncio.Event()

    async def configs() -> AsyncIterator[PoolConfig]:
        yield PoolConfig(max_connections_per_host=2)
        await reload_gate.wait()  # held until the test has exercised the first bound
        yield PoolConfig(max_connections_per_host=4)

    phase = Phase(concurrency=Concurrency(barrier=asyncio.Barrier(2)))

    async with (
        serving(probe_app(phase)) as server,
        reloading_pool(configs()) as pool,
    ):
        url = f"http://{server.host}:{server.port}/"

        # Bound is 2: four requests clear in two waves of two, so the server sees
        # at most two connections at once.
        assert await fire(pool, url, 4) == [200, 200, 200, 200]
        assert phase.concurrency.peak == 2

        # Reload to bound 4 and wait for the rebuilt pool to become current.
        phase.concurrency = Concurrency(barrier=asyncio.Barrier(4))
        reload_gate.set()
        await pool.updated()

        # Same origin, same sampled pool, new bound: eight requests now clear in
        # two waves of four, so the ceiling has risen to exactly four.
        assert await fire(pool, url, 8) == [200] * 8
        assert phase.concurrency.peak == 4
