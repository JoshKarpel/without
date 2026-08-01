from __future__ import annotations

import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from dataclasses import field
from uuid import uuid4

import pytest
from integration.durable import Order
from integration.durable import Reached
from integration.durable import RedisCheckpoints
from integration.durable import Services
from integration.durable import fulfilment
from integration.durable import run_saga
from integration.durable import unwinding
from redis.asyncio import Redis

# `just test` starts the services in compose.yaml and publishes each address; these
# tests drive the real server it started rather than a fake, and skip when it did not
# (no podman on this machine, or pytest run directly).
pytestmark = pytest.mark.compose

ORDER = Order(order_id="o-42", sku="gizmo", cents=1999)


@pytest.fixture
async def redis() -> AsyncIterator[Redis]:
    published = os.environ.get("WITHOUT_TESTS_REDIS")
    if not published:  # pragma: no cover - the arm that runs is the one where this whole file is uncovered
        pytest.skip("WITHOUT_TESTS_REDIS is unset: run `just test`, which starts the services in compose.yaml")

    # podman-compose reports the published port alone, docker compose the bind address
    # it is published on (`0.0.0.0:32768`), which is a wildcard a client cannot dial.
    # Either way the loopback address is where the port is reachable.
    host, _, port = published.strip().rpartition(":")
    client = Redis(host=host or "127.0.0.1", port=int(port))
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def workflow() -> str:
    # Every test gets its own idempotency key rather than flushing the database,
    # because the stack is shared by every worker in the session: a flush would pull
    # another test's checkpoint out from under it.
    return f"test-{uuid4().hex}"


@dataclass(slots=True)
class Gateway:
    calls: list[str] = field(default_factory=list)
    broken: set[str] = field(default_factory=set)

    async def perform(self, effect: str, result: str) -> str:
        self.calls.append(effect)
        if effect in self.broken:
            raise RuntimeError(f"{effect} is down")
        return result

    def services(self) -> Services:
        async def charge(order: Order) -> str:
            return await self.perform("charge", f"ch-{order.order_id}")

        async def reserve(order: Order) -> str:
            return await self.perform("reserve", f"rs-{order.sku}")

        async def ship(charge_id: str, reservation_id: str) -> str:
            return await self.perform("ship", f"tr-{charge_id}-{reservation_id}")

        async def refund(charge_id: str) -> str:
            return await self.perform("refund", f"rf-{charge_id}")

        async def release(reservation_id: str) -> str:
            return await self.perform("release", f"rl-{reservation_id}")

        return Services(charge=charge, reserve=reserve, ship=ship, refund=refund, release=release)


async def test_a_workflow_resumes_from_a_checkpoint_left_in_redis_by_a_dead_process(
    redis: Redis,
    workflow: str,
) -> None:
    # The whole point of the string keys, end to end: the first "process" leaves a
    # hash behind, and a second one that shares nothing with it but the workflow's
    # id picks the run up from that hash alone.
    checkpoints = RedisCheckpoints(redis=redis)
    crashed = Gateway(broken={"ship"})
    services = crashed.services()

    with pytest.raises(RuntimeError, match="ship is down"):
        await run_saga(fulfilment(services), unwinding(services), Reached.of, checkpoints, workflow, ORDER)

    # What an operator sees with `redis-cli`: one hash per workflow, one field per
    # completed step, each holding that step's result as JSON.
    assert set(await redis.hkeys(f"workflow:{workflow}")) == {b"charged", b"reserved"}
    assert await redis.hget(f"workflow:{workflow}", "charged") == b'"ch-o-42"'
    assert await redis.ttl(f"workflow:{workflow}") > 0, "a checkpoint expires on its own rather than being swept"

    recovered = Gateway()
    receipt = await run_saga(
        fulfilment(recovered.services()),
        unwinding(recovered.services()),
        Reached.of,
        checkpoints,
        workflow,
        ORDER,
    )

    assert receipt["tracking"] == "tr-ch-o-42-rs-gizmo"
    assert recovered.calls == ["ship"], "the charge and the reservation were read back out of Redis"


async def test_a_compensation_is_recorded_under_its_own_key(redis: Redis, workflow: str) -> None:
    checkpoints = RedisCheckpoints(redis=redis)
    gateway = Gateway(broken={"ship"})
    services = gateway.services()

    with pytest.raises(RuntimeError, match="ship is down"):
        await run_saga(fulfilment(services), unwinding(services), Reached.of, checkpoints, workflow, ORDER)

    assert await checkpoints.load(f"{workflow}:unwind") == {
        "refunded": "rf-ch-o-42",
        "released": "rl-rs-gizmo",
        "unwound": {"refunded": "rf-ch-o-42", "released": "rl-rs-gizmo"},
    }
    assert sorted(gateway.calls[-2:]) == ["refund", "release"]
