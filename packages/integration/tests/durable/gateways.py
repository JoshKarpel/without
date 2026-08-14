from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field

from integration.durable import Order
from integration.durable import Payouts
from integration.durable import Services
from without_asgi import RawHeaders
from without_http import Client
from without_http import request

type JsonObject = dict[str, object]

# The effects a durable workflow reaches out to, recorded rather than performed, so a
# test can say what a pass actually did. Shared by the store-backed suites (Redis and
# Postgres) because the whole claim being tested is that they behave the same: the same
# workflow, the same assertions, a different store underneath.

# The host the in-memory client addresses the payments app by. Nothing resolves it: an
# absolute URL is what a `Client` takes, and no socket is opened either way.
BASE = "http://payments.test"


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


def paying(calls: list[str]) -> Payouts:
    """A payout's effects, recording each one so a test can see what a pass performed."""

    async def items(order_id: str) -> dict[str, int]:
        calls.append("items")
        return {"piano": 90_000, "stool": 4_000}

    async def capture(sku: str, amount: int) -> str:
        calls.append(f"capture:{sku}")
        return f"cap-{sku}"

    async def pay(order_id: str, total: int) -> str:
        calls.append("pay")
        return f"pay-{total}"

    return Payouts(items=items, capture=capture, pay=pay)


async def post_json(client: Client, path: str, payload: object, *, key: str | None = None) -> tuple[int, JsonObject]:
    """`POST` a JSON body (with an optional idempotency key) and read the whole answer."""
    headers: RawHeaders = ((b"content-type", b"application/json"),)
    if key is not None:
        headers = (*headers, (b"idempotency-key", key.encode()))
    body = json.dumps(payload).encode()
    async with request(client, "POST", f"{BASE}{path}", headers=headers, body=body) as (head, answer):
        return head.status, as_object(json.loads(await answer.read()))


async def get_json(client: Client, path: str) -> tuple[int, JsonObject]:
    """`GET` a path and read the whole answer."""
    async with request(client, "GET", f"{BASE}{path}") as (head, answer):
        return head.status, as_object(json.loads(await answer.read()))


def as_object(value: object) -> JsonObject:
    """
    Narrow a decoded JSON value to an object, failing here rather than downstream.

    Every endpoint here answers with a JSON object, and every nested field these tests
    read is one too, so anything else is the API breaking its own contract.
    """
    assert isinstance(value, dict)
    return value


async def until(
    client: Client,
    workflow: str,
    reached: Callable[[JsonObject], object],
) -> JsonObject:
    """Watch a workflow the way a UI would: poll its status until it says what we want."""
    while True:
        _status, state = await get_json(client, f"/orders/{workflow}")
        if reached(state):
            return as_object(state["recorded"])
        await asyncio.sleep(0.05)
