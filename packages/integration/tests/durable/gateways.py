from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from dataclasses import field

import httpx
from integration.durable import Order
from integration.durable import Payouts
from integration.durable import Services

# The effects a durable workflow reaches out to, recorded rather than performed, so a
# test can say what a pass actually did. Shared by the store-backed suites (Redis and
# Postgres) because the whole claim being tested is that they behave the same: the same
# workflow, the same assertions, a different store underneath.


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


async def until(client: httpx.AsyncClient, workflow: str, reached: object) -> dict[str, object]:
    """Watch a workflow the way a UI would: poll its status until it says what we want."""
    while True:
        state = json.loads((await client.get(f"/orders/{workflow}")).text)
        if reached(state):  # type: ignore[operator]
            recorded: dict[str, object] = state["recorded"]
            return recorded
        await asyncio.sleep(0.05)
