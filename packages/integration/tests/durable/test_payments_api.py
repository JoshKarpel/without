from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Never

import httpx
import pytest
from doubles import MemoryCheckpoints
from doubles import MemoryWakeups
from integration.durable import Pass
from integration.durable.api import Payments
from integration.durable.api import payments_app
from without_http import serving

# The API served for real by `without-http` and driven by an ordinary HTTP client,
# against in-memory stores: the endpoints hold nothing else, so a container adds
# nothing to what these can prove (the compose-marked tests cover Redis itself).

WORKFLOW = "idem-key-9f2"
ORDER = {"items": {"widget": 1200, "gizmo": 800}}


@pytest.fixture
def payments() -> Payments:
    return Payments(checkpoints=MemoryCheckpoints(), wakeups=MemoryWakeups())


@pytest.fixture
async def client(payments: Payments) -> AsyncIterator[httpx.AsyncClient]:
    async with serving(payments_app(payments)) as server:
        async with httpx.AsyncClient(base_url=f"http://{server.host}:{server.port}") as client:
            yield client


def hashes(payments: Payments) -> dict[str, dict[str, object]]:
    assert isinstance(payments.checkpoints, MemoryCheckpoints)
    return payments.checkpoints.hashes


def queue(payments: Payments) -> list[str]:
    assert isinstance(payments.wakeups, MemoryWakeups)
    return list(payments.wakeups.queue)


async def test_submitting_an_order_records_it_and_makes_the_workflow_ready(
    client: httpx.AsyncClient,
    payments: Payments,
) -> None:
    response = await client.post("/orders", json=ORDER, headers={"idempotency-key": WORKFLOW})

    assert response.status_code == 202
    assert json.loads(response.text) == {"workflow": WORKFLOW, "status": f"/orders/{WORKFLOW}"}
    assert hashes(payments)[WORKFLOW] == {"order": ORDER["items"]}
    assert queue(payments) == [WORKFLOW], "the API runs nothing; it makes the workflow runnable"


async def test_submitting_the_same_key_twice_addresses_the_same_workflow(
    client: httpx.AsyncClient,
    payments: Payments,
) -> None:
    await client.post("/orders", json=ORDER, headers={"idempotency-key": WORKFLOW})
    await client.post("/orders", json=ORDER, headers={"idempotency-key": WORKFLOW})

    assert list(hashes(payments)) == [WORKFLOW], "the idempotency key *is* the workflow id"
    assert queue(payments) == [WORKFLOW, WORKFLOW], "a second pass is harmless: it finds the work recorded"


async def test_resubmitting_a_changed_basket_under_one_key_does_not_replace_the_order(
    client: httpx.AsyncClient,
    payments: Payments,
) -> None:
    # What an idempotency key promises, and what a plain overwrite would break: a
    # workflow that has already captured against the first basket must not find a
    # different one underneath it on the next pass.
    await client.post("/orders", json=ORDER, headers={"idempotency-key": WORKFLOW})
    second = await client.post(
        "/orders",
        json={"items": {"piano": 90_000}},
        headers={"idempotency-key": WORKFLOW},
    )

    assert second.status_code == 202
    assert hashes(payments)[WORKFLOW] == {"order": ORDER["items"]}, "the first order recorded is the one that runs"


async def test_an_order_without_an_idempotency_key_is_rejected(client: httpx.AsyncClient) -> None:
    response = await client.post("/orders", json=ORDER)

    assert response.status_code == 422
    assert json.loads(response.text)["field"] == "idempotency-key"


async def test_an_order_whose_body_does_not_parse_is_rejected(client: httpx.AsyncClient) -> None:
    response = await client.post("/orders", json={"items": "all of them"}, headers={"idempotency-key": WORKFLOW})

    assert response.status_code == 422
    assert json.loads(response.text) == {"error": "invalid body", "fields": 1}


async def test_confirming_records_the_approval_and_makes_the_workflow_ready(
    client: httpx.AsyncClient,
    payments: Payments,
) -> None:
    response = await client.post(f"/orders/{WORKFLOW}/confirmation", json={"approved_by": "auditor-7"})

    assert response.status_code == 202
    assert hashes(payments)[WORKFLOW] == {"approved-by": "auditor-7"}
    assert queue(payments) == [WORKFLOW]


async def test_the_status_endpoint_shows_what_the_workflow_has_recorded(
    client: httpx.AsyncClient,
    payments: Payments,
) -> None:
    await payments.checkpoints.supply(WORKFLOW, "order", ORDER["items"])
    await payments.checkpoints.supply(WORKFLOW, "paid", "pay-2000")

    response = await client.get(f"/orders/{WORKFLOW}")

    assert response.status_code == 200
    assert json.loads(response.text) == {
        "workflow": WORKFLOW,
        "recorded": {"order": ORDER["items"], "paid": "pay-2000"},
        "done": True,
    }


async def test_a_workflow_nobody_has_submitted_is_a_404(client: httpx.AsyncClient) -> None:
    response = await client.get("/orders/never-heard-of-it")

    assert response.status_code == 404
    assert json.loads(response.text) == {"error": "no workflow never-heard-of-it"}


async def test_an_unrouted_path_is_a_404(client: httpx.AsyncClient) -> None:
    response = await client.get("/nope")

    assert response.status_code == 404
    assert json.loads(response.text) == {"error": "no route for GET /nope"}


@dataclass(frozen=True, slots=True)
class BrokenCheckpoints:
    """A store whose writes fail: the app's own bug, not the client's."""

    async def load(self, workflow: str) -> dict[str, object]:  # pragma: no cover - present to satisfy the protocol
        return {}

    async def claim(self, workflow: str, lease: timedelta) -> Pass | None:  # pragma: no cover - same
        return Pass(workflow=workflow, token=1)

    async def record(self, holder: Pass, key: str, value: object) -> object:  # pragma: no cover - same
        raise RuntimeError("the store is down")

    async def transact(self, holder: Pass, key: str, effect: Never) -> object:  # pragma: no cover - uncallable
        # `Never` is how a store says it cannot co-commit anything: no caller can produce
        # an argument for this, so the method exists without being reachable.
        raise RuntimeError("the store is down")

    async def release(self, holder: Pass) -> None:  # pragma: no cover - same
        return None

    async def supply(self, workflow: str, key: str, value: object) -> object:
        raise RuntimeError("the store is down")


async def test_a_failure_that_is_not_the_requests_fault_is_a_500_not_a_422() -> None:
    # The recovery policy answers for what a *client* got wrong and nothing else, so a
    # store that cannot write falls through it and surfaces as a server error. Reading
    # a 422 here would tell the caller to fix a request that was fine.
    payments = Payments(checkpoints=BrokenCheckpoints(), wakeups=MemoryWakeups())

    async with serving(payments_app(payments)) as server:
        async with httpx.AsyncClient(base_url=f"http://{server.host}:{server.port}") as client:
            response = await client.post("/orders", json=ORDER, headers={"idempotency-key": WORKFLOW})

    assert response.status_code == 500
