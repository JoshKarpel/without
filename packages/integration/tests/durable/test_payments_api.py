from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Never

import httpx
import pytest
from integration.durable.api import MAX_WORKFLOW_ID
from integration.durable.api import Payments
from integration.durable.api import payments_app
from without_durability import MemoryCheckpointer
from without_durability import MemoryScheduler
from without_durability import Pass
from without_durability import Recorded
from without_durability import SplitDurable
from without_http import serving

# The API served for real by `without-http` and driven by an ordinary HTTP client,
# against in-memory stores: the endpoints hold nothing else, so a container adds
# nothing to what these can prove (the compose-marked tests cover the real stores).

WORKFLOW = "idem-key-9f2"
ORDER = {"items": {"widget": 1200, "gizmo": 800}}


@pytest.fixture
def payments() -> Payments:
    return Payments(durable=SplitDurable(MemoryCheckpointer(), MemoryScheduler()))


@pytest.fixture
async def client(payments: Payments) -> AsyncIterator[httpx.AsyncClient]:
    async with serving(payments_app(payments)) as server:
        async with httpx.AsyncClient(base_url=f"http://{server.host}:{server.port}") as client:
            yield client


async def recorded(payments: Payments, workflow: str = WORKFLOW) -> dict[str, object]:
    """What the store holds for a workflow, read back through the interface rather than raw."""
    return await payments.durable.checkpointer.load(workflow)


def workflows(payments: Payments) -> list[str]:
    """Which workflows the store has heard of at all, which `load` alone cannot say."""
    assert isinstance(payments.durable.checkpointer, MemoryCheckpointer)
    return list(payments.durable.checkpointer.hashes)


def queue(payments: Payments) -> list[str]:
    assert isinstance(payments.durable.scheduler, MemoryScheduler)
    return list(payments.durable.scheduler.queue)


async def test_submitting_an_order_records_it_and_makes_the_workflow_ready(
    client: httpx.AsyncClient,
    payments: Payments,
) -> None:
    response = await client.post("/orders", json=ORDER, headers={"idempotency-key": WORKFLOW})

    assert response.status_code == 202
    assert json.loads(response.text) == {"workflow": WORKFLOW, "status": f"/orders/{WORKFLOW}"}
    assert await recorded(payments) == {"order": ORDER["items"]}
    assert queue(payments) == [WORKFLOW], "the API runs nothing; it makes the workflow runnable"


async def test_submitting_the_same_key_twice_addresses_the_same_workflow(
    client: httpx.AsyncClient,
    payments: Payments,
) -> None:
    await client.post("/orders", json=ORDER, headers={"idempotency-key": WORKFLOW})
    await client.post("/orders", json=ORDER, headers={"idempotency-key": WORKFLOW})

    assert workflows(payments) == [WORKFLOW], "the idempotency key *is* the workflow id"
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
    assert await recorded(payments) == {"order": ORDER["items"]}, "the first order recorded is the one that runs"


async def test_an_order_without_an_idempotency_key_is_rejected(client: httpx.AsyncClient) -> None:
    response = await client.post("/orders", json=ORDER)

    assert response.status_code == 422
    assert json.loads(response.text)["field"] == "idempotency-key"


async def test_an_order_whose_idempotency_key_is_empty_is_rejected(
    client: httpx.AsyncClient,
    payments: Payments,
) -> None:
    # A header that is present and blank, which is not the same as absent: it reaches the
    # parser as a value, and it would name a workflow every empty key shared.
    response = await client.post("/orders", json=ORDER, headers={"idempotency-key": ""})

    assert response.status_code == 422
    assert json.loads(response.text)["field"] == "idempotency-key"
    assert workflows(payments) == [], "no workflow was opened under the empty id"


async def test_an_order_whose_idempotency_key_is_too_long_is_rejected(
    client: httpx.AsyncClient,
    payments: Payments,
) -> None:
    # The one thing a well-behaved sender's UUID does not establish: an id becomes key
    # structure in the store, so an unbounded one is unbounded storage the client picks.
    response = await client.post("/orders", json=ORDER, headers={"idempotency-key": "k" * (MAX_WORKFLOW_ID + 1)})

    assert response.status_code == 422
    assert json.loads(response.text)["field"] == "idempotency-key"
    assert await recorded(payments) == {}, "nothing reached the store under the id it refused"


@pytest.mark.parametrize("key", ["{tenant}-9f2", "idem-{", "idem-}"])
async def test_an_order_whose_idempotency_key_carries_a_hash_tag_is_rejected(
    client: httpx.AsyncClient,
    payments: Payments,
    key: str,
) -> None:
    # Braces delimit Redis Cluster's hash tag, so an id carrying its own decides which
    # slot its workflow lands on. Correctness survives that (a workflow's two keys still
    # agree on the tag), but a sender that puts the same tag on every request puts the
    # whole deployment on one node, which is not a choice the sender gets to make.
    response = await client.post("/orders", json=ORDER, headers={"idempotency-key": key})

    assert response.status_code == 422
    assert json.loads(response.text)["field"] == "idempotency-key"
    assert workflows(payments) == [], "no workflow was opened under an id the store would have to parse"


@pytest.mark.parametrize("key", ["tenant-a/order-7", "order?7", "order#7", "order%2F7", "order 7"])
async def test_an_order_whose_idempotency_key_would_not_survive_a_url_is_rejected(
    client: httpx.AsyncClient,
    payments: Payments,
    key: str,
) -> None:
    # This app puts the id in a path: it answers with `/orders/{workflow}` and takes the
    # approval on `/orders/{workflow}/confirmation`. A key holding a path delimiter would
    # otherwise open a perfectly good workflow whose status URL resolves to nothing and
    # whose payout can never be approved, which for a payout above the threshold means one
    # that can never finish. Percent-escaping does not recover it either, since the header
    # is stored as sent and the path arrives decoded.
    response = await client.post("/orders", json=ORDER, headers={"idempotency-key": key})

    assert response.status_code == 422
    assert json.loads(response.text)["field"] == "idempotency-key"
    assert workflows(payments) == [], "no workflow was opened under an id this app cannot address"


@pytest.mark.parametrize("items", [{"piano": -8500}, {"piano": 0}, {"piano": True}], ids=["negative", "zero", "bool"])
async def test_an_order_whose_line_items_are_not_positive_amounts_is_rejected(
    client: httpx.AsyncClient,
    payments: Payments,
    items: dict[str, object],
) -> None:
    # The approval gate compares a *sum*, so a negative line item makes it a net rather
    # than a total: a basket of 90,000 and -85,000 captures 90,000 through a gate that
    # only ever saw 5,000. A `bool` is an `int` in Python and pydantic's lax mode coerces
    # it, so `true` would be captured as one cent.
    response = await client.post("/orders", json={"items": items}, headers={"idempotency-key": WORKFLOW})

    assert response.status_code == 422
    assert await recorded(payments) == {}, "nothing reached the store under an order it refused"


async def test_an_order_whose_idempotency_key_is_as_long_as_the_bound_allows_is_accepted(
    client: httpx.AsyncClient,
) -> None:
    # The bound itself, so a change to it cannot silently become off-by-one.
    response = await client.post("/orders", json=ORDER, headers={"idempotency-key": "k" * MAX_WORKFLOW_ID})

    assert response.status_code == 202


async def test_an_order_whose_body_does_not_parse_is_rejected(client: httpx.AsyncClient) -> None:
    response = await client.post("/orders", json={"items": "all of them"}, headers={"idempotency-key": WORKFLOW})

    assert response.status_code == 422
    assert json.loads(response.text) == {"error": "invalid body", "fields": 1}


async def test_confirming_records_the_approval_and_makes_the_workflow_ready(
    client: httpx.AsyncClient,
    payments: Payments,
) -> None:
    await payments.durable.checkpointer.supply(WORKFLOW, "order", ORDER["items"])

    response = await client.post(f"/orders/{WORKFLOW}/confirmation", json={"approved_by": "auditor-7"})

    assert response.status_code == 202
    assert await recorded(payments) == {"order": ORDER["items"], "approved-by": "auditor-7"}
    assert queue(payments) == [WORKFLOW]


async def test_confirming_a_workflow_nobody_submitted_records_nothing(
    client: httpx.AsyncClient,
    payments: Payments,
) -> None:
    # An approval names a payout, so an id with no order behind it has nothing to approve.
    # Writing one anyway would mint a checkpoint for a workflow that does not exist, which
    # the status endpoint then reports as real.
    response = await client.post("/orders/never-heard-of-it/confirmation", json={"approved_by": "auditor-7"})

    assert response.status_code == 404
    assert await payments.durable.checkpointer.load("never-heard-of-it") == {}
    assert queue(payments) == []


async def test_the_status_endpoint_shows_what_the_workflow_has_recorded(
    client: httpx.AsyncClient,
    payments: Payments,
) -> None:
    await payments.durable.checkpointer.supply(WORKFLOW, "order", ORDER["items"])
    await payments.durable.checkpointer.supply(WORKFLOW, "paid", "pay-2000")

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
class BrokenCheckpointer:
    """A store whose writes fail: the app's own bug, not the client's."""

    async def load(self, workflow: str) -> dict[str, object]:  # pragma: no cover - present to satisfy the protocol
        return {}

    async def claim(self, workflow: str, lease: timedelta) -> Pass | None:  # pragma: no cover - same
        return Pass(workflow=workflow, token=1)

    async def record(self, holder: Pass, key: str, value: object) -> Recorded:  # pragma: no cover - same
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
    payments = Payments(durable=SplitDurable(BrokenCheckpointer(), MemoryScheduler()))

    async with serving(payments_app(payments)) as server:
        async with httpx.AsyncClient(base_url=f"http://{server.host}:{server.port}") as client:
            response = await client.post("/orders", json=ORDER, headers={"idempotency-key": WORKFLOW})

    assert response.status_code == 500


@pytest.mark.parametrize("key", [".", ".."], ids=["a dot segment", "a parent segment"])
async def test_an_order_whose_idempotency_key_is_a_dot_segment_is_rejected(
    client: httpx.AsyncClient,
    payments: Payments,
    key: str,
) -> None:
    # The one shape percent-encoding cannot see: `.` and `..` survive it untouched and are
    # then *removed* by every client that resolves a reference, so `/orders/..` resolves
    # to `/` before it is ever sent. A workflow named that captures money against an id
    # whose status URL is somebody else's resource and whose confirmation endpoint cannot
    # be reached at all, which for a payout above the threshold is one that never finishes.
    response = await client.post("/orders", json=ORDER, headers={"idempotency-key": key})

    assert response.status_code == 422
    assert json.loads(response.text)["field"] == "idempotency-key"
    assert workflows(payments) == []


@pytest.mark.parametrize("key", ["order+7", "tenant:7", "order,7", "order(7)", "a$b", "x&y"])
async def test_an_idempotency_key_a_path_segment_carries_is_accepted(client: httpx.AsyncClient, key: str) -> None:
    # The rule must not be stricter than its own sentence. A path segment carries the
    # sub-delimiters along with `:` and `@`, so a base64-shaped key or a scoped one is
    # usable as written and refusing it would cost a client its idempotency key for
    # nothing the routing needed.
    submitted = await client.post("/orders", json=ORDER, headers={"idempotency-key": key})

    assert submitted.status_code == 202
    assert json.loads(submitted.text)["status"] == f"/orders/{key}"

    assert (await client.get(f"/orders/{key}")).status_code == 200


async def test_an_order_with_no_line_items_is_rejected(client: httpx.AsyncClient, payments: Payments) -> None:
    # Not a small payout but a workflow that should never have started: it captures
    # nothing, pays the gateway zero, and records a `paid` for it.
    response = await client.post("/orders", json={"items": {}}, headers={"idempotency-key": WORKFLOW})

    assert response.status_code == 422
    assert await recorded(payments) == {}
