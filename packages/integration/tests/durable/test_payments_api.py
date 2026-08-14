from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Never

import pytest
from gateways import BASE
from gateways import get_json as get
from gateways import post_json as post
from integration.durable.api import MAX_WORKFLOW_ID
from integration.durable.api import Payments
from integration.durable.api import payments_app
from without_durability import MemoryCheckpointer
from without_durability import MemoryScheduler
from without_durability import Pass
from without_durability import Recorded
from without_durability import SplitDurable
from without_http import Client
from without_http import request
from without_http.testing import loopback_client

# The API driven over the real HTTP wire but no socket (`loopback_client`), against
# in-memory stores: what these prove is the *endpoints*, so neither a bound port nor a
# container adds anything (the compose-marked tests cover the real stores). The wire is
# still in the path because one case turns on the server's own isolation, where a store
# failure the app does not recover from becomes a `500`.

WORKFLOW = "idem-key-9f2"
ORDER = {"items": {"widget": 1200, "gizmo": 800}}


@pytest.fixture
def payments() -> Payments:
    return Payments(durable=SplitDurable(MemoryCheckpointer(), MemoryScheduler()))


@pytest.fixture
async def client(payments: Payments) -> AsyncIterator[Client]:
    async with loopback_client(payments_app(payments)) as client:
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
    client: Client,
    payments: Payments,
) -> None:
    status, answer = await post(client, "/orders", ORDER, key=WORKFLOW)

    assert status == 202
    assert answer == {"workflow": WORKFLOW, "status": f"/orders/{WORKFLOW}"}
    assert await recorded(payments) == {"order": ORDER["items"]}
    assert queue(payments) == [WORKFLOW], "the API runs nothing; it makes the workflow runnable"


async def test_submitting_the_same_key_twice_addresses_the_same_workflow(
    client: Client,
    payments: Payments,
) -> None:
    await post(client, "/orders", ORDER, key=WORKFLOW)
    await post(client, "/orders", ORDER, key=WORKFLOW)

    assert workflows(payments) == [WORKFLOW], "the idempotency key *is* the workflow id"
    assert queue(payments) == [WORKFLOW, WORKFLOW], "a second pass is harmless: it finds the work recorded"


async def test_resubmitting_a_changed_basket_under_one_key_does_not_replace_the_order(
    client: Client,
    payments: Payments,
) -> None:
    # What an idempotency key promises, and what a plain overwrite would break: a
    # workflow that has already captured against the first basket must not find a
    # different one underneath it on the next pass.
    await post(client, "/orders", ORDER, key=WORKFLOW)
    status, _second = await post(client, "/orders", {"items": {"piano": 90_000}}, key=WORKFLOW)

    assert status == 202
    assert await recorded(payments) == {"order": ORDER["items"]}, "the first order recorded is the one that runs"


async def test_an_order_without_an_idempotency_key_is_rejected(client: Client) -> None:
    status, answer = await post(client, "/orders", ORDER)

    assert status == 422
    assert answer["field"] == "idempotency-key"


async def test_an_order_whose_idempotency_key_is_empty_is_rejected(
    client: Client,
    payments: Payments,
) -> None:
    # A header that is present and blank, which is not the same as absent: it reaches the
    # parser as a value, and it would name a workflow every empty key shared.
    status, answer = await post(client, "/orders", ORDER, key="")

    assert status == 422
    assert answer["field"] == "idempotency-key"
    assert workflows(payments) == [], "no workflow was opened under the empty id"


async def test_an_order_whose_idempotency_key_is_too_long_is_rejected(
    client: Client,
    payments: Payments,
) -> None:
    # The one thing a well-behaved sender's UUID does not establish: an id becomes key
    # structure in the store, so an unbounded one is unbounded storage the client picks.
    status, answer = await post(client, "/orders", ORDER, key="k" * (MAX_WORKFLOW_ID + 1))

    assert status == 422
    assert answer["field"] == "idempotency-key"
    assert await recorded(payments) == {}, "nothing reached the store under the id it refused"


@pytest.mark.parametrize("key", ["{tenant}-9f2", "idem-{", "idem-}"])
async def test_an_order_whose_idempotency_key_carries_a_hash_tag_is_rejected(
    client: Client,
    payments: Payments,
    key: str,
) -> None:
    # Braces delimit Redis Cluster's hash tag, so an id carrying its own decides which
    # slot its workflow lands on. Correctness survives that (a workflow's two keys still
    # agree on the tag), but a sender that puts the same tag on every request puts the
    # whole deployment on one node, which is not a choice the sender gets to make.
    status, answer = await post(client, "/orders", ORDER, key=key)

    assert status == 422
    assert answer["field"] == "idempotency-key"
    assert workflows(payments) == [], "no workflow was opened under an id the store would have to parse"


@pytest.mark.parametrize("key", ["tenant-a/order-7", "order?7", "order#7", "order%2F7", "order 7"])
async def test_an_order_whose_idempotency_key_would_not_survive_a_url_is_rejected(
    client: Client,
    payments: Payments,
    key: str,
) -> None:
    # This app puts the id in a path: it answers with `/orders/{workflow}` and takes the
    # approval on `/orders/{workflow}/confirmation`. A key holding a path delimiter would
    # otherwise open a perfectly good workflow whose status URL resolves to nothing and
    # whose payout can never be approved, which for a payout above the threshold means one
    # that can never finish. Percent-escaping does not recover it either, since the header
    # is stored as sent and the path arrives decoded.
    status, answer = await post(client, "/orders", ORDER, key=key)

    assert status == 422
    assert answer["field"] == "idempotency-key"
    assert workflows(payments) == [], "no workflow was opened under an id this app cannot address"


@pytest.mark.parametrize("items", [{"piano": -8500}, {"piano": 0}, {"piano": True}], ids=["negative", "zero", "bool"])
async def test_an_order_whose_line_items_are_not_positive_amounts_is_rejected(
    client: Client,
    payments: Payments,
    items: dict[str, object],
) -> None:
    # The approval gate compares a *sum*, so a negative line item makes it a net rather
    # than a total: a basket of 90,000 and -85,000 captures 90,000 through a gate that
    # only ever saw 5,000. A `bool` is an `int` in Python and pydantic's lax mode coerces
    # it, so `true` would be captured as one cent.
    status, _answer = await post(client, "/orders", {"items": items}, key=WORKFLOW)

    assert status == 422
    assert await recorded(payments) == {}, "nothing reached the store under an order it refused"


async def test_an_order_whose_idempotency_key_is_as_long_as_the_bound_allows_is_accepted(
    client: Client,
) -> None:
    # The bound itself, so a change to it cannot silently become off-by-one.
    status, _answer = await post(client, "/orders", ORDER, key="k" * MAX_WORKFLOW_ID)

    assert status == 202


async def test_an_order_whose_body_does_not_parse_is_rejected(client: Client) -> None:
    status, answer = await post(client, "/orders", {"items": "all of them"}, key=WORKFLOW)

    assert status == 422
    assert answer == {"error": "invalid body", "fields": 1}


async def test_confirming_records_the_approval_and_makes_the_workflow_ready(
    client: Client,
    payments: Payments,
) -> None:
    await payments.durable.checkpointer.supply(WORKFLOW, "order", ORDER["items"])

    status, _answer = await post(client, f"/orders/{WORKFLOW}/confirmation", {"approved_by": "auditor-7"})

    assert status == 202
    assert await recorded(payments) == {"order": ORDER["items"], "approved-by": "auditor-7"}
    assert queue(payments) == [WORKFLOW]


async def test_confirming_a_workflow_nobody_submitted_records_nothing(
    client: Client,
    payments: Payments,
) -> None:
    # An approval names a payout, so an id with no order behind it has nothing to approve.
    # Writing one anyway would mint a checkpoint for a workflow that does not exist, which
    # the status endpoint then reports as real.
    status, _answer = await post(client, "/orders/never-heard-of-it/confirmation", {"approved_by": "auditor-7"})

    assert status == 404
    assert await payments.durable.checkpointer.load("never-heard-of-it") == {}
    assert queue(payments) == []


async def test_the_status_endpoint_shows_what_the_workflow_has_recorded(
    client: Client,
    payments: Payments,
) -> None:
    await payments.durable.checkpointer.supply(WORKFLOW, "order", ORDER["items"])
    await payments.durable.checkpointer.supply(WORKFLOW, "paid", "pay-2000")

    status, answer = await get(client, f"/orders/{WORKFLOW}")

    assert status == 200
    assert answer == {
        "workflow": WORKFLOW,
        "recorded": {"order": ORDER["items"], "paid": "pay-2000"},
        "done": True,
    }


async def test_a_workflow_nobody_has_submitted_is_a_404(client: Client) -> None:
    status, answer = await get(client, "/orders/never-heard-of-it")

    assert status == 404
    assert answer == {"error": "no workflow never-heard-of-it"}


async def test_an_unrouted_path_is_a_404(client: Client) -> None:
    status, answer = await get(client, "/nope")

    assert status == 404
    assert answer == {"error": "no route for GET /nope"}


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

    # Read past the helpers here: the body is the server's own plain-text error, not this
    # app's JSON, which is the point. The app never answered at all.
    async with loopback_client(payments_app(payments)) as client:
        headers = ((b"content-type", b"application/json"), (b"idempotency-key", WORKFLOW.encode()))
        async with request(client, "POST", f"{BASE}/orders", headers=headers, body=json.dumps(ORDER).encode()) as (
            head,
            _body,
        ):
            assert head.status == 500


@pytest.mark.parametrize("key", [".", ".."], ids=["a dot segment", "a parent segment"])
async def test_an_order_whose_idempotency_key_is_a_dot_segment_is_rejected(
    client: Client,
    payments: Payments,
    key: str,
) -> None:
    # The one shape percent-encoding cannot see: `.` and `..` survive it untouched and are
    # then *removed* by every client that resolves a reference, so `/orders/..` resolves
    # to `/` before it is ever sent. A workflow named that captures money against an id
    # whose status URL is somebody else's resource and whose confirmation endpoint cannot
    # be reached at all, which for a payout above the threshold is one that never finishes.
    status, answer = await post(client, "/orders", ORDER, key=key)

    assert status == 422
    assert answer["field"] == "idempotency-key"
    assert workflows(payments) == []


@pytest.mark.parametrize("key", ["order+7", "tenant:7", "order,7", "order(7)", "a$b", "x&y"])
async def test_an_idempotency_key_a_path_segment_carries_is_accepted(client: Client, key: str) -> None:
    # The rule must not be stricter than its own sentence. A path segment carries the
    # sub-delimiters along with `:` and `@`, so a base64-shaped key or a scoped one is
    # usable as written and refusing it would cost a client its idempotency key for
    # nothing the routing needed.
    status, answer = await post(client, "/orders", ORDER, key=key)

    assert status == 202
    assert answer["status"] == f"/orders/{key}"

    assert (await get(client, f"/orders/{key}"))[0] == 200


async def test_an_order_with_no_line_items_is_rejected(client: Client, payments: Payments) -> None:
    # Not a small payout but a workflow that should never have started: it captures
    # nothing, pays the gateway zero, and records a `paid` for it.
    status, _answer = await post(client, "/orders", {"items": {}}, key=WORKFLOW)

    assert status == 422
    assert await recorded(payments) == {}
