# The API half of the pair: three endpoints, none of which runs a workflow. Submitting
# an order and confirming a payout are the same two-line move, and that symmetry is the
# design rather than a coincidence:
#
#   supply one value into the workflow's checkpoint, then make the workflow ready.
#
# A submission supplies the order the workflow is waiting on; a confirmation supplies the
# approval it is waiting on. Both are values some *other* process wrote, which is
# exactly what `Run.awaiting` reads, so the API needs no notion of starting a workflow
# versus resuming one, and no channel to a running process. The worker finds out the
# same way either time.
#
# `supply` rather than `record` because these writes come from outside any pass, and
# deliberately do not take the workflow's claim. An approval that failed because a worker
# happened to be mid-pass would be an API that gets slower the busier the system is, for
# a value nothing is racing it to write. What it does keep is first-writer-wins, which is
# what makes a resubmission harmless.
#
# That is what replaces a client library talking to a workflow server: the API writes
# to a hash and a list, and holds nothing. It can be restarted mid-flight, scaled to
# any number of instances, or deployed separately from the worker, because the only
# state it touches is the store both share.
#
# The workflow id is the request's `Idempotency-Key`, so a resubmitted order is not a
# second workflow: the second submission records the same order over the same key and
# makes the same workflow ready, and the pass it triggers finds every step already
# recorded and performs nothing.

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import ValidationError
from without_asgi import ASGIApp
from without_asgi import HttpScope
from without_asgi import Response
from without_asgi import make_asgi_app
from without_web import STR
from without_web import ExtractionError
from without_web import Router
from without_web import body
from without_web import catching
from without_web import get
from without_web import handle
from without_web import header_param
from without_web import http_scope
from without_web import once
from without_web import path_param
from without_web import post

from integration.durable.shell import Checkpoints
from integration.durable.wakeups import Wakeups
from integration.responses import json_response


class SubmittedOrder(BaseModel):
    """An order as it arrives: a sku-to-cents mapping, which is what the workflow reads."""

    model_config = ConfigDict(frozen=True)

    items: dict[str, int]


class Confirmation(BaseModel):
    """A human's answer to a payout waiting on one."""

    model_config = ConfigDict(frozen=True)

    approved_by: str


@dataclass(frozen=True, slots=True)
class Payments:
    """
    Everything the API touches: the checkpoint store and the ready queue.

    Injected as router state, so the endpoints are functions of values and the app is
    assembled once at the entrypoint (`payments_app`), which is also what lets the
    tests drive it against dicts.
    """

    checkpoints: Checkpoints
    wakeups: Wakeups


order_body = body(SubmittedOrder.model_validate_json, schema=SubmittedOrder)
confirmation_body = body(Confirmation.model_validate_json, schema=Confirmation)
# The workflow id *is* the idempotency key, so submitting twice cannot start two
# payouts: the same key names the same checkpoint, and the pass it triggers finds
# the work already recorded.
#
# It is also the one place a client's own text becomes a workflow id, so it is where an
# id contract would be enforced if a deployment wanted one (see `RedisCheckpoints` for
# what the Redis store asks of an id, and `run_saga` for the one constraint that holds
# whatever the store is). This takes the header as given, because the ids that matter
# are UUIDs their senders generated and every constraint on the list is one they meet
# without trying. Parsing here rather than in the store is the point: it would be one
# extractor deciding once, not a check on every call.
idempotency_key = header_param("idempotency-key", once(bytes.decode), schema={"type": "string"}, required=True)
workflow_id = path_param("workflow", STR)


@post("/orders", order_body, idempotency_key, summary="Submit an order for payout")
async def submit_order(payments: Payments, order: SubmittedOrder, workflow: str) -> Response:
    """
    Supply the order as the value the workflow is waiting on, then make it ready.

    The `202` is honest: nothing has been paid, and the only claim made is that the
    order is durable and someone will pick it up. The status URL is where the client
    watches it happen.

    Resubmitting under the same key is not an update. The first order recorded is the
    one the workflow runs, so a client that retries with a changed basket gets the
    original back, which is what an idempotency key promises and the alternative
    (letting the second overwrite) would break for a workflow already spending it.
    """
    await payments.checkpoints.supply(workflow, "order", order.items)
    await payments.wakeups.make_ready(workflow)
    return json_response(202, {"workflow": workflow, "status": f"/orders/{workflow}"})


@post(t"/orders/{workflow_id}/confirmation", workflow_id, confirmation_body, summary="Approve a held payout")
async def confirm_order(payments: Payments, workflow: str, confirmation: Confirmation) -> Response:
    """
    Record the approval the workflow suspended on, then make it ready.

    Identical in shape to submitting, because it is the same act: a value arrives that
    the workflow cannot produce for itself. Nothing here knows whether a workflow is
    actually waiting; recording an approval nobody asked for leaves an unread field.
    """
    await payments.checkpoints.supply(workflow, "approved-by", confirmation.approved_by)
    await payments.wakeups.make_ready(workflow)
    return json_response(202, {"workflow": workflow, "status": f"/orders/{workflow}"})


@get(t"/orders/{workflow_id}", workflow_id, summary="What a workflow has done so far")
async def show_order(payments: Payments, workflow: str) -> Response:
    """
    The checkpoint, rendered as-is: every step this workflow has recorded.

    A progress view for free, because the durable state *is* the state. There is no
    separate status field to keep in sync with it, and `paid` appearing is what "done"
    means.
    """
    recorded = await payments.checkpoints.load(workflow)
    if not recorded:
        return json_response(404, {"error": f"no workflow {workflow}"})
    return json_response(200, {"workflow": workflow, "recorded": recorded, "done": "paid" in recorded})


async def unrouted(payments: Payments, scope: HttpScope) -> Response:
    return json_response(404, {"error": f"no route for {scope.method} {scope.path}"})


async def recover(exc: Exception) -> Response | None:
    match exc:
        case ExtractionError(cause=ValidationError() as invalid):
            return json_response(422, {"error": "invalid body", "fields": invalid.error_count()})
        case ExtractionError():
            return json_response(422, {"error": str(exc), "field": exc.field})
        case _:
            return None


payments_router: Router[Payments] = Router(
    routes=(submit_order, confirm_order, show_order),
    fallback=handle(http_scope(), fn=unrouted),
    middleware=catching(recover),
)


def payments_app(payments: Payments) -> ASGIApp:
    """The ASGI app, holding its two stores for the server's lifetime."""

    @asynccontextmanager
    async def hold() -> AsyncIterator[Payments]:
        yield payments

    return make_asgi_app(hold, http=payments_router.dispatch)
