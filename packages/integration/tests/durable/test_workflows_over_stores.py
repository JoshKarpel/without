from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from contextlib import suppress
from datetime import datetime
from datetime import timedelta
from uuid import uuid4

import httpx
import pytest
from gateways import Gateway
from gateways import paying
from gateways import until
from integration.durable import Order
from integration.durable import Payments
from integration.durable import Reached
from integration.durable import fulfilment
from integration.durable import pay_out
from integration.durable import payments_app
from integration.durable import submitting
from integration.durable import unwinding
from stores import durable  # noqa: F401 - the parametrized fixture every test here takes
from without_dag import CompiledGraph
from without_durability import Checkpointer
from without_durability import Durable
from without_durability import Run
from without_durability import Suspended
from without_durability import claimed
from without_durability import now_utc
from without_durability import resume
from without_durability import run_saga
from without_durability import work
from without_http import serving

# The same workflows over every store, which is the claim the `Checkpointer` and `Scheduler`
# seams exist to support and the thing no single-store suite can show. The stores' own
# packages test what each one *is* (its scripts, its statements, its failure modes); this
# tests that a workflow cannot tell which it got.
#
# It is `compose`-marked because two of the four parameters need a server. The memory and
# SQLite parameters would run anywhere, and losing them on a machine without podman is
# the price of keeping the parametrization in one place rather than splitting the suite.

pytestmark = pytest.mark.compose

ORDER = Order(order_id="o-42", sku="gizmo", cents=1999)


@pytest.fixture
def workflow() -> str:
    # Every test gets its own id rather than clearing the store, because the servers are
    # shared by every worker in the session: clearing would pull another test's
    # checkpoint out from under it.
    return f"test-{uuid4().hex}"


async def passing[T](checkpointer: Checkpointer, workflow: str, body: Callable[[Run], Awaitable[T]]) -> T:
    """One claimed pass, released on the way out, which is what the worker does."""
    holder = await claimed(checkpointer, workflow)
    try:
        return await resume(holder, checkpointer, body)
    finally:
        await checkpointer.release(holder)


async def saga[In, Out, Reaches, Undone](
    forward: CompiledGraph[In, Out],
    unwind: CompiledGraph[Reaches, Undone],
    reaches: Callable[[Mapping[str, object]], Reaches],
    checkpointer: Checkpointer,
    workflow: str,
    value: In,
) -> Out:
    holder = await claimed(checkpointer, workflow)
    try:
        return await run_saga(forward, unwind, reaches, checkpointer, holder, value)
    finally:
        await checkpointer.release(holder)


async def test_a_workflow_resumes_from_a_checkpoint_left_by_a_dead_process(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # The whole point of naming steps, end to end: the first "process" leaves a
    # checkpoint behind, and a second one that shares nothing with it but the workflow's
    # id picks the run up from that checkpoint alone.
    checkpointer = durable.checkpointer
    crashed = Gateway(broken={"ship"})
    services = crashed.services()

    with pytest.raises(RuntimeError, match="ship is down"):
        await saga(fulfilment(services), unwinding(services), Reached.of, checkpointer, workflow, ORDER)

    assert await checkpointer.load(workflow) == {"charged": "ch-o-42", "reserved": "rs-gizmo"}

    recovered = Gateway()
    receipt = await saga(
        fulfilment(recovered.services()),
        unwinding(recovered.services()),
        Reached.of,
        checkpointer,
        workflow,
        ORDER,
    )

    assert receipt["tracking"] == "tr-ch-o-42-rs-gizmo"
    assert recovered.calls == ["ship"], "the charge and the reservation were read back out of the store"


async def test_a_compensation_is_recorded_under_its_own_key(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    gateway = Gateway(broken={"ship"})
    services = gateway.services()

    with pytest.raises(RuntimeError, match="ship is down"):
        await saga(fulfilment(services), unwinding(services), Reached.of, durable.checkpointer, workflow, ORDER)

    assert await durable.checkpointer.load(f"{workflow}:unwind") == {
        "refunded": "rf-ch-o-42",
        "released": "rl-rs-gizmo",
        "unwound": {"refunded": "rf-ch-o-42", "released": "rl-rs-gizmo"},
    }
    assert sorted(gateway.calls[-2:]) == ["refund", "release"]


async def test_a_workflow_suspended_on_an_approval_resumes_when_another_process_records_it(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # The thing a signal usually needs a server for: the pass that asked for the approval
    # is gone, and what resumes the workflow is one value written into its checkpoint by
    # something that shares nothing with it.
    checkpointer = durable.checkpointer
    asked: list[str] = []

    async def body(run: Run) -> dict[str, object]:
        return await pay_out(run, "ord-42", paying(asked), settling=timedelta(), approval_over=10_000)

    with pytest.raises(Suspended) as suspension:
        await passing(checkpointer, workflow, body)

    assert suspension.value.key == "approved-by"
    assert set(await checkpointer.load(workflow)) == {"items", "captured:piano", "captured:stool", "settling"}
    assert asked == ["items", "capture:piano", "capture:stool"], "the money moved, the payout did not"

    await checkpointer.supply(workflow, "approved-by", "auditor-7")

    answered: list[str] = []

    async def resumed(run: Run) -> dict[str, object]:
        return await pay_out(run, "ord-42", paying(answered), settling=timedelta(), approval_over=10_000)

    payout = await passing(checkpointer, workflow, resumed)

    assert payout["approved_by"] == "auditor-7"
    assert payout["captures"] == {"piano": "cap-piano", "stool": "cap-stool"}
    assert answered == ["pay"], "everything before the approval was read back out of the store"


# The API and the worker over every store, across a settlement window this really does
# wait out. It is short because nothing here is testing how long a workflow can sleep, and
# it is a value the test chooses rather than the module default, so waiting it out means
# reading the deadline the workflow recorded instead of guessing a duration.
SETTLING = timedelta(milliseconds=200)


@pytest.mark.timeout(60)
async def test_an_order_submitted_to_the_api_is_carried_to_payout_by_the_worker(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    worker = asyncio.create_task(work(durable, submitting(settling=SETTLING)))

    try:
        async with (
            serving(payments_app(Payments(durable=durable))) as server,
            httpx.AsyncClient(base_url=f"http://{server.host}:{server.port}") as client,
        ):
            accepted = await client.post(
                "/orders",
                json={"items": {"piano": 90_000, "stool": 4_000}},
                headers={"idempotency-key": workflow},
            )
            assert accepted.status_code == 202

            # The captures happen at once; the payout then waits out its settlement
            # window, and the worker is the only thing that knows the window exists.
            recorded = await until(client, workflow, lambda state: "settling" in state["recorded"])
            assert set(recorded) == {"order", "items", "captured:piano", "captured:stool", "settling"}

            # Past the deadline the workflow becomes ready again, the pass runs, and it
            # stops on the confirmation, where nothing but a person can move it. Waiting
            # out the deadline the *workflow* recorded rather than a constant chosen here
            # keeps the two from drifting apart, and no margin is needed on top: `done`
            # cannot become true until the confirmation below, so this reads the same
            # whether or not the wakeup has landed yet.
            deadline = datetime.fromisoformat(str(recorded["settling"]))
            await asyncio.sleep(max((deadline - now_utc()).total_seconds(), 0.0))
            held = await client.get(f"/orders/{workflow}")
            assert json.loads(held.text)["done"] is False

            confirmed = await client.post(f"/orders/{workflow}/confirmation", json={"approved_by": "auditor-7"})
            assert confirmed.status_code == 202

            paid = await until(client, workflow, lambda state: state["done"])

        assert paid["approved-by"] == "auditor-7"
        assert paid["paid"] == f"pay-{workflow}-94000"
    finally:
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker
