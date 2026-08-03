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
from without_durability import Completed
from without_durability import Durable
from without_durability import Outcome
from without_durability import Run
from without_durability import Waiting
from without_durability import claimed
from without_durability import now_utc
from without_durability import resume
from without_durability import run_durably
from without_durability import work
from without_http import serving

# The same workflows over every store, which is the claim the `Checkpointer` and `Scheduler`
# interfaces exist to support and the thing no single-store suite can show. The stores' own
# packages test what each one *is* (its scripts, its statements, its failure modes); this
# tests that a workflow cannot tell which it got.
#
# It is `compose`-marked because two of the four parameters need a server. The memory and
# SQLite parameters would run anywhere, and losing them on a machine without podman is
# the price of keeping the parametrization in one place rather than splitting the suite.

pytestmark = pytest.mark.compose


class Unwound(Exception):
    """A workflow that was compensated, and so must not be run again under its own id."""


ORDER = Order(order_id="o-42", sku="gizmo", cents=1999)


@pytest.fixture
def workflow() -> str:
    # Every test gets its own id rather than clearing the store, because the servers are
    # shared by every worker in the session: clearing would pull another test's
    # checkpoint out from under it.
    return f"test-{uuid4().hex}"


async def passing[T](checkpointer: Checkpointer, workflow: str, body: Callable[[Run], Awaitable[T]]) -> Outcome[T]:
    """One claimed pass, released on the way out, which is what the worker does."""
    holder = await claimed(checkpointer, workflow)
    try:
        return await resume(holder, checkpointer, body)
    finally:
        await checkpointer.release(holder)


def finished[T](outcome: Outcome[T]) -> T:
    """What a pass returned, failing here rather than downstream if it stopped short."""
    assert isinstance(outcome, Completed), f"the pass did not finish: {outcome}"
    return outcome.value


async def saga[In, Out, Reaches, Undone](
    forward: CompiledGraph[In, Out],
    unwind: CompiledGraph[Reaches, Undone],
    reaches: Callable[[Mapping[str, object]], Reaches],
    checkpointer: Checkpointer,
    workflow: str,
    value: In,
) -> Out:
    """
    A saga as a second `run_durably` under a second id, which is all one is.

    Spelled out here rather than imported, and run against every store, so the claim
    that a compensation needs no mechanism is tested rather than asserted: what makes
    a rollback resumable is the same checkpoint and the same claim the forward run uses.
    A compensated workflow is *finished*, and saying so is the driver's job because the
    id namespace is the driver's. Nothing in the forward checkpoint records that its
    steps were given back, so a client retrying the same idempotency key would resume it,
    find every node recorded, and ship against a charge that has been refunded and stock
    that has been released. The rollback records `unwound` under the forward id, and this
    refuses to run a workflow carrying it: one key, written where the run that must not
    happen again would look.
    """
    if "unwound" in await checkpointer.load(workflow):
        raise Unwound(f"{workflow!r} was compensated, so its steps have been given back")
    holder = await claimed(checkpointer, workflow)
    try:
        return await run_durably(forward, checkpointer, holder, value)
    except Exception:
        undoing = await claimed(checkpointer, f"{workflow}:unwind")
        try:
            await run_durably(unwind, checkpointer, undoing, reaches(await checkpointer.load(workflow)))
            await checkpointer.supply(workflow, "unwound", True)
        finally:
            await checkpointer.release(undoing)
        raise
    finally:
        await checkpointer.release(holder)


async def test_a_workflow_resumes_from_a_checkpoint_left_by_a_dead_process(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # The whole point of naming steps, end to end: the first "process" leaves a
    # checkpoint behind, and a second one that shares nothing with it but the workflow's
    # id picks the run up from that checkpoint alone.
    #
    # The first process *dies* rather than failing, which is the distinction the saga
    # driver draws and the reason this one runs the graph directly: a run that failed and
    # was compensated has had its steps given back and must not be resumed, where a run
    # whose process disappeared has a checkpoint and nothing else, and is exactly what
    # resumption is for.
    checkpointer = durable.checkpointer
    crashed = Gateway(broken={"ship"})
    holder = await claimed(checkpointer, workflow)

    with pytest.raises(RuntimeError, match="ship is down"):
        await run_durably(fulfilment(crashed.services()), checkpointer, holder, ORDER)

    await checkpointer.release(holder)

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

    suspension = await passing(checkpointer, workflow, body)

    assert suspension == Waiting(key="approved-by")
    assert set(await checkpointer.load(workflow)) == {
        "items",
        "captured:piano",
        "captured:stool",
        "settling",
        "held-for-approval",
    }
    assert asked == ["items", "capture:piano", "capture:stool"], "the money moved, the payout did not"

    await checkpointer.supply(workflow, "approved-by", "auditor-7")

    answered: list[str] = []

    async def resumed(run: Run) -> dict[str, object]:
        return await pay_out(run, "ord-42", paying(answered), settling=timedelta(), approval_over=10_000)

    payout = finished(await passing(checkpointer, workflow, resumed))

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


async def test_a_compensated_workflow_is_not_resumed_under_its_own_id(
    durable: Durable,  # noqa: F811
    workflow: str,
) -> None:
    # The other half of resumption, over every store: a run that failed and was
    # compensated has had its steps given back, and every node is still recorded with the
    # id it recorded. Resuming that would ship against a refunded charge and released
    # stock, which is the recovery path the design rests on turned into the worst outcome
    # available. The rollback writes one key under the forward id, and the driver reads it.
    checkpointer = durable.checkpointer
    gateway = Gateway(broken={"ship"})
    services = gateway.services()

    with pytest.raises(RuntimeError, match="ship is down"):
        await saga(fulfilment(services), unwinding(services), Reached.of, checkpointer, workflow, ORDER)

    assert "refund" in gateway.calls
    gateway.broken.clear()
    gateway.calls.clear()

    with pytest.raises(Unwound, match="was compensated"):
        await saga(fulfilment(services), unwinding(services), Reached.of, checkpointer, workflow, ORDER)

    assert gateway.calls == [], "and the retry performed nothing rather than shipping"
