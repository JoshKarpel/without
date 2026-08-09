# The workflow this deployment runs, and the effects it runs against. It is the piece
# `without-durability` deliberately does not have: the library supplies the interfaces, the
# runners, and the worker loop, and what a pass actually *does* is the application's.
#
# `work(durable, submitted)` is the whole of the wiring, which is the shape the split is
# meant to make obvious: the body is an argument, not a plugin point.

from __future__ import annotations

from collections.abc import Awaitable
from collections.abc import Callable
from datetime import timedelta

from without_durability import Run

from integration.durable.payout import Payout
from integration.durable.payout import Payouts
from integration.durable.payout import parse_items
from integration.durable.payout import pay_out

SETTLING = timedelta(seconds=1)
APPROVAL_OVER = 10_000


def submitting(
    *,
    settling: timedelta = SETTLING,
    approval_over: int = APPROVAL_OVER,
) -> Callable[[Run], Awaitable[Payout]]:
    """
    A payout over whatever the API submitted.

    The order arrives as a *recorded value* rather than an argument, because the worker
    is handed a workflow id and nothing else. `awaiting` is the same call a human
    confirmation uses: an order that has not been submitted is simply one whose first
    value has not landed yet, so the worker needs no separate notion of "this workflow
    has not started".

    The two knobs are arguments rather than constants read inside so a test can run the
    same body against a settlement window it does not have to wait out.
    """

    async def body(run: Run) -> Payout:
        items = await run.awaiting("order", parse_items)
        return await pay_out(run, run.workflow, in_memory(items), settling=settling, approval_over=approval_over)

    return body


submitted = submitting()


def in_memory(items: dict[str, int]) -> Payouts:
    """
    The payout's effects, standing in for a gateway and a warehouse.

    `items` hands back what the request carried; in a real system it reads the order
    table, which is why `pay_out` keeps it a step at all rather than an argument: the
    read can fail, and its result is worth recording.
    """

    async def line_items(order_id: str) -> dict[str, int]:
        return items

    async def capture(sku: str, amount: int) -> str:
        return f"cap-{sku}-{amount}"

    async def pay(order_id: str, total: int) -> str:
        return f"pay-{order_id}-{total}"

    return Payouts(items=line_items, capture=capture, pay=pay)
