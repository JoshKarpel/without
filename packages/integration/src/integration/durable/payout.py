# A payout workflow written as ordinary code, chosen because every one of its shapes
# is one the fulfilment *graph* in `core` cannot express:
#   - the number of captures comes from a step's result, not from the workflow's
#     input, so the fan-out is data-dependent at *run* time rather than build time;
#   - a settlement window is waited out across crashes, which needs a step that can
#     stop mid-workflow rather than one that runs to completion;
#   - a large payout waits on a person, which needs a value the workflow never
#     produces and another process supplies.
# Everything the graph version does better is still true: it validates the whole
# checkpoint before running a thing, and its structure is a value you can diagram.
#
# Effects arrive as injected callables and every step result is JSON-native, the two
# constraints `stepwise` carries: the code between steps re-runs on every pass, and a
# step's result round-trips through the store's codec.

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta

from without_durability import Run

type Cents = dict[str, int]
type Payout = dict[str, object]


@dataclass(frozen=True, slots=True)
class Payouts:
    """
    The effectful edges of the workflow, injected rather than reached for.

    `items` is what makes the workflow's shape a runtime question: how many captures
    there are is not known until it answers, and it can answer differently for
    different orders.
    """

    items: Callable[[str], Awaitable[Cents]]
    capture: Callable[[str, int], Awaitable[str]]
    pay: Callable[[str, int], Awaitable[str]]


def parse_items(recorded: object) -> Cents:
    """
    The line items a step recorded, parsed back into the shape the rest of this asks for.

    A step's result comes back through the store's codec, so this is the boundary
    where a mapping-of-strings-to-anything becomes a mapping of sku to cents, once,
    rather than every reader re-checking.
    """
    if not isinstance(recorded, dict):
        raise TypeError(f"the line items were recorded as {recorded!r}, which is not a mapping")
    return {str(sku): as_cents(sku, amount) for sku, amount in recorded.items()}


def as_cents(sku: object, amount: object) -> int:
    if not isinstance(amount, int):
        raise TypeError(f"the amount recorded for {sku!r} is {amount!r}, which is not a whole number of cents")
    return amount


def parse_approver(recorded: object) -> str:
    """Who approved the payout, as recorded by whichever process took the approval."""
    if not isinstance(recorded, str):
        raise TypeError(f"the approval holds {recorded!r}, which does not name an approver")
    return recorded


async def pay_out(
    run: Run,
    order_id: str,
    services: Payouts,
    *,
    settling: timedelta,
    approval_over: int,
) -> Payout:
    """
    Capture every line item, wait out the settlement window, then pay, with a person
    in the loop above `approval_over`.

    Written as the straight line it is. The durability is in the four `run` calls and
    nowhere else: each step's name is what its result is filed under, so the second
    pass after a crash re-reaches this same line and picks the result back up. The
    capture keys carry their sku, which is what lets a fan-out of unknown width resume
    item by item rather than all-or-nothing.
    """
    items = parse_items(await run.step("items", lambda: services.items(order_id)))
    skus = sorted(items)

    captures = await asyncio.gather(
        *(run.step(f"captured:{sku}", capturing(services, sku, items[sku])) for sku in skus)
    )

    total = sum(items.values())
    await run.sleep("settling", settling)
    approved_by = parse_approver(await run.awaiting("approved-by")) if total > approval_over else None

    reference = await run.step("paid", lambda: services.pay(order_id, total))

    return {
        "order_id": order_id,
        "total": total,
        "reference": reference,
        "approved_by": approved_by,
        "captures": dict(zip(skus, captures, strict=True)),
    }


def capturing(services: Payouts, sku: str, amount: int) -> Callable[[], Awaitable[str]]:
    """One capture as a thunk, so `run.step` decides whether it happens at all."""

    async def capture() -> str:
        return await services.capture(sku, amount)

    return capture
