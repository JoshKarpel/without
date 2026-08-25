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
from without_streams import cancel_futures

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
    if not recorded:
        raise ValueError("the line items are empty, and an order with nothing in it has no payout to make")
    return {str(sku): as_cents(sku, amount) for sku, amount in recorded.items()}


def as_cents(sku: object, amount: object) -> int:
    """
    One line item's amount, which is a positive whole number of cents or nothing at all.

    Positive is the load-bearing half, and it is checked here because here is where the
    workflow reads it. The approval gate is a comparison against the *sum*, so an amount
    allowed to be negative makes the sum a net rather than a total, and a basket that
    captures 90,000 cents against a 5,000-cent line of credit passes a gate that only ever
    saw 5,000 while the gateway moves the full amount. A `bool` is refused for the same
    reason it is a surprise: it is an `int` in Python and would be captured as one.
    """
    if not isinstance(amount, int) or isinstance(amount, bool):
        raise TypeError(f"the amount recorded for {sku!r} is {amount!r}, which is not a whole number of cents")
    if amount <= 0:
        raise ValueError(f"the amount recorded for {sku!r} is {amount}, and a line item must be worth something")
    return amount


async def over(total: int, approval_over: int) -> bool:
    """Whether this payout needs a person, decided once and recorded like any other step."""
    return total > approval_over


def parse_held(recorded: object) -> bool:
    """Whether a pass already decided this payout waits for a person."""
    if not isinstance(recorded, bool):
        raise TypeError(f"the approval gate holds {recorded!r}, which does not say whether a person is needed")
    return recorded


def parse_approver(recorded: object) -> str:
    """Who approved the payout, as recorded by whichever process took the approval."""
    if not isinstance(recorded, str):
        raise TypeError(f"the approval holds {recorded!r}, which does not name an approver")
    return recorded


def parse_reference(recorded: object) -> str:
    """A gateway's reference for a capture or a payment, read back out of the store."""
    if not isinstance(recorded, str):
        raise TypeError(f"a gateway reference was recorded as {recorded!r}, which is not one")
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
    nowhere else: each step's name is what its result is filed under, so the second pass
    after a crash re-reaches this same line and picks the result back up. The capture
    keys carry their sku, which is what lets a fan-out of unknown width resume item by
    item rather than all-or-nothing.

    Each step names its parser alongside its key, so a step whose result is used without
    being parsed is not expressible.
    """
    items = await run.step("items", lambda: services.items(order_id), parse_items)
    skus = sorted(items)

    # Spawned into a list rather than gathered straight off a generator, so the fan-out
    # has an owner: `gather` raises the first failure and leaves its siblings running,
    # which here would mean captures still writing into a pass that is already over,
    # landing after `resume` reported an outcome and after the worker released the claim.
    #
    # Cancelling is safe to do here precisely because it is not `run.step`'s whole story:
    # a sibling that is *inside* its gateway call has charged nothing and should end, and
    # one that has charged and is writing the record keeps writing it, because `step`
    # holds that write past the cancellation. Without that, one declined line item would
    # re-charge every line item that happened to be mid-record beside it.
    #
    # A `TaskGroup` would cancel them too, and would change what a failure *is*: it wraps
    # children in an `ExceptionGroup`, so a `Fenced` raised by one capture would reach the
    # worker as a group that its `except (Fenced, Contended)` cannot see, and losing the
    # workflow would be logged as the workflow failing. The exception's *type* is
    # load-bearing here, so the draining is done by hand and the type is left alone.
    capturing_each = [
        asyncio.ensure_future(run.step(f"captured:{sku}", capturing(services, sku, items[sku]), parse_reference))
        for sku in skus
    ]
    try:
        captures: list[str] = await asyncio.gather(*capturing_each)
    finally:
        # The ones still running, rather than all of them: `cancel_futures` cancels the
        # set and then awaits each so its teardown finishes, and awaiting one that has
        # already *failed* re-raises there, before the awaits behind it. The failure is
        # the ordinary case here (it is what brought us into this `finally`), and it is
        # already in flight to the caller, so re-raising it costs nothing and skipping
        # the rest of the list costs the cancellation this whole shape exists for.
        await cancel_futures(capture for capture in capturing_each if not capture.done())

    total = sum(items.values())
    await run.sleep("settling", settling)
    # The gate is *recorded*, not recomputed, and it is the one decision here that has to
    # be. `approval_over` is a deployment's setting rather than the workflow's own value,
    # so a pass after a deploy that raised it would find a held payout no longer held and
    # pay it with nobody in the loop, leaving a checkpoint that shows no approval and no
    # trace that one was ever wanted. It is the same argument `sleep` makes by recording
    # its deadline instead of its duration: what a later pass must agree with is the
    # decision, not the input it was made from.
    held = await run.step("held-for-approval", lambda: over(total, approval_over), parse_held)
    approved_by = await run.awaiting("approved-by", parse_approver) if held else None

    reference = await run.step("paid", lambda: services.pay(order_id, total), parse_reference)

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
