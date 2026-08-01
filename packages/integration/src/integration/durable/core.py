# The pure half of a durable workflow: what the work *is*, with no idea that it can
# be interrupted. Fulfilling an order charges the card and reserves the stock (two
# steps that need nothing from each other, so they run at once), hands both to the
# carrier, and renders a receipt: a diamond, which is the shape `without-dag`
# exists for. Beside it sits the *unwinding*, the saga's compensation: the graph
# that gives back whatever the forward run managed to take.
#
# Two constraints durability puts on this half, and neither is a framework concern:
#   - Every node is *named* by the caller, so the key a result is stored under is a
#     name in this source rather than an identity minted at build time. Rename a
#     node and its old result no longer applies to it, which is the honest answer:
#     a `checkpoint` written by an older version of this function is rejected by
#     `CompiledGraph`, not silently half-applied.
#   - Every node's result is JSON-native, because a checkpointed result round-trips
#     through the store. The codec is the *app's* boundary decision (see `store`),
#     so a richer one would let these steps return domain values instead; the toy
#     stays with strings and mappings to keep the boundary in one obvious place.
#
# The effects are injected as `Services` rather than reached for, so the same graphs
# run against a real gateway or a test double with no patching, and so this module
# never imports a client.

from __future__ import annotations

from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass

from pydantic import BaseModel
from pydantic import ConfigDict
from without_dag import CompiledGraph
from without_dag import Graph
from without_dag import NodeKey

type Receipt = dict[str, object]
type Rollback = dict[str, object]


class Order(BaseModel):
    """
    The workflow's input, and the one value that is *not* checkpointed.

    An input is fed positionally on every call, so it is recovered from wherever
    the request itself lives (the queue message, the request row) rather than from
    the checkpoint. That split is why `Graph.of` names its entries by position
    while a node takes a name: only node results have to survive a crash.
    """

    model_config = ConfigDict(frozen=True)

    order_id: str
    sku: str
    cents: int


@dataclass(frozen=True, slots=True)
class Services:
    """
    The effectful edges of the workflow, as plain injected callables.

    Each `charge`/`reserve`/`ship` is an *at-most-once* effect in the real world:
    charging twice is a duplicate charge, shipping twice is a second parcel. The
    checkpoint is what keeps them at exactly-once across a retry, since a step
    whose result was recorded is never re-entered. `refund` and `release` are their
    compensations, the undo half a saga needs; shipping deliberately has none,
    because a parcel already in the air cannot be recalled, which is exactly why it
    is the last effect the workflow performs.
    """

    charge: Callable[[Order], Awaitable[str]]
    reserve: Callable[[Order], Awaitable[str]]
    ship: Callable[[str, str], Awaitable[str]]
    refund: Callable[[str], Awaitable[str]]
    release: Callable[[str], Awaitable[str]]


@dataclass(frozen=True, slots=True)
class Reached:
    """
    How far a forward run got, parsed out of its checkpoint.

    The saga's whole input. Deciding what to undo is a *pure function of a value*
    (the recorded results), not a replay of a history or an inspection of a running
    engine: the checkpoint already says which steps happened and what each returned,
    so `of` parses that raw mapping into the typed question the unwinding asks, and
    nothing downstream re-checks the store.
    """

    charge_id: str | None
    reservation_id: str | None

    @classmethod
    def of(cls, checkpoint: Mapping[NodeKey, object]) -> Reached:
        return cls(charge_id=recorded_id(checkpoint, "charged"), reservation_id=recorded_id(checkpoint, "reserved"))


def recorded_id(checkpoint: Mapping[NodeKey, object], key: NodeKey) -> str | None:
    """
    The id a step recorded, or `None` if that step never completed.

    Absence is the ordinary answer (the run stopped before that step), so it is
    typed rather than raised. A value of the wrong shape is not: it means the store
    holds something this workflow did not write, which no undo should act on.
    """
    match checkpoint.get(key):
        case None:
            return None
        case str() as recorded:
            return recorded
        case other:
            raise TypeError(f"{key!r} was recorded as {other!r}, which is not an id this workflow wrote")


async def render(order: Order, charge_id: str, tracking: str) -> Receipt:
    """Fold the effectful steps' results into the workflow's output value."""
    return {"order_id": order.order_id, "charge_id": charge_id, "tracking": tracking, "cents": order.cents}


def fulfilment(services: Services) -> CompiledGraph[Order, Receipt]:
    """
    Wire the fulfilment steps into a compiled graph over one `Order`.

    Compiled once at startup and called per order: the scheduling structure is
    input-independent, so the per-order cost is running the steps. `charged` and
    `reserved` share only the order, so they run concurrently and `shipped` joins
    them.
    """
    graph, (order,) = Graph.of(Order)
    charged = graph.node("charged", services.charge, order)
    reserved = graph.node("reserved", services.reserve, order)
    shipped = graph.node("shipped", services.ship, charged, reserved)
    receipt = graph.node("receipt", render, order, charged, shipped)
    return graph.build(output=receipt, limit=4)


def unwinding(services: Services) -> CompiledGraph[Reached, Rollback]:
    """
    Wire the compensations into a graph over how far the forward run got.

    A saga's rollback is just another workflow, so it is another graph: run it
    through the same durable runner and a crash *during* the rollback resumes it
    rather than double-refunding. Each step decides for itself whether it has
    anything to undo, because that is a question about one effect and belongs with
    it; the two undos are independent, so they run concurrently, and neither
    depends on the forward graph's shape beyond the ids it recorded.
    """

    async def refund(reached: Reached) -> str | None:
        if reached.charge_id is None:
            return None  # the charge never landed, so there is nothing to give back
        return await services.refund(reached.charge_id)

    async def release(reached: Reached) -> str | None:
        if reached.reservation_id is None:
            return None
        return await services.release(reached.reservation_id)

    async def unwound(refunded: str | None, released: str | None) -> Rollback:
        return {"refunded": refunded, "released": released}

    graph, (reached,) = Graph.of(Reached)
    refunded = graph.node("refunded", refund, reached)
    released = graph.node("released", release, reached)
    rollback = graph.node("unwound", unwound, refunded, released)
    return graph.build(output=rollback, limit=2)
