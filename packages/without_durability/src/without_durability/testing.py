from __future__ import annotations

from without_durability.interfaces import Checkpointer
from without_durability.interfaces import Pass
from without_durability.stepwise import Run
from without_durability.stepwise import StepKey
from without_durability.stepwise import extending


def passing[Effect](
    holder: Pass,
    checkpointer: Checkpointer[Effect],
    recorded: dict[StepKey, object] | None = None,
) -> Run[Effect]:
    """
    A `Run` wired as `resume` wires one, for a test driving a single method rather than a body.

    The wiring is here rather than at each call site so that a test says what it is about
    (this holder, this checkpointer, these records) instead of restating how a pass is
    assembled, and so that a store's own suite and this package's say it the same way.
    Anything testing the assembly itself builds its own.
    """
    return Run(
        holder=holder,
        checkpointer=checkpointer,
        recorded=recorded if recorded is not None else {},
        extend=extending(checkpointer),
    )
