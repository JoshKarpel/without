from __future__ import annotations

import asyncio


async def yield_once() -> None:
    """
    Yield control for exactly one event-loop turn, so a just-scheduled task
    reaches its first suspension point.

    Use this *only* to let a task you just `create_task`-d run far enough to park
    on its first `await`, and *only* when the code it parks in gives you nothing
    to wait on: a library coroutine like `sleep_forever`, or `Sample.updated`
    registering its waiter just before it suspends. Proving such a task *blocks*
    (rather than completing) means giving it one turn and then observing it is
    still pending; there is no signal to await instead, because the suspension
    happens inside code you do not control. Cancelling before that turn would
    preempt the task at its very first step, so the test would pass even for a
    task that never blocks: the one turn is what makes the assertion meaningful.

    It is one deterministic turn, not a timing guess. The scheduled task can only
    park or finish within that turn, so a single `yield_once()` is exact; it never
    needs a second call or a retry loop.

    Avoid it everywhere a real signal exists, which is almost everywhere. If the
    code you are waiting on is yours, have it announce its progress and wait on
    that:

    - waiting for a worker to reach a point: `set` an `asyncio.Event` (or
      `release` a `Semaphore`) there and `await` it;
    - waiting for a sampled `Context` to pick up the next value: `await
      context.updated()`;
    - waiting for a stream or queue to drain: drive it to completion and assert
      on the result.

    Reaching for `yield_once()` to "wait until the background work has probably
    caught up" is the anti-pattern it must not become: yielding once and *hoping*
    is a race, no matter how many times you call it. Use an explicit signal there.
    """
    await asyncio.sleep(0)


async def resolved_next_turn[T](value: T) -> T:
    """
    Suspend for one event-loop turn, then return `value`.

    A deterministic stand-in for awaited contained I/O (a DB read, an RPC) in a
    test that needs a step or a unit of work to *genuinely* suspend and resume.
    The future is resolved on the next turn via `call_soon`, which forces a real
    suspension; awaiting an already-resolved future returns without ever yielding
    control, so the "contained I/O suspends" path would go untested. Like
    `yield_once`, it is one deterministic turn, not a timing guess.
    """
    loop = asyncio.get_running_loop()
    result: asyncio.Future[T] = loop.create_future()
    loop.call_soon(result.set_result, value)
    return await result
