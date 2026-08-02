# without-durability

Durable workflows built on the observation that once a workflow's state is a
**checkpoint any process can read**, most of a workflow engine stops being
necessary. A workflow is an ordinary async function whose effects are named;
resuming means calling it again; each step it reaches hands back what is already
recorded instead of running.

```python
from without_durability import MemoryCheckpointer, Run, claimed, resume

def as_text(recorded: object) -> str:
    if not isinstance(recorded, str):
        raise TypeError(f"{recorded!r} is not the reference this step recorded")
    return recorded

async def fulfil(run: Run) -> str:
    charge = await run.step("charged", lambda: gateway.charge(order), as_text)
    await run.sleep("settling", timedelta(days=3))          # survives a crash on day two
    approver = await run.awaiting("approved-by", as_text)    # another process writes this
    return await run.step("paid", lambda: gateway.pay(charge, approver), as_text)

checkpointer = MemoryCheckpointer()
await resume(await claimed(checkpointer, "order-42"), checkpointer, fulfil)
```

Each read names a parser because a step hands back what the *store* holds, not
what its effect returned: the value has been through a codec, so a step returning
a tuple is handed a list on the pass that ran it. A parser makes the return type
something a function proved rather than something a `cast` asserted.

There is no server here and no engine. What there is instead is a seam, and the
seam is where the interesting part lives.

## What the store has to guarantee

A protocol of `load` and `record` is too weak to run a workflow safely at any
scale, because it cannot say "only if nobody else is running this" or "only if I
am still the one who may write". Two passes then both find a step unrecorded and
both perform its effect, and no amount of care in the runner fixes it.

So `Checkpointer` states the requirements and an implementation says how it meets
them: `claim` grants at most one live pass and issues strictly increasing fencing
tokens, `record` refuses a write from a superseded pass and never overwrites a
recorded step. That is the same problem Temporal answers with a server and DBOS
answers by requiring Postgres; here it is stated as a contract, so a deployment
brings whatever store it already runs.

`Scheduler` is the other half of a workflow's state, its right to run, and `Durable`
is the pair plus the transitions that have to cross both at once. Backends live
in their own packages, so this one depends on nothing but `without` and
`without-dag`:

- [`without-durability-redis`](https://pypi.org/project/without-durability-redis/), where each guarantee is a small Lua script
- [`without-durability-postgres`](https://pypi.org/project/without-durability-postgres/), where each is an ordinary transaction
- [`without-durability-sqlite`](https://pypi.org/project/without-durability-sqlite/), the same over one file, with no server and no driver

`MemoryCheckpointer` and `MemoryScheduler` ship here, so a test injects a dict
rather than starting a container. They meet the protocol's requirements rather
than approximating them.

## Two mechanisms, one checkpoint

`run_durably` and `run_saga` run a `without-dag` `CompiledGraph` against the same
seam, recording each `(node key, result)` before pulling the next, so a resumed
run re-enters only what had not finished and a failed one can drive a
compensating graph that is itself checkpointed.

`Run.transact` is the one place a step is more than bookkeeping: it hands the
store an effect the *store* can perform, so the work and the record commit
together and that step is exactly-once rather than at-least-once. What bounds it
is not this seam but distributed transactions, so it is available exactly for
effects that live where the checkpoint lives.

`work(durable, body)` is a queue worker over the same seams: a pool of passes, a
timer, and backpressure that falls out of pulling one delivery per free slot.

See the
[`without-durability` guide](https://without.help/without-durability/)
(with the [API reference](https://without.help/without-durability/reference/))
for the full surface, what each store can and cannot promise, and the gaps that
keep this a substrate rather than a replacement for Temporal or DBOS.
