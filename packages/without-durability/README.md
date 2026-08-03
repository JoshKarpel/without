# without-durability

Durable workflows built on the observation that once a workflow's state is a
**checkpoint any process can read**, most of a workflow engine stops being
necessary. A workflow is an ordinary async function whose effects are named;
resuming means calling it again; each step it reaches hands back what is already
recorded instead of running.

```python
from without_durability import Completed, MemoryCheckpointer, Run, Sleeping, Waiting, claimed, resume

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
match await resume(await claimed(checkpointer, "order-42"), checkpointer, fulfil):
    case Completed(value=reference): ...   # the workflow finished
    case Sleeping(due=due): ...            # schedule a wakeup for `due`
    case Waiting(key=key): ...             # nothing to schedule; someone must write `key`
```

A pass comes back as one of those three rather than raising two of them, so what to do
next is a `match` a type checker can tell you is incomplete. Inside the workflow a
suspension is still an exception, because that is how you stop in the middle of
straight-line code; `resume` is the boundary where it becomes a value.

Each read names a parser because a step hands back what the *store* holds, not
what its effect returned: the value has been through a codec, so a step returning
a tuple is handed a list on the pass that ran it. A parser makes the return type
something a function proved rather than something a `cast` asserted.

There is no server here and no engine. What there is instead is an interface, and
the interface is where the interesting part lives.

A protocol of `load` and `record` is too weak to run a workflow safely at any
scale, because it cannot say "only if nobody else is running this" or "only if I
am still the one who may write". So `Checkpointer` states the requirements and an
implementation says how it meets them: `claim` grants at most one live pass and
issues strictly increasing fencing tokens, `record` refuses a write from a
superseded pass and never overwrites a recorded step. That is the same problem
Temporal answers with a server and DBOS answers by requiring Postgres; here it is
stated as an interface, so a deployment brings whatever store it already runs.
`Scheduler` is the other half of a workflow's state, its right to run, and
`Durable` is the pair plus the transitions that have to cross both at once.

Stores live in their own packages, so this one depends on nothing but `without`
and `without-dag`:

- [`without-durability-redis`](https://pypi.org/project/without-durability-redis/), where each guarantee is a small Lua script
- [`without-durability-postgres`](https://pypi.org/project/without-durability-postgres/), where each is an ordinary transaction
- [`without-durability-sqlite`](https://pypi.org/project/without-durability-sqlite/), the same over one file, with no server and no driver

`MemoryCheckpointer` and `MemoryScheduler` ship here too, so a test injects a dict
rather than starting a container.

The same interface carries a second mechanism: `run_durably` and `run_saga` run a
`without-dag` `CompiledGraph` against it, recording each `(node key, result)`
before pulling the next. `work(durable, body)` turns either into a running
service, a pool of passes plus a timer, with backpressure that falls out of
pulling one delivery per free slot.

See the
[`without-durability` guide](https://without.help/without-durability/)
(with the [API reference](https://without.help/without-durability/reference/))
for the full surface: the two mechanisms, the worker, `Run.transact` and the
exactly-once step it buys, what each store can and cannot promise, and the gaps
that keep this a substrate rather than a replacement for Temporal or DBOS.
