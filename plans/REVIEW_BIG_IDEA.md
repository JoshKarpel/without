# Review: BIG_IDEA.md

Observations and open questions from a first read, recorded before scaffolding.
These are arguing points, not conclusions.

## Sharpened thesis

The original framing of "anything can be modeled as a stateful stream
processor" reads as vacuous (so can a Turing machine). The clarification from
discussion is what makes it load-bearing:

> The vacuousness is the power. Python has many competing frameworks with
> similar-but-subtly-different shapes (ASGI apps, Click commands, Kafka
> consumers, asyncio protocols, Airflow operators). They are non-interoperable
> because none of them names the shared lower layer. If we name that layer,
> the pieces compose.

So the bet is not "you *can* model things this way" but "naming the common
substrate buys interoperability that the ecosystem currently lacks." That
reframes the project from a framework into a *contract* (a narrow waist), more
like ASGI or WSGI than like FastAPI. The success metric follows: not "is it
expressive" but "can two independently written pieces (a k8s-configmap context
and an HTTP stream) snap together without either knowing about the other."

The agreed real test: build a web server on top of it and see what the user
code looks like. If that code is worse than `async def handle(request)`, the
abstraction has not paid rent.

## What is strong

- **Sans-IO as the testability lever.** Proven pattern (h11, h2). Maps onto
  functional-core/imperative-shell: the processor is the pure core, the
  executor is the shell. Hold this line and testing falls out for free.
- **Lifespan-as-a-variable.** The observation that an HTTP-request handler and
  a Redis replica are the same shape with different state lifespans is a real,
  non-obvious unification. It names a *what* (stateful stream processor)
  independent of a *how*.
- **Library-not-framework / user-visible control flow.** Correct north star;
  also the hardest constraint to keep (see tension below).

## Tensions to resolve

- **DAG vs. visible control flow.** "Declare inputs, not order" (declarative
  DAG) and "control flow totally user-visible" (imperative, readable) pull
  opposite directions. Airflow/Prefect/Dagster all resolve toward *less*
  visible control flow. You cannot fully have both. A decision is required:
  which wins on conflict. Working hypothesis: visibility wins, and the DAG is
  *recovered* from declared dependencies for parallelism, not hand-assembled by
  the user.
- **Mutable vs. immutable context by type choice.** Encoding "does this state
  escape / can observers see mutation" in whether the author used a mutable
  class vs. a frozen pydantic model is fragile, and it is exactly the
  values-over-places concern. The Redis-clone question ("is the held state a
  long-lived processor or a mutable context?") is a symptom of not having
  separated *identity* from *state* yet. Make that an explicit, first-class API
  decision rather than an emergent property of a type annotation.
- **Context is just a processor.** "Contexts can be updated by streams of
  events too" (file watcher reloading config) means a context already *is* a
  stateful stream processor whose state other processors read. That collapse is
  elegant and worth leaning into: a context = a processor + a read handle to
  its current state. The sketch encodes this.

## Visualization requirement (from discussion)

Agreed: recover the DAG dynamically from declared inputs rather than having the
user assemble it. Added constraint: there MUST be an easy way to visualize the
recovered shape (a mermaid diagram generator is the target), powered by `@`
decorators as a first step. This keeps control flow user-visible (you can *see*
the graph the engine inferred) while letting the executor own ordering and
parallelism. The decorator MUST return the wrapped object unchanged so user
control flow stays plain Python (library, not framework). The scaffold includes
a first-step `without.graph` doing exactly this: a `@node` decorator that
infers edges from declared inputs and renders mermaid.

## Resolution: no privileged executor; events vs. behaviors

The first sketch had a stub `Executor` as the open question. Discussion
dissolved it. The model is "processors all the way down": the only thing a user
writes is a `Processor`, a transformation from a stream of inputs to a stream of
outputs, and one processor's output stream is another's input stream. There is
still a runtime that provides impure source streams and runs the loops, but it
is a thin interpreter of the wiring, not a concept the user models with, so it
is not a peer of `Processor` and gets no `Executor` type.

What "processors all the way down" does *not* dissolve, and the model keeps
explicit:

- **The I/O boundary.** Homogeneity of *interface* (everything is stream to
  stream) is good; homogeneity of *implementation* (every node may do I/O)
  would throw away the sans-IO testability that is the point. So the interior is
  pure stream-transformers (built from a reducer via `from_reducer`, which
  supplies the scan) and only the source streams at the edge touch the world.
- **Two edge types (Conal Elliott's events vs. behaviors).** An *event* edge
  feeds every output onward (`compose`, which is just composition). A *behavior*
  edge exposes a stream's *latest* value as a `Context` (`sample`,
  latest-wins, no backpressure). This is what the "is held state a processor or
  a context?" question was circling: it is a processor; "context" names how a
  reader connects to it. `sample` is the one connector that needs something
  running, so it is the visible seam where the imperative shell appears.

Known-hard problems inherited from dataflow/FRP, to decide deliberately rather
than discover: backpressure on event edges, glitches on diamond dependencies
(the DAG-from-declared-inputs idea makes diamonds common), and feedback cycles
and teardown order.

## Sequencing recommendation

Build HTTP *last*, not second. It is the most demanding validation and will
force premature abstractions. Order:

1. config-from-env (`pydantic-settings`) — trivial, a static context.
2. config-from-k8s-configmap (`watchfiles` + `pydantic`) — proves the
   context-updated-by-an-event-stream loop end to end.
3. a toy line-protocol server (Redis-ish) — proves long-lived processor state.
4. HTTP (sans-IO deps) — the real test of the contract.

If the same core survives all four, there is something here. If not, we found
out cheaply.

## The contract is the project

Everything else is a plugin. If the signature for "feed an event and the
current context into a processor" cannot be written down as types, the idea is
not ready. If it can, that signature *is* the project. The first scaffold
deliverable is therefore `without.contracts` as a typed sketch, not prose.
