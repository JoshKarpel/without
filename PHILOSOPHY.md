# The Philosophy of `without`

Two ideas, and what follows from them.

The first is that a **stateful stream processor** is a universal way to model
computation, general enough that a web handler, a database replica, a log
pipeline, a config reloader, and a durable workflow are all the same shape with
different lifetimes. The second is that an ecosystem built on that shape should
be **layers with narrow interfaces between them**, so that a user meets only the
altitude they need, descends when they need more, and can replace any one layer
without rewriting the others.

Everything else here is downstream of those two. The craft principles the code
leans on (values over places, parse don't validate, functional core and
imperative shell) are the tools that make the two ideas work in Python; they get
their own section near the end, and they are not the philosophy.

This is the standard new work is measured against, not a tour of what exists:
each package's own guide carries its design narrative, and where a guide and this
document disagree about what the code *does*, the guide is closer to the code.
Some of what follows is ahead of the code, and those gaps are marked in the main
text rather than left to be discovered.

## Everything is a stateful stream processor

### The claim, and why its emptiness is the point

Python has many frameworks with similar but incompatible shapes: ASGI apps,
Click commands, Kafka consumers, asyncio protocols, task queues, DAG operators.
They do not compose, because none of them names the layer they share. The wager
is that if you name that layer precisely enough to write down as types, pieces
written independently snap together. That makes the project a **narrow waist**
in the sense the internet protocol stack means it, a thin universal layer that
everything above and below is written against, and so an interface rather than a
framework.

The shape is a processor that consumes a stream of inputs, carries state across
them, and produces a stream of outputs. Taken as a claim about the world, that is
nearly vacuous, in the way "everything is a file" and "everything is a Turing
machine" are vacuous. The emptiness is what makes it work. A universal modeling
method earns its keep not by describing any one system especially well but by
describing *all* of them the same way, so that two systems written by people who
never met can be wired together without an adapter.

So the success metric is not expressiveness. It is composition. Can a
configuration source and a network connection, written independently and ignorant
of each other, be plugged together? If yes, the interface is doing its job, and
the interface is the project. Everything else is a plugin, and a plugin is also
how an interface gets tested: one with a single implementation has not been shown
to be an interface at all.

### Lifespan is a variable, not a category

The sharpest form of the claim: an HTTP request handler and a long-lived database
replica are the *same shape* with different state lifespans. One holds state for
milliseconds, the other for weeks. Nothing else about them differs at this layer.

Frameworks usually treat those as separate kinds of thing with separate
vocabularies, which is why moving logic between them means rewriting it. Naming
the shared shape names a *what* independent of any *how*, and the lifespan
becomes a parameter rather than a category you are stuck inside.

The practical payoff is that lifecycle bookkeeping disappears. A connection's
lifecycle simply *is* its stream's lifecycle: end of stream is end of connection,
so a pile of "is this finished yet" state never needs to exist.

### The model nests, and you choose how far

The shape holds at every zoom level, which is what makes it worth calling
universal:

- A server is a stream of connections. A connection is a stream of requests. A
  request is a stream of events. Same shape three times, at three lifetimes.
- Inside a single step, one input value can drive a whole concurrent graph of
  sub-computations that recombines into one output. That is value-level fan-out
  living *within* a processor, distinct from splitting the stream itself.
- A durable workflow's pass is a stream of completed steps folded into a
  checkpoint, which is why adding durability needed a store and a queue rather
  than an engine.

Available at every level is not the same as mandatory at every level, and the
difference matters more than it first appears. Reaching for the shape is how you
make two things snap together, so it earns its place at the boundaries you
actually have to compose across. Below those, ordinary code is ordinary code, and
decomposing further because the model would permit it buys nothing.

So one problem admits several honest zoom choices. A workflow can be a set of
services exchanging messages over queues, which puts the processor boundary at
every step; or it can be straight-line code with a stream of runs moving behind
it, which puts the boundary around the whole workflow and leaves its interior as
plain Python. People build both, and this is meant to *help* build either rather
than to insist on one. The question a new problem raises is which zoom level pays,
not whether the model needs an escape hatch.

### The model has two halves: events and behaviors

A stream alone cannot express held state without smuggling in a mutable place, so
the model has a second half, borrowed from Conal Elliott's
[functional reactive programming](http://conal.net/papers/icfp97/): *events*,
where every occurrence matters and a consumer sees all of them, and *behaviors*,
where only the latest value matters and a reader samples it without blocking.

This is what stops long-lived state from being a special kind of object. Config,
a connection pool, a sampling rate: none is a different category, each is another
processor's output that a reader samples instead of consumes. The question "is
this state, or is it a process?" dissolves, because "behavior" names how a reader
connects to something rather than what that thing is.

The duality then recurs at every layer that has both reads. A graph executor
yields each result the instant it lands *and* offers the single-value read that
keeps only the output. A new interface should be able to say which half it
offers, and one that is secretly both is an interface to split.

### State threads through; it does not sit somewhere

Where state lives is decided by its scope, not by taste:

- Thread it *down* when it is scoped to that level. A per-connection counter
  belongs to that connection.
- Funnel it *up* to a single serial owner when it is shared. A serial consumer
  pulls its next event only after the current one is fully handled, so its
  read-modify-write is serialized without a lock even across suspension points.

That dissolves what looks like a forced choice between one serialized owner and
many concurrent workers over shared mutable state. You can have the concurrent,
fractally per-request shape *and* keep shared state out of any place, by
funneling it into one serial owner the concurrent parts message.

This is also where the model parts ways with the actor model it superficially
resembles. Both serialize mutation through one queue, which is why they rhyme.
They differ on whether the serial owner is a place you address or a value you
compose. Borrow the mailbox, not the identity.

### There is no privileged executor

Something has to supply impure source streams and run the loops, but it is a thin
interpreter of the wiring rather than a concept a user models with, so it gets no
type and no peer status beside the substrate's own.

Homogeneity of *interface* is the goal, and homogeneity of *implementation* is
explicitly not: if every node were free to do I/O, the testability that makes the
model worth having would be gone.

## Layers you can descend, remix, and replace

One universal shape is not enough on its own. If the substrate is all you ship,
every user starts from raw materials, and the interoperability the first idea
buys is spent on ceremony. The second idea is that the ecosystem is a stack of
thin layers, each a narrow interface over the one below.

### Progressive disclosure of complexity

The top layer should answer the common case in a line. The layer beneath it
should be a *supported interface* rather than an escape hatch you are punished
for reaching, and descending to it should cost only the layer you replaced, never
the ones above and below.

The shape this takes in practice: the unopinionated boundary ships no router at
all, and the opinionated router is a separate package whose dispatch function
simply *is* the type the boundary already accepts, so bring-your-own or none
stays first class. The graph executor ships scheduling and leaves graph
definition to a typed frontend, so a different frontend could sit on the same
interface without either side learning about the other. Durable workflows ship
the interfaces and leave the stores to their own packages. A logging pipeline
ships the record, the composition, and *optional* renderers, so a formatter is
never on the path everyone crosses.

The test for an interface: could the layer above be replaced wholesale without
the layer below noticing? If not, the interface is in the wrong place, or it does
not exist and you have one layer wearing two names.

### Components travel, because they are self-contained values

Remixing only works if a piece can be lifted out of the assembly that produced it.
So there is no registry to register into, no singleton to reach for, and no
ambient application object: you hold the thing itself, and identity is the value
rather than a name in a table something else keeps in sync.

A route that carries its own prefix in its own segments can be reversed into a
URL with no router present, moved between applications, and shipped by a package
that does not know where it will be mounted. A route that is a name in a
registry can do none of that without the registry coming along.

### A layer must not decide for the layer above

Produce the typed value and leave the boundary encoding to whoever owns it. No
mandatory serializer, no content type, no exception-to-status registry, no
formatter baked into the common path. Push the decision out, or make it
injectable with a sane default.

The test for a proposed helper: does it bake in a decision the application should
own? A layer that answers a question the layer above should answer has to be
worked around exactly when their answers differ, which is exactly when it
matters. One value with two renderings, both chosen by the caller, is the shape
to aim for.

Overflow, backpressure, and degradation are boundary decisions too. A bounded
queue with a caller-chosen capacity and a visible drop count is policy made
observable; an unbounded queue chosen silently on the caller's behalf is a
decision taken from them.

### A dependency is a choice, so take only the ones that aren't

The same test governs what we install, because a dependency in a library's
metadata is a decision imposed on everyone downstream. The question is not "how
few dependencies can we have"; it is *how few choices do we make on the user's
behalf*. Those come apart, and reading the first for the second produces a
library that reimplements a protocol badly to keep a number low.

So the criterion is whether a real choice exists. Where a capability has one
obvious implementation and no live alternatives with different trade-offs, taking
the dependency decides nothing: nobody was going to pick differently, and
declining it means shipping a worse copy of the same thing, or pushing an
assembly step onto every user to no end. Brotli is that case (the stdlib has no
brotli and Google's bindings are the implementation), and so are
`aiohappyeyeballs` for dual-stack connection ordering and `h11` / `h2` /
`wsproto` for sans-IO protocol state machines. We take those, and they belong in
the layer that needs them rather than the layer that happens to already have
them, so `br` is in the server's default coding table and works without ceremony.

Where the alternatives are real, the choice is the user's and a default that
picks one is the layer deciding above itself. JSON encoding is the clearest
example: the stdlib, `orjson`, `msgspec`, and the rest trade speed against type
coverage against strictness, and which trade is right is a property of the
application. So `json_content` takes a `dumps` argument, defaults to the stdlib
because a default must add no dependency, and the fast encoder is one argument
away for whoever wants it. The same shape recurs everywhere the layers meet: a
coding table, a serializer, a resolver, a schema generator.

The two halves of the test are worth stating together, since only the second is
about counting. Take the dependency when it makes no choice for anyone; make it
an argument when it does.

### Each layer describes itself, and the whole is a merge

Whichever layer parses or produces a value is the single source of truth for that
value's schema. A combined description, an API document, a diagram, a dependency
graph, is then *recovered* from the parts rather than maintained beside them. A
description kept in parallel with the thing it describes is denormalized state,
and it drifts; derive it and it cannot.

Keeping enough structure around to make that derivation possible is part of the
design rather than an afterthought.

### Narrow interfaces need restraint

Layers stay swappable only while the interfaces between them stay small, so new
surface arrives on evidence: a need a shipped package actually has, not one that
can be imagined. Duplication is cheaper than the wrong abstraction, and a cut
point is found rather than planned.

Two habits keep the interfaces thin. Prefer a new *composition* of the existing
vocabulary to a new mechanism, so exception handling is ordinary middleware and
mounting is a transform over route values rather than a wrapper the router has to
know about. And weigh what an addition carries: a builder that introduces no
machinery is nearly free, while one that brings a queue and a background task is
where complexity actually lives.

The counterpart is knowing which coupling is *essential*. Decompose until only
the genuinely inseparable remains bundled, then say plainly why it is
inseparable: a size-based rotation threshold is a function of the bytes written,
so it has to live in the write path, and once size is there the joint size-and-time
decision has to be too. Everything a monolithic version fuses together *besides*
that still composes out.

What restraint costs is real: a genuine need waits, and someone writes by hand
what a connector would have given them. That is the trade, taken deliberately.

The gap today: fan-out to several terminal branches ships, but fan-in over a
*changing* set of sources does not, and more than one package has now wanted it.

## The tools we build it with

None of what follows is this project's idea, and none of it is the philosophy.
These are the practices that make the two ideas above work in Python, and each
earns its place by supporting one of them. They are listed so that a
contributor knows which discipline is load-bearing here and why, not to argue for
them again.

**[Values over places](https://www.infoq.com/presentations/Value-Values/)**
(Rich Hickey) is what makes the stream model composable at all. An immutable
value can be handed to every branch of a fan-out with no lock and no defensive
copy, which is why splitting a stream is free, why a checkpoint is just a
mapping, and why a serial owner's state is safe across suspension points. Two
habits follow: when something arrives as a place, turn it into a value at the
edge while it is still live; and watch for values that are secretly places, since
a frozen container can hold a mutable interior.

**[Functional core, imperative shell](https://www.destroyallsoftware.com/screencasts/catalog/functional-core-imperative-shell)**
(Gary Bernhardt) is what makes a layer swappable. A core that does not know which
shell runs it can be run by a second one, and that is a property you can check
rather than a virtue you can claim: if swapping the shell forces an edit to the
core, the split was nominal. I/O is decoupled, not forbidden, so a step may await
it as long as the effect completes within the step and never escapes. Where
purity is genuinely impossible, name the edge and contain it: a foreign system
met this way is a source, strictly upstream, never a callback partner.

**[Parse, don't validate](https://lexi-lambda.github.io/blog/2019/11/05/parse-don-t-validate/)**
(Alexis King) is what makes an interface real. A layer that hands typed values
across does not need the layer above to re-check them, so the boundary carries a
proof rather than a convention. A rejection at that boundary should be an ordinary
outcome the surrounding structure absorbs, not an error escaping to somewhere
with less context. Defaults follow role: a type filled from outside input carries
none, so a forgotten field fails loudly, while a type the application constructs
carries them for ergonomics.

**Types as the guardrail** are what make remixing safe. Wiring the wrong layer to
the wrong one should be a static error, not a runtime surprise: a cycle
unrepresentable through a builder API, a request-body read on a route that has no
body rejected at the call site. Prefer, in order, a structure that cannot express
the mistake, a static error, a loud failure at the boundary, and only then a
runtime guard.

**[Simple, not easy](https://github.com/matthiasn/talk-transcripts/blob/master/Hickey_Rich/SimpleMadeEasy.md)**
(Hickey again) decides what shape a layer presents, and the specific trap is
mistaking symmetry for simplicity. The recurring instance is who holds the
continuation: where the library calls inward, the user writes a node and the
shape is the substrate's; where the user holds the continuation and the code
after the call is theirs, they are at the rim writing a script, and the honest
shape there is imperative. A client request is at the rim. Contorting a rim API
into a node to make the library look uniform buys nothing and costs the caller.

**Strictness follows authorship** settles how to react to an unexpected value,
and the question is who wrote it. A value the application author produced is
under their control, so an unexpected one is a bug in code they own: fail loud,
where it can be seen and fixed. A value from a remote peer is controlled by
nobody here, so hard-failing would let any peer take the system down: log enough
for an operator and degrade. A third case is neither, since a peer adding a field
or an event kind must not break a consumer that never asked for it; ignoring it
is a deliberate choice rather than a swallowed error.

**Dependencies are arguments.** Injection is what turns "replace a layer" into a
call-site change rather than an edit, and it is what lets the whole stack be
tested without mocks, since injecting a different value is the entire technique.

## What is open

The project inherits the hard problems of dataflow and chooses to face them
rather than discover them. Backpressure is handled where it arises rather than
bolted on, and where a path deliberately has none, that is stated as a property.
Glitches on diamond dependencies, feedback cycles, and teardown ordering are
open, and are tracked as open.

One gap in the two big ideas is named above: a missing fan-in over a changing set
of sources. Two smaller ones sit in the craft: a derived-name convention that is
documented rather than enforced, and a workflow-authoring rule that nothing
checks.

The same honesty applies to what a package does not do. A guide that ends with
what is deliberately absent, and why, does more for a reader than one that ends
at the feature list, because silence about a limit reads as a claim that there is
none. At runtime the same rule holds: a system that sheds load or serves
something degraded must make that visible, since a silent drop reads exactly like
having handled everything.
