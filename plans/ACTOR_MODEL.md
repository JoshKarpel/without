# The Actor Model Question

An initial take on Checkpoint 6's open question #2: is `without` an actor
framework wearing stream-processing clothes, or does the stream framing buy
something actors do not? This is a thinking aid, not a decision: a position to
chew over and pressure-test, not a settled conclusion.

## Crux

`without` is not an actor framework wearing stream clothes, but it is closer
than the stream framing lets on. The honest relationship is that **an actor is a
*derivable pattern* in `without`, not a primitive**: an actor is a `from_fold`
whose input is a dynamic merge of every sender's stream. You don't *have*
actors; you reconstruct one at exactly one spot (the `inbox` funnel), and that
single reconstruction is the only place the resemblance bites.

This points at option (iii) from the checkpoint (articulate precisely why the
two differ), with an option (ii) posture in the code (stay stream-first, treat
the actor resemblance as a consequence). It argues against option (i) (lean in
and name actor concepts as first-class vocabulary).

## The decisive distinction: an actor *is a place*

The deepest difference is not on the checkpoint's candidate list (a)/(b)/(c).
It is the one the `values-over-places` rule already names.

An actor address is a reference to an identity-bearing, mutable thing: you send
to *it*, and what it holds changes over time. That is the definition of a place.
The actor model is places-over-values at its core: the actor has identity, a
hidden mutable cell, and you reach it by reference.

`without` is the dual:

- State is a **threaded value** through the fold, not a cell hidden behind an
  address. This sharpens candidate distinction (a): it is not "no shared place
  by convention," it is "the actor concept *is* the place we refused to build."
- The reply target **rides in the value** (`Connected.send`) rather than through
  a side registry keyed by a surrogate id. The code comment on `Connected`
  already makes the values-over-places argument; it just has not been connected
  to the actor question yet.

Cleanest articulation: the actor model is places-over-values (identity +
address + cell); `without` is values-over-values (threaded state +
reply-in-the-value + structural composition). Both serialize
state-mutation-through-a-queue identically, which is why they rhyme. They differ
on whether the serial owner is a place you address or a value you compose.

This framing subsumes the checkpoint's (b) and (c):

- **(b) composition by stream transform vs addressing** follows from value vs
  place: structural composition (`pipe`/`distribute`/`tee`/`merge`) is possible
  precisely because there is no identity to address.
- **(c) end-to-end backpressure vs unbounded mailboxes** follows too: bounded
  backpressure is possible precisely because the substrate is a bounded stream
  rather than a mailbox bolted under an address.

So value-vs-place is the root; (a), (b), (c) are its consequences.

## Honest counter-pressure

Two places the stream-first story is genuinely weaker. Do not paper over them.

1. **Addressing for dynamic, many-to-many topology.** Static `merge` cannot
   express "A sends to B by name without B's stream being wired into A." The
   `inbox` queue plus the per-connection `replies` queue *is* a hand-rolled
   address pair (the checkpoint already flags this). The deciding question is
   empirical: does `without` believe real systems are mostly-static dataflow
   with a few dynamic fan-in points (servers), or are they genuinely dynamic
   actor graphs? If the former, you need exactly *one* named primitive (the
   dynamic-merge / `inbox`) for the fan-in case and never import the full actor
   object model. If the latter, you will keep reinventing addresses and should
   reconsider.

2. **Supervision and "let it crash."** The actor model's real-world payoff in
   Erlang is not dataflow: it is the supervision tree and the fault/availability
   model. `without` has nothing here, and arguably should not pretend to. This
   is actually evidence *for* (iii): `without` is borrowing the mailbox, not the
   fault model, so calling it an actor framework would over-claim.

## Recommendation

Lean (iii) with a (ii) posture in the code:

- **Name one primitive, not a vocabulary.** Build the deferred **dynamic-merge
  connector** and let it be the single sanctioned "mailbox": a dynamic fan-in
  that still reads as a stream and feeds a fold. That replaces the raw
  `asyncio.Queue` funnel with something compositional, pulling the one un-clean
  spot back inside the stream model. Do not introduce `Actor`, `address`,
  `tell`/`ask` as first-class types.
- **Keep `ask`/`tell` as descriptive prose, not API.** They are accurate names
  for what a sender does; they are not new concepts. The moment they become
  types, you have started building the place-based model you just argued
  against.
- **State the relationship in `BIG_IDEA.md` as a one-liner test** the reader can
  apply: can you point to *the* keyspace and message it from anywhere (address),
  or is its input a stream you wired/merged (composition)? Today the answer is
  "address" (the shared `inbox`), which is why the toy feels actor-ish. The
  dynamic-merge connector is precisely the move that flips the answer back to
  "composition" without losing the dynamic-connection-set capability.

Resist option (i). Naming the actor concepts first-class would reintroduce
identity and addressing as primitives, quietly trading the values-over-places
foundation for the exact place-based model the fold was built to avoid. The
resemblance is real because both discipline shared mutation through a single
serial queue, but that is a convergent solution to mutual exclusion, not
evidence that `without` is secretly building actors.

## One thing to decide first

Is the dynamic-merge connector the *resolution* of this question, or separate
from it? The read here is that they are the same question: building that
connector *is* how you answer "stream-first, actor-as-consequence" in code
rather than in prose. The connector's signature and semantics (against the
current `merge` / `stream_from_queue`) are the natural next sketch.
