# How a step's result becomes something a store can hold, and how it comes back.
#
# It is an interface rather than a constant because it is a *boundary* decision, and the
# boundary belongs to the application: what a workflow's steps return, what an operator
# needs to read out of the store, and what a service written in another language has to
# parse are questions this library cannot answer. Baking `json.dumps` into four stores
# would answer them four times, identically, and wrongly for anyone whose steps return a
# domain value the stdlib encoder has never heard of.

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol


class CheckpointCodec[Encoded](Protocol):
    """
    How a checkpointed value crosses into a store and back out.

    `Encoded` is what *this* store can hold: text for the three shipped here, since a
    Redis hash field, a SQLite `TEXT` column, and a Postgres `jsonb` all take it. It is a
    type parameter rather than a fixed `str` because that is a fact about each store and
    not about codecs, and a store that holds bytes should be able to say so.

    Two requirements, and the second is the one that is easy to miss.

    - `decode(encode(value))` MUST equal `value` for every value a workflow's steps
      return. A codec that does not round-trip makes a resumed pass see something the
      first pass did not, silently, one crash later. The stdlib `JsonCodec` below does
      *not* round-trip a tuple (it comes back a list) or a mapping with non-string keys,
      which is why a workflow using it must keep its step results JSON-native.
    - `encode` MUST be deterministic: equal values encode equal. `Checkpointer.record`
      decides who won a race by comparing encodings, so a codec that renders one value
      two ways reports a conflict that did not happen.

    Both are properties of the pair, which is why a codec is one object rather than two
    functions: the stores do not merely encode, they compare encodings to decide who won
    a race and hand the *decoded* form back so a pass reads what the next pass will.

    Only the encoded side is a parameter, and that asymmetry is real rather than an
    oversight. `Encoded` genuinely varies: the stores here hold text, and one that held
    bytes would say so. The decoded side cannot, because a checkpoint is heterogeneous by
    construction: a workflow's `"charged"` holds a string, its `"items"` a mapping, its
    `"settling"` a deadline, and one codec carries all of them. A `Decoded` parameter
    would sit in `encode`'s argument *and* `decode`'s return, making it invariant, so a
    `CheckpointCodec[Step, str]` would be refused by the very store it was written for.

    Precision belongs inside a codec instead, where it costs nothing: a pydantic codec's
    `TypeAdapter` can be as exact as it likes about what a workflow returns while still
    presenting `object` here. That is the move `without_dag.Node` already makes, crossing
    the executor interface as `object` with a typed frontend restoring precision above it.
    """

    def encode(self, value: object) -> Encoded: ...

    def decode(self, encoded: Encoded) -> object: ...


@dataclass(frozen=True, slots=True)
class JsonCodec:
    """
    The stdlib's JSON, as a `CheckpointCodec[str]`, and the default every store here takes.

    JSON because it is what makes a checkpoint readable by an operator with `redis-cli` or
    `psql` and by a service written in something other than Python, which is most of what a
    durable workflow's state is *for*. The stdlib because a default should add no
    dependency; it is the slowest of the reasonable choices and the narrowest, and both are
    the point of the codec being swappable.

    What it costs is stated rather than hidden: a step result MUST be JSON-native, and
    "JSON-serializable" is not the same thing. A tuple encodes and comes back a list, and a
    mapping with integer keys comes back with string ones, so both break the round trip the
    protocol requires. A codec that knows the application's types (a pydantic
    `TypeAdapter`, msgspec with a schema) is how a workflow gets to return domain values,
    and swapping one in changes the store's construction and nothing else.
    """

    def encode(self, value: object) -> str:
        return json.dumps(value)

    def decode(self, encoded: str) -> object:
        return json.loads(encoded)


# One shared instance rather than a default factory, because a codec with no fields is a
# value: nothing about it is per-store, so every store holding the same one is correct.
JSON: CheckpointCodec[str] = JsonCodec()
