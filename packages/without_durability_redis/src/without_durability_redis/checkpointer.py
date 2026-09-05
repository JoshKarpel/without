# The `Checkpointer` implementation the durable runner talks to. A workflow is one Redis
# hash, a completed step is one field in it, and the vocabulary is small, because the
# shape `CompiledGraph.stream` emits (a mapping of name to result) is already the shape a
# hash holds. Nothing above this module mentions Redis, and nothing in it mentions the
# workflow's domain.
#
# A field holds `<position>:<encoding>` rather than the encoding alone, which is how this
# store meets `load`'s ordering guarantee. A hash cannot carry it: Redis preserves field
# order only while the hash is listpack-encoded and converts to a hashtable past
# `hash-max-listpack-entries` or `hash-max-listpack-value`, after which `HGETALL` order is
# unspecified. Since the order has to be recorded somewhere, recording it *in the field*
# keeps it to one key, so there is no second structure to expire in step with this one and
# no branch that could allocate a position for a write that turned out to lose. The prefix
# is added and stripped inside the scripts, so it never reaches the codec or an effect.
#
# Every write here is a Lua script, and each is a script for the same reason: what it
# does is only correct as *one* step. Checking whether a workflow is free and taking it,
# checking a fencing token and applying the write it guards, testing whether a key is
# already recorded and reading back the winner. Split any of those into two round trips
# and the gap between them is where the guarantee leaks.
#
# The keys are hash-tagged (`workflow:{id}`, `workflow:{id}:pass`) so that the tagged id
# decides the slot and a workflow's two keys always land on the same one. Without that,
# `record` (which touches both) is a cross-slot command that Redis Cluster refuses, so
# the tag is what keeps this correct on more than a single node.

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from datetime import timedelta
from typing import cast

from redis.asyncio import Redis
from redis.commands.core import AsyncScript
from redis.exceptions import ResponseError
from without_durability.codec import JSON
from without_durability.codec import CheckpointCodec
from without_durability.interfaces import INBOX
from without_durability.interfaces import INBOX_DIGITS
from without_durability.interfaces import Entry
from without_durability.interfaces import Fenced
from without_durability.interfaces import Pass
from without_durability.interfaces import Recorded
from without_durability.interfaces import check_duration

from without_durability_redis.units import milliseconds
from without_durability_redis.units import seconds

# The one place the packed field format is defined, spliced into every script that writes
# or reads one so the two halves cannot drift apart.
#
# The position handed to `pack_value` is `HLEN` taken immediately before the write, which
# is the field's insertion index: no field is ever deleted on its own (nothing here calls
# `HDEL`), first-writer-wins means none is ever replaced, and the hash expires whole, so a
# reused workflow id starts from zero with no surviving fields to collide with. That first
# clause is an invariant of this module rather than something Redis enforces, so a future
# `HDEL` here would silently start handing out positions that are already taken.
#
# Splitting on the first colon is safe whatever the codec produces, because the position is
# always digits and always in front.
PACKING = """
local function pack_value(position, encoded) return position .. ':' .. encoded end
local function bare_value(packed) return string.sub(packed, string.find(packed, ':', 1, true) + 1) end
"""

# Take the workflow if nobody holds it, and stamp the taking with a number that only
# ever goes up. It is the store, not the claimant, that decides the ordering, so two
# processes cannot mint the same one. The clock is the server's rather than a caller's,
# because a lease compared against the claimant's clock is only as good as the agreement
# between the two, which is exactly what fails when a machine is unhealthy enough to
# stall mid-pass.
#
# The token is `max(now, previous + 1)`, which is a hybrid logical clock and not merely
# a counter, and the difference is the one hazard a plain counter leaves open. These
# keys expire, so a workflow that goes quiet for longer than the `ttl` is forgotten
# entirely; if its id is then reused, a counter would hand the new incarnation token 1
# while some pass stalled since before the expiry still holds token 3, and the corpse
# would outrank the living. Seeding from the wall clock closes that without coupling the
# two keys' lifetimes, and taking `previous + 1` when that is larger keeps it strictly
# monotonic within one incarnation even if the clock steps backwards under it.
#
# The precision is the limit of that half: two claims separated by an expiry but not by a
# millisecond would be stamped the same, and equal tokens do not fence each other. What
# makes that unreachable is the `ttl` rather than the arithmetic, since an expiry cannot
# happen in under a day. The guarantee rests on that margin, not on the token alone.
#
#   KEYS[1]  the workflow's pass hash
#   ARGV[1]  lease, in milliseconds
#   ARGV[2]  expiry for the pass hash, in seconds
#   returns  the fencing token, or nil if another pass holds the workflow
CLAIM = """
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local held_until = tonumber(redis.call('HGET', KEYS[1], 'until') or '0')
if held_until > now_ms then return nil end
local previous = tonumber(redis.call('HGET', KEYS[1], 'token') or '0')
local token = math.max(now_ms, previous + 1)
redis.call('HSET', KEYS[1], 'token', token, 'until', now_ms + tonumber(ARGV[1]))
redis.call('EXPIRE', KEYS[1], ARGV[2])
return token
"""

# The fenced, conditional write. Refuse anything from a superseded pass, never overwrite
# a step that is already recorded, and hand back whatever is stored once the dust
# settles, so a caller that lost the race learns the winner's value instead of carrying
# on with its own.
#
# It reports *who won* alongside that value, which is the one thing the caller cannot
# work out afterwards (see `Recorded`). The comparison that decides it is between
# encodings, here, where both are in hand, so two passes that ran the same effect and
# produced the same encoding count as winning for both rather than as a race.
#
#   KEYS[1]  the workflow's steps hash
#   KEYS[2]  the workflow's pass hash
#   ARGV[1]  step name, the hash field to write
#   ARGV[2]  the step's result, encoded
#   ARGV[3]  the writing pass's fencing token
#   ARGV[4]  expiry for both hashes, in seconds
#   returns  {1 if this call's encoding is what is stored else 0, the encoding stored},
#            or a FENCED error if the pass is superseded
#
# The comparison is between *bare* encodings, which is what keeps a tie counting as a win
# for both passes: comparing the packed text instead would find the positions differ and
# report every tie as a loss.
RECORD = (
    PACKING
    + """
local fence = tonumber(redis.call('HGET', KEYS[2], 'token') or '0')
if tonumber(ARGV[3]) < fence then
  return redis.error_reply('FENCED pass ' .. ARGV[3] .. ' superseded by ' .. fence)
end
redis.call('HSETNX', KEYS[1], ARGV[1], pack_value(redis.call('HLEN', KEYS[1]), ARGV[2]))
redis.call('EXPIRE', KEYS[1], ARGV[4])
redis.call('EXPIRE', KEYS[2], ARGV[4])
local stored = bare_value(redis.call('HGET', KEYS[1], ARGV[1]))
if stored == ARGV[2] then return {1, stored} end
return {0, stored}
"""
)

# The same conditional write without the fence, for a value that comes from outside any
# pass. It is deliberately not gated on a claim: an approval must not fail because a
# worker happens to be mid-pass, and first-writer-wins is the whole guarantee it needs.
#
#   KEYS[1]  the workflow's steps hash
#   ARGV[1]  step name, the hash field to write
#   ARGV[2]  the value, encoded
#   ARGV[3]  expiry for the steps hash, in seconds
#   returns  the encoding stored after the call, this caller's or the earlier winner's
SUPPLY = (
    PACKING
    + """
local written = redis.call('HSETNX', KEYS[1], ARGV[1], pack_value(redis.call('HLEN', KEYS[1]), ARGV[2]))
redis.call('EXPIRE', KEYS[1], ARGV[3])
if written == 1 then return ARGV[2] end
return bare_value(redis.call('HGET', KEYS[1], ARGV[1]))
"""
)

# `SUPPLY` under a field this script mints instead of one the caller brought: the append
# that puts a message in a workflow's inbox.
#
# The position is `HLEN` again, and here it does double duty: it is both the field's place
# in the load order and the number the key is built from, which is what keeps the two from
# ever disagreeing. Everything that makes `HLEN` a sound position makes it a sound name as
# well (nothing here deletes a field, first-writer-wins never replaces one, and the hash
# expires whole), so the field this writes is known absent and `HSET` cannot land on top
# of anybody's message. Two appends racing is not a case: the whole script is one step.
#
#   KEYS[1]  the workflow's steps hash
#   ARGV[1]  the message, encoded
#   ARGV[2]  expiry for the steps hash, in seconds
#   returns  the field the message was filed under
APPEND = (
    PACKING
    + f"""
local position = redis.call('HLEN', KEYS[1])
local key = '{INBOX}' .. string.format('%0{INBOX_DIGITS}d', position)
redis.call('HSET', KEYS[1], key, pack_value(position, ARGV[1]))
redis.call('EXPIRE', KEYS[1], ARGV[2])
return key
"""
)

# Give the workflow back early, but keep the token. Zeroing the deadline rather than
# deleting the key is what preserves the fence across a clean handover: the next claim
# gets the next number up, so a pass that comes back from the dead still loses.
#
#   KEYS[1]  the workflow's pass hash
#   ARGV[1]  the releasing pass's fencing token
#   returns  0 always; a release by a superseded pass is a no-op, not an error
RELEASE = """
if tonumber(redis.call('HGET', KEYS[1], 'token') or '0') == tonumber(ARGV[1]) then
  redis.call('HSET', KEYS[1], 'until', 0)
end
return 0
"""


# The prefix `RECORD` and `TRANSACT` refuse a superseded pass with, and the one thing
# `record` and `transact` read out of an error before deciding it is not theirs.
FENCED = "FENCED "


def fenced(error: ResponseError) -> bool:
    """
    Whether this is one of our scripts refusing a superseded pass, or a real store error.

    Anchored at the start rather than searched for anywhere in the message, because the
    two are answered very differently: a fence tells a pass to stand down, and anything
    else (a `WRONGTYPE`, a script that will not load, a server out of memory) means the
    checkpoint is unusable and must reach the caller. redis-py surfaces a script's
    `error_reply` as the server's string verbatim, so the message *starts* with the code
    exactly as Redis's own do, and a recorded value carrying the word further in cannot
    be mistaken for one.
    """
    return str(error).startswith(FENCED)


@dataclass(frozen=True, slots=True)
class LuaEffect:
    """
    A piece of work this Redis can do, written as the Lua it would be on its own.

    The `Effect` type for `RedisCheckpointer`. `source` is an ordinary script body: it
    reads `KEYS` and `ARGV` from index 1 as if it were the only thing running, because
    `transact` splices it into a wrapper that supplies the fence check and the record and
    rebinds those two tables. It MUST return whatever the store's `CheckpointCodec`
    decodes, since what it returns is what the checkpoint comes to hold; under the
    default `JsonCodec` that means JSON text, and `cjson.encode` is the usual way to
    produce it. The wrapper puts the field's position in front of that on the way in and
    takes it off on the way out, so an effect neither writes a prefix nor ever sees one.
    The codec is the one place an effect has to know which one its store was
    built with, and it is unavoidable, because the encoding happens in the server where
    the Python codec cannot reach.

    Its `keys` MUST hash to the workflow's own slot, which on a single node is free and
    on a cluster means carrying the same `{id}` tag. That is not a quirk of the wrapper:
    it is what "the same datastore" reduces to once the datastore is partitioned, and a
    script spanning two slots is a distributed transaction wearing a local disguise.
    """

    source: str
    keys: tuple[str, ...] = ()
    # Whatever Redis will put on the wire, which is narrower than `object` on purpose:
    # a script argument that needs encoding is a codec decision, and this store already
    # made one for step results. An effect that wants to pass a structure encodes it.
    args: tuple[str | int | float | bytes, ...] = ()


# Perform an effect and record it in one commit, which is the whole of exactly-once and
# the only thing here that is more than bookkeeping. The order matters: fence first (a
# superseded pass must not act), then the *existence* check (a step already recorded must
# not run again, which is what makes a replay perform nothing at all), then the effect,
# then the record, all in one script, so no crash can land between the work and its
# receipt.
#
# The effect's own source is spliced in rather than loaded at run time, because
# `loadstring` is not available in Redis's sandbox and because a script's text is a value
# this app supplies rather than anything a request carries. Rebinding `KEYS` and `ARGV`
# inside the wrapper is what lets that source be written as if it ran alone.
#
#   KEYS[1]   the workflow's steps hash
#   KEYS[2]   the workflow's pass hash
#   KEYS[3..] the effect's own keys, which it sees as KEYS[1..]
#   ARGV[1]   step name, the hash field to write
#   ARGV[2]   the acting pass's fencing token
#   ARGV[3]   expiry for both hashes, in seconds
#   ARGV[4..] the effect's own arguments, which it sees as ARGV[1..]
#   returns   the encoding stored after the call, the effect's result or an earlier one,
#             or a FENCED error if the pass is superseded
TRANSACT = (
    PACKING
    + """
local outer_keys, outer_args = KEYS, ARGV
local fence = tonumber(redis.call('HGET', KEYS[2], 'token') or '0')
if tonumber(ARGV[2]) < fence then
  return redis.error_reply('FENCED pass ' .. ARGV[2] .. ' superseded by ' .. fence)
end
local already = redis.call('HGET', KEYS[1], ARGV[1])
if already then
  -- Re-armed on the way out, exactly as `RECORD` does, because a replay is the *only*
  -- thing that reaches this branch: a workflow whose passes all land on steps already
  -- transacted would otherwise write nothing, and expire while it was being worked on.
  redis.call('EXPIRE', KEYS[1], ARGV[3])
  redis.call('EXPIRE', KEYS[2], ARGV[3])
  return bare_value(already)
end
local result = (function()
  local KEYS = {unpack(outer_keys, 3)}
  local ARGV = {unpack(outer_args, 4)}
  %s
end)()
-- The position is read here rather than before the effect, where it would be the same
-- number: the field is known absent on this branch, so the count is this step's index.
-- What the effect returns is recorded as it came, with the prefix added on the way in and
-- taken off on the way out, so an effect never sees one and never writes one.
redis.call('HSET', KEYS[1], ARGV[1], pack_value(redis.call('HLEN', KEYS[1]), result))
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('EXPIRE', KEYS[2], ARGV[3])
return result
"""
)


@dataclass(frozen=True, slots=True)
class RedisCheckpointer:
    """
    A workflow's completed steps as one Redis hash, and its claim as another.

    The client MUST be built with `decode_responses=True`. That is this app's choice to
    make (it owns both ends of this hash), and making it once here is what keeps every
    read from carrying a bytes-or-text branch it would never take. It is also what fixes
    `codec` to a `CheckpointCodec[str]`: a hash field can hold bytes, but a client
    decoding every reply has already decided this store speaks text.

    `codec` is how a step's result becomes the encoding a hash field carries and comes
    back, and it defaults
    to the stdlib's JSON. Change it to widen what a step may return or to speed the
    encoding up; what it MUST keep is the round trip, since a resumed pass reads what it
    produced. A `LuaEffect` under `transact` has to agree with it, which is the one thing
    the type cannot check, because that encoding happens in the server.

    `namespace` keeps the workflow keys clear of whatever else shares the database, and
    `ttl` is the answer to the question a checkpoint store cannot dodge: these records
    outlive the process that wrote them, so something has to decide when a workflow is
    beyond resuming. Setting it on the hash rather than sweeping is what lets a finished
    or abandoned workflow expire on its own.

    It is re-armed only on a write, which makes it a bound on how long a workflow may
    *wait* as much as on how long a finished one is kept: a workflow suspended for longer
    than `ttl` writes nothing meanwhile, so its checkpoint expires while its entry in the
    sleeping set (which carries no expiry) survives, and the wakeup it eventually gets
    finds nothing recorded. So `ttl` MUST exceed the longest sleep or approval any
    workflow using this store can sit in.

    How durable a write actually is stops at what the server is configured for. It
    returns when Redis has accepted the write, which with the default snapshotting and
    asynchronous replication is not the same as surviving a failover, and nothing here
    asks for more with `WAIT`. `run_durably`'s reasoning about the window between an
    effect and its record assumes that gap is closed; closing it is this store's job, not
    the runner's.

    ## What a workflow id has to be

    A workflow id becomes *key structure* here rather than data, which is what gives it any
    constraints at all. They are not checked at run time, deliberately: the ordinary id is a
    UUID or a ULID and satisfies all of this without anyone thinking about it, so paying
    for a validation on every call to catch a caller who went out of their way would be
    the wrong trade. Enforce it where ids are minted if you need to.

    - It MUST NOT contain `{` or `}`. Those delimit the cluster hash tag, so an id
      carrying its own braces makes Redis take some prefix of it as the tag instead of
      the whole id. Both of a workflow's keys still agree on that prefix, so nothing
      breaks, but the slot is then chosen by an arbitrary fragment and keys stop
      spreading evenly across a cluster.
    - It SHOULD be bounded in length. Redis keys are held in memory and an id appears in
      two of them per workflow, plus any key an effect derives from `hash_key`.

    Both of those are about *this* store, and neither applies to a scheduler here, which
    holds an id as a stream field or a sorted-set member rather than in a key name. A SQL
    store binds it as a query parameter and so asks nothing of it at all, which is the
    tell: this is a property of building keys by interpolation, not of workflow ids. And
    nothing in `without-durability` derives one id from another, so these two are the
    whole list rather than the part of it one store happens to care about.
    """

    redis: Redis
    namespace: str = "workflow"
    ttl: timedelta = timedelta(days=1)
    codec: CheckpointCodec[str] = JSON
    # Registered once at construction: this precomputes each digest and holds the client,
    # so a call sends the digest and falls back to the source only when the server has
    # not seen it. One field per script rather than a tuple read by index, so the name a
    # call site uses is checked against the script it was registered with.
    take: AsyncScript = field(init=False, repr=False, compare=False)
    write: AsyncScript = field(init=False, repr=False, compare=False)
    offer: AsyncScript = field(init=False, repr=False, compare=False)
    file_away: AsyncScript = field(init=False, repr=False, compare=False)
    hand_back: AsyncScript = field(init=False, repr=False, compare=False)
    # The expiry every write re-arms, rendered once rather than per call: `ttl` is fixed
    # at construction and every `record`, `supply`, and `transact` sends it, so computing
    # it per step is work on the one path that scales with traffic.
    ttl_seconds: int = field(init=False, repr=False, compare=False)
    # One built script per distinct effect source, so a `transact` in a loop pays the
    # splice and the digest once rather than per step. Bounded by the number of effects
    # the application has written, which is a property of its source rather than of its
    # load; an app minting script bodies per request would want its own cache policy.
    transactions: dict[str, AsyncScript] = field(default_factory=dict, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        check_duration("a ttl", self.ttl)
        object.__setattr__(self, "take", self.redis.register_script(CLAIM))
        object.__setattr__(self, "write", self.redis.register_script(RECORD))
        object.__setattr__(self, "offer", self.redis.register_script(SUPPLY))
        object.__setattr__(self, "file_away", self.redis.register_script(APPEND))
        object.__setattr__(self, "hand_back", self.redis.register_script(RELEASE))
        object.__setattr__(self, "ttl_seconds", seconds(self.ttl))

    def hash_key(self, workflow: str) -> str:
        # The braces are Redis Cluster's hash tag: the slot comes from what is inside
        # them, so this key and `pass_key` share one and a script may touch both.
        return f"{self.namespace}:{{{workflow}}}"

    def pass_key(self, workflow: str) -> str:
        return f"{self.hash_key(workflow)}:pass"

    async def load(self, workflow: str) -> dict[str, object]:
        """
        Every step this workflow has recorded, in the order they were first recorded.

        One `HGETALL`, sorted here rather than by the server, because the order is carried
        by the fields themselves: `HGETALL` order is insertion order only while the hash is
        small enough to stay listpack-encoded, and is unspecified once it is not.

        `maxsplit=1` matters, since an encoded value has colons of its own. A field with no
        colon cannot occur, so there is no branch for one: every write path packs, and
        `int` raising is the right answer if that ever stops being true.
        """
        # The cast *is* `decode_responses=True`: redis-py types every read as
        # bytes-or-text because the flag is a runtime choice its types cannot see.
        recorded = cast(dict[str, str], await self.redis.hgetall(self.hash_key(workflow)))
        packed = [(field, *value.split(":", 1)) for field, value in recorded.items()]
        packed.sort(key=lambda field: int(field[1]))
        return {field: self.codec.decode(encoded) for field, _position, encoded in packed}

    async def claim(self, workflow: str, lease: timedelta) -> Pass | None:
        token = await self.take(
            keys=[self.pass_key(workflow)],
            args=[milliseconds(lease), self.ttl_seconds],
        )
        if token is None:
            return None
        return Pass(workflow=workflow, token=int(cast(int, token)))

    async def record(self, holder: Pass, key: str, value: object) -> Recorded:
        try:
            first, stored = cast(
                tuple[int, str],
                await self.write(
                    keys=[self.hash_key(holder.workflow), self.pass_key(holder.workflow)],
                    args=[key, self.codec.encode(value), holder.token, self.ttl_seconds],
                ),
            )
        except ResponseError as error:
            if not fenced(error):
                raise
            raise Fenced(f"{holder.workflow!r} moved on while this pass held it: {error}") from error
        return Recorded(value=self.codec.decode(stored), first=bool(first))

    def transaction(self, source: str) -> AsyncScript:
        """
        The wrapper script for one effect body, spliced and digested once.

        It cannot be built at construction, because it is the *effect* that decides the
        body. What it can do is build each one only the first time it sees it: the splice
        and the SHA are pure functions of the source, and an application's effects are
        written in its source rather than derived from a request, so the set is small.
        """
        built = self.transactions.get(source)
        if built is None:
            built = self.redis.register_script(TRANSACT % source)
            self.transactions[source] = built
        return built

    async def transact(self, holder: Pass, key: str, effect: LuaEffect) -> object:
        """
        Run `effect` and record it as `key`, in one script, so the step happens once.

        Whatever the effect returns becomes the step's encoding, so it has to already be in
        the shape this store's codec reads back (JSON text by default). The encoding happens
        in the server, which is exactly why it cannot be the codec's job. The script puts
        the field's position in front of it on the way in and takes it off on the way out,
        so an effect neither writes that prefix nor sees one.
        """
        try:
            stored = await self.transaction(effect.source)(
                keys=[self.hash_key(holder.workflow), self.pass_key(holder.workflow), *effect.keys],
                args=[key, holder.token, self.ttl_seconds, *effect.args],
            )
        except ResponseError as error:
            if not fenced(error):
                raise
            raise Fenced(f"{holder.workflow!r} moved on while this pass held it: {error}") from error
        return self.codec.decode(cast(str, stored))

    async def supply(self, workflow: str, key: str, value: object) -> object:
        stored = await self.offer(
            keys=[self.hash_key(workflow)],
            args=[key, self.codec.encode(value), self.ttl_seconds],
        )
        return self.codec.decode(cast(str, stored))

    async def append(self, workflow: str, value: object) -> Entry:
        """
        File `value` in this workflow's inbox, under the field the script mints for it.

        The encoding goes out and comes back rather than being round-tripped through the
        store, which is the one place this differs from `supply` and is sound for the
        reason `supply`'s read-back is not: the field is known absent inside the script,
        so there is no earlier writer whose value could be what is stored. Decoding what
        was just encoded still runs, since that is what every other write here promises
        and what makes a value that cannot survive its own codec fail on the way in.
        """
        encoded = self.codec.encode(value)
        key = await self.file_away(keys=[self.hash_key(workflow)], args=[encoded, self.ttl_seconds])
        return Entry(key=cast(str, key), value=self.codec.decode(encoded))

    async def release(self, holder: Pass) -> None:
        await self.hand_back(keys=[self.pass_key(holder.workflow)], args=[holder.token])
