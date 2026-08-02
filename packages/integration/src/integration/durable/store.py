# The one place that knows both Redis and JSON: the `Checkpoints` implementation the
# durable runner talks to. A workflow is one Redis hash, a completed step is one
# field in it, and the vocabulary is small, because the shape `CompiledGraph.stream`
# emits (a mapping of name to result) is already the shape a hash holds. Nothing above
# this module mentions Redis, and nothing in it mentions the workflow's domain.
#
# The codec is the app's boundary decision, not the framework's, and JSON is this
# app's: it is what makes a checkpoint readable by an operator with `redis-cli` and
# by a service written in another language, at the cost of restricting node results
# to JSON-native values. Swapping in a pydantic `TypeAdapter` per node key, or a
# msgpack codec, changes this file alone.
#
# Three of the four writes here are Lua scripts, and each is a script for the same
# reason: what it does is only correct as *one* step. Checking whether a workflow is
# free and taking it, checking a fencing token and applying the write it guards,
# testing whether a key is already recorded and reading back the winner. Split any of
# those into two round trips and the gap between them is where the guarantee leaks.
# This is the whole of what Temporal's server and DBOS's Postgres are doing for their
# users, at the scale this app needs it: exclusion has to be enforced by whatever holds
# the data, because that is the only party that sees every writer.
#
# The keys are hash-tagged (`workflow:{id}`, `workflow:{id}:pass`) so that the tagged
# id decides the slot and a workflow's two keys always land on the same one. Without
# that, `record` (which touches both) is a cross-slot command that Redis Cluster
# refuses, so the tag is what keeps this correct on more than a single node.

from __future__ import annotations

import json
from dataclasses import dataclass
from dataclasses import field
from datetime import timedelta
from typing import cast

from redis.asyncio import Redis
from redis.commands.core import AsyncScript
from redis.exceptions import ResponseError

from integration.durable.shell import Fenced
from integration.durable.shell import Pass

# Take the workflow if nobody holds it, and stamp the taking with a number that only
# ever goes up. `HINCRBY` is what makes the token a *fence* rather than a name: it is
# the store, not the claimant, that decides the ordering, so two processes cannot mint
# the same one. The expiry is read from the server's own clock rather than a caller's,
# because a lease compared against the claimant's clock is only as good as the agreement
# between the two, which is exactly what fails when a machine is unhealthy enough to
# stall mid-pass.
CLAIM = """
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local held_until = tonumber(redis.call('HGET', KEYS[1], 'until') or '0')
if held_until > now_ms then return nil end
local token = redis.call('HINCRBY', KEYS[1], 'token', 1)
redis.call('HSET', KEYS[1], 'until', now_ms + tonumber(ARGV[1]))
redis.call('EXPIRE', KEYS[1], ARGV[2])
return token
"""

# The fenced, conditional write. Refuse anything from a superseded pass, never overwrite
# a step that is already recorded, and hand back whatever is stored once the dust
# settles, so a caller that lost the race learns the winner's value instead of carrying
# on with its own.
RECORD = """
local fence = tonumber(redis.call('HGET', KEYS[2], 'token') or '0')
if tonumber(ARGV[3]) < fence then
  return redis.error_reply('FENCED pass ' .. ARGV[3] .. ' superseded by ' .. fence)
end
local written = redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[4])
redis.call('EXPIRE', KEYS[2], ARGV[4])
if written == 1 then return ARGV[2] end
return redis.call('HGET', KEYS[1], ARGV[1])
"""

# The same conditional write without the fence, for a value that comes from outside any
# pass. It is deliberately not gated on a claim: an approval must not fail because a
# worker happens to be mid-pass, and first-writer-wins is the whole guarantee it needs.
SUPPLY = """
local written = redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
if written == 1 then return ARGV[2] end
return redis.call('HGET', KEYS[1], ARGV[1])
"""

# Give the workflow back early, but keep the token. Zeroing the deadline rather than
# deleting the key is what preserves the fence across a clean handover: the next claim
# gets the next number up, so a pass that comes back from the dead still loses.
RELEASE = """
if tonumber(redis.call('HGET', KEYS[1], 'token') or '0') == tonumber(ARGV[1]) then
  redis.call('HSET', KEYS[1], 'until', 0)
end
return 0
"""


@dataclass(frozen=True, slots=True)
class RedisCheckpoints:
    """
    A workflow's completed steps as one Redis hash, and its claim as another.

    The client MUST be built with `decode_responses=True`. That is this app's choice
    to make (it owns both ends of this hash), and making it once here is what keeps
    every read from carrying a bytes-or-text branch it would never take.

    `namespace` keeps the workflow keys clear of whatever else shares the database,
    and `ttl` is the answer to the question a checkpoint store cannot dodge: these
    records outlive the process that wrote them, so something has to decide when a
    workflow is beyond resuming. It is set on the hash rather than swept, so a
    finished or abandoned workflow expires on its own and the store needs no
    control plane of its own.

    It is re-armed only on a write, which makes it a bound on how long a workflow may
    *wait* as much as on how long a finished one is kept: a workflow suspended for
    longer than `ttl` writes nothing meanwhile, so its checkpoint expires while its
    entry in the sleeping set (which carries no expiry) survives, and the wakeup it
    eventually gets finds nothing recorded. So `ttl` MUST exceed the longest sleep or
    approval any workflow using this store can sit in.

    How durable a write actually is stops at what the server is configured for. It
    returns when Redis has accepted the write, which with the default snapshotting and
    asynchronous replication is not the same as surviving a failover, and nothing here
    asks for more with `WAIT`. `run_durably`'s reasoning about the window between an
    effect and its record assumes that gap is closed; closing it is this store's job,
    not the runner's.
    """

    redis: Redis
    namespace: str = "workflow"
    ttl: timedelta = timedelta(days=1)
    # Registered once at construction, the way `RedisWakeups` registers its own: this
    # precomputes each digest and holds the client, so a call sends the digest and falls
    # back to the source only when the server has not seen it.
    scripts: tuple[AsyncScript, ...] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        registered = tuple(self.redis.register_script(source) for source in (CLAIM, RECORD, SUPPLY, RELEASE))
        object.__setattr__(self, "scripts", registered)

    @property
    def take(self) -> AsyncScript:
        return self.scripts[0]

    @property
    def write(self) -> AsyncScript:
        return self.scripts[1]

    @property
    def offer(self) -> AsyncScript:
        return self.scripts[2]

    @property
    def hand_back(self) -> AsyncScript:
        return self.scripts[3]

    def hash_key(self, workflow: str) -> str:
        # The braces are Redis Cluster's hash tag: the slot comes from what is inside
        # them, so this key and `pass_key` share one and a script may touch both.
        return f"{self.namespace}:{{{workflow}}}"

    def pass_key(self, workflow: str) -> str:
        return f"{self.hash_key(workflow)}:pass"

    async def load(self, workflow: str) -> dict[str, object]:
        # The cast *is* `decode_responses=True`: redis-py types every read as
        # bytes-or-text because the flag is a runtime choice its types cannot see.
        recorded = cast(dict[str, str], await self.redis.hgetall(self.hash_key(workflow)))
        return {field: json.loads(value) for field, value in recorded.items()}

    async def claim(self, workflow: str, lease: timedelta) -> Pass | None:
        token = await self.take(
            keys=[self.pass_key(workflow)],
            args=[int(lease.total_seconds() * 1000), int(self.ttl.total_seconds())],
        )
        if token is None:
            return None
        return Pass(workflow=workflow, token=int(cast(int, token)))

    async def record(self, holder: Pass, key: str, value: object) -> object:
        try:
            stored = await self.write(
                keys=[self.hash_key(holder.workflow), self.pass_key(holder.workflow)],
                args=[key, json.dumps(value), holder.token, int(self.ttl.total_seconds())],
            )
        except ResponseError as error:
            # The script's own refusal, which redis-py surfaces as the server's string.
            # Anything else is a real problem with the store and must not be swallowed.
            if "FENCED" not in str(error):
                raise
            raise Fenced(f"{holder.workflow!r} moved on while this pass held it: {error}") from error
        return json.loads(cast(str, stored))

    async def supply(self, workflow: str, key: str, value: object) -> object:
        stored = await self.offer(
            keys=[self.hash_key(workflow)],
            args=[key, json.dumps(value), int(self.ttl.total_seconds())],
        )
        return json.loads(cast(str, stored))

    async def release(self, holder: Pass) -> None:
        await self.hand_back(keys=[self.pass_key(holder.workflow)], args=[holder.token])
