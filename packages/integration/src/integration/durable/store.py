# The one place that knows both Redis and JSON: the `Checkpoints` implementation the
# durable runner talks to. A workflow is one Redis hash, a completed step is one
# field in it, and `HSET`/`HGETALL` are the whole vocabulary, because the shape
# `CompiledGraph.stream` emits (a mapping of name to result) is already the shape a
# hash holds. Nothing above this module mentions Redis, and nothing in it mentions
# the workflow's domain.
#
# The codec is the app's boundary decision, not the framework's, and JSON is this
# app's: it is what makes a checkpoint readable by an operator with `redis-cli` and
# by a service written in another language, at the cost of restricting node results
# to JSON-native values. Swapping in a pydantic `TypeAdapter` per node key, or a
# msgpack codec, changes this file alone.

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from typing import cast

from redis.asyncio import Redis


@dataclass(frozen=True, slots=True)
class RedisCheckpoints:
    """
    A workflow's completed steps, as one Redis hash per workflow.

    The client MUST be built with `decode_responses=True`. That is this app's choice
    to make (it owns both ends of this hash), and making it once here is what keeps
    every read from carrying a bytes-or-text branch it would never take.

    `namespace` keeps the workflow keys clear of whatever else shares the database,
    and `ttl` is the answer to the question a checkpoint store cannot dodge: these
    records outlive the process that wrote them, so something has to decide when a
    workflow is beyond resuming. It is set on the hash rather than swept, so a
    finished or abandoned workflow expires on its own and the store needs no
    control plane of its own.
    """

    redis: Redis
    namespace: str = "workflow"
    ttl: timedelta = timedelta(days=1)

    def hash_key(self, workflow: str) -> str:
        return f"{self.namespace}:{workflow}"

    async def load(self, workflow: str) -> dict[str, object]:
        # The cast *is* `decode_responses=True`: redis-py types every read as
        # bytes-or-text because the flag is a runtime choice its types cannot see.
        recorded = cast(dict[str, str], await self.redis.hgetall(self.hash_key(workflow)))
        return {field: json.loads(value) for field, value in recorded.items()}

    async def record(self, workflow: str, key: str, value: object) -> None:
        # One round trip, and the expiry is re-armed on every write, so a workflow
        # that keeps making progress keeps its checkpoint alive.
        async with self.redis.pipeline() as pipeline:
            pipeline.hset(self.hash_key(workflow), key, json.dumps(value))
            pipeline.expire(self.hash_key(workflow), self.ttl)
            await pipeline.execute()
