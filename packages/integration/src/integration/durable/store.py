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

from redis.asyncio import Redis
from without_dag import NodeKey


def node_key(field: bytes | str) -> NodeKey:
    return field.decode() if isinstance(field, bytes) else field


@dataclass(frozen=True, slots=True)
class RedisCheckpoints:
    """
    A workflow's completed steps, as one Redis hash per workflow.

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

    async def load(self, workflow: str) -> dict[NodeKey, object]:
        recorded: dict[bytes | str, bytes | str] = await self.redis.hgetall(self.hash_key(workflow))
        # Whether a field comes back as bytes or text is the injected client's
        # `decode_responses` setting, which is the app's to choose; `json.loads`
        # takes either, so only the node's name has to be normalized.
        return {node_key(field): json.loads(value) for field, value in recorded.items()}

    async def record(self, workflow: str, key: NodeKey, value: object) -> None:
        # One round trip, and the expiry is re-armed on every write, so a workflow
        # that keeps making progress keeps its checkpoint alive.
        async with self.redis.pipeline() as pipeline:
            pipeline.hset(self.hash_key(workflow), key, json.dumps(value))
            pipeline.expire(self.hash_key(workflow), self.ttl)
            await pipeline.execute()
