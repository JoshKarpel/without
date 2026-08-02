# without-durability-redis

[`without-durability`](https://pypi.org/project/without-durability/)'s two seams
over Redis: a workflow's completed steps as one hash, its claim as another, and a
queue of workflows that can run now.

```python
from redis.asyncio import Redis
from without_durability import SplitDurable
from without_durability_redis import RedisCheckpointer, RedisStreamScheduler

redis = Redis(host=..., decode_responses=True)   # this client owns both ends of every key
durable = SplitDurable(RedisCheckpointer(redis=redis), RedisStreamScheduler(redis=redis))
```

Every guarantee here is a small Lua script, and each is a script for the same
reason: it is only correct as *one* step. Checking whether a workflow is free and
taking it; checking a fencing token and applying the write it guards; testing
whether a key is recorded and reading back the winner. Split any of them into two
round trips and the gap between them is where the guarantee leaks.

`LuaEffect` is what this store can commit alongside a record, so a step whose
effect *is* a Redis write happens exactly once. That is worth stating plainly,
because the usual framing (that exactly-once needs Postgres) is wrong about why: a
Lua script is an atomic commit over Redis data, and the real constraint is that
you can only transact within one datastore.

On a cluster that constraint becomes a slot. A workflow's two keys are hash-tagged
(`workflow:{id}`) so a script may touch both, and an effect's keys must carry the
same tag. Redis enforces this rather than trusting it: declared keys spanning
slots are refused outright, and a script reaching a key another node owns dies
partway having written nothing.

Two queues ship, and the difference between them is the finding rather than a
choice you have to make carefully. `RedisStreamScheduler` is a stream read as a consumer
group beside a deadline-scored sorted set, which buys a blocking read so an idle
worker costs nothing. `RedisSetScheduler` is one sorted set scored by when each
workflow becomes visible, which makes the timer, the consumer group, the pending
list, and the trimmer all disappear, and costs the blocking read.

See the
[`without-durability-redis` guide](https://without.help/without-durability-redis/)
(with the [API reference](https://without.help/without-durability-redis/reference/))
for the scripts themselves, what each key holds and for how long, and the two
sharp edges (Redis's default persistence, and a TTL that can outlive a suspended
workflow).
