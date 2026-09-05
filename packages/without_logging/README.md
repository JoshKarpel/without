# without-logging

A `without` pipeline for logs. Logger calls (yours and every third-party
library's) ultimately produce a stream of records; this package parses each one
into an immutable `Record` value, lets you filter and enrich them as ordinary
processors, and drains the result into a sink you own.

The two stdlib logging pain points it targets: the impenetrable, mutable,
noun-heavy configuration, and the monolithic handlers that bundle unrelated
decisions (when to flush, how to rotate, how to format) into one object. Here a
record is a value, each stage is a `Processor`, and the sink is whatever you
compose.

```python
import logging

from without_streams import compose, from_selector, from_sink
from without_logging import Level, at_least, capture


async def write(record):
    print(f"{record.timestamp:%H:%M:%S} {record.level_name} {record.message}")


pipeline = compose(from_selector(at_least(Level.WARNING)), from_sink(write))

async with capture(pipeline):  # attaches to the root logger for the block
    logging.getLogger("app").warning("disk almost full", extra={"free_pct": 3})
```

`capture` is the one impure piece: it activates a handler on the root logger,
turns the pushed records into a `Stream`, and runs your pipeline against them for
the life of the block. Everything upstream of the sink is pure and testable
without touching the logging machinery.

To write to a file without paying a thread hop per line, `without-streams`'
`offload` runs a blocking writer on a single dedicated thread, fed by a queue
that delivers items in bursts (so the writer flushes when it catches up, with no
flush-frequency knob). Writers are
named by destination: `to_rotating_file` writes to a file (owning the byte count and
clock, rotating on any combination of size (`max_bytes`), a relative interval
(`max_age`), and absolute wall-clock boundaries (`schedule=at_times(...)`)), and
`to_stream` writes to a caller-owned text stream (`sys.stderr`, a socket) without
closing it. Both take strings (render a `Record` to text with a `from_map` in front):

```python
from datetime import timedelta

from without_streams import compose, from_map, from_selector
from without_streams import offload
from without_logging import Level, at_least, to_rotating_file

writer = to_rotating_file(lambda i, when: directory / f"app.{i}.log", max_bytes=64 << 20, max_age=timedelta(hours=1))
async with offload(writer) as sink:
    lines = compose(from_map(render), sink)  # Record -> str -> file
    async with capture(compose(from_selector(at_least(Level.WARNING)), lines)):
        ...  # WARNING+ records written off the event loop, rotated by size and time
```

See the
[`without-logging` guide](https://without.help/without-logging/)
(with the [API reference](https://without.help/reference/without_logging/))
for the design narrative: why stdlib becomes a one-way source, why filtering is
just the core `from_selector` builder, and where fan-out to multiple sinks slots
in.
