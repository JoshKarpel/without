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
from without import compose, from_selector, from_sink
from without_logging import Level, at_least, capture


async def write(record):
    print(f"{record.timestamp:%H:%M:%S} {record.level_name} {record.message}")


pipeline = compose(from_selector(at_least(Level.WARNING)), from_sink(write))

async with capture(pipeline):          # attaches to the root logger for the block
    logging.getLogger("app").warning("disk almost full", extra={"free_pct": 3})
```

`capture` is the one impure piece: it activates a handler on the root logger,
turns the pushed records into a `Stream`, and runs your pipeline against them for
the life of the block. Everything upstream of the sink is pure and testable
without touching the logging machinery.

To write to a file without paying a thread hop per line, `offload(write_lines(...))`
runs a blocking writer on a single dedicated thread, fed by a queue. `write_lines`
takes strings, so rendering a `Record` to text is a `from_map` composed in front:

```python
from without import compose, from_map, from_selector
from without_logging import Level, at_least, offload, write_lines

async with offload(write_lines(path)) as writer:
    lines = compose(from_map(render), writer)   # Record -> str -> file
    async with capture(compose(from_selector(at_least(Level.WARNING)), lines)):
        ...  # WARNING+ records written to the file off the event loop
```

See the
[`without-logging` guide](https://without.help/guides/without-logging/)
(with the [API reference](https://without.help/reference/without_logging/))
for the design narrative: why stdlib becomes a one-way source, why filtering is
just the core `from_selector` builder, and where fan-out to multiple sinks slots
in.
