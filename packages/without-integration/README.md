# without-integration

Not a real package: the aggregator where `without` and every plugin are imported
together and exercised as a whole. Each new plugin is added to this package's
dependencies so its interaction with the rest gets a home for tests.

It also hosts validation artifacts that are not meant to be distributed. `kv` is
a toy line-protocol key-value server (Redis-ish) built on `without`, proving the
contract supports long-lived processor state and request/response: a pure core
(parse a line, fold it into an immutable keyspace threaded by `from_scan`,
render a reply) under an asyncio TCP shell that multiplexes many connections
onto one shared-state processor.
