# without-integration

Not a real package: the aggregator where `without` and every plugin are imported
together and exercised as a whole. Each new plugin is added to this package's
dependencies so its interaction with the rest gets a home for tests.

It also hosts validation artifacts that are not meant to be distributed. `kv` is
a toy line-protocol key-value server (Redis-ish) built on `without`, proving the
contract supports long-lived processor state and request/response. It splits into
`kv.core` (the pure keyspace: parse a line, fold it into an immutable `Store`,
render a reply) and `kv.shell` (a generic line-server transport plus the wiring
that runs the core over it), a small demonstration that `without` is a principled
way to write an imperative shell.

`flags` is a stateless feature-flag service built on the `without-asgi` adapters.
It shows the FastAPI-shaped concerns (routing, a middleware, lifespan) as plain
`without` wiring, with handlers that read flags from a live `without-configmap`
`Context` at request time, so a ConfigMap reload reaches in-flight requests. It
splits into `flags.core` (pure: the `Flags` model and response rendering) and
`flags.app` (the ASGI app: a `Router` value, a header middleware, and the
config-watch held for the server's lifetime via `without-asgi`'s `with_lifespan`).
