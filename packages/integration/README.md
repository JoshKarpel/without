# integration

Not a real package, and never published: the name deliberately sits outside the
`without*` family that the publish workflow globs, and a `Private :: Do Not
Upload` classifier makes PyPI reject it as a backstop. It is the aggregator where
`without` and every plugin are imported together and exercised as a whole. Each
new plugin is added to this package's dependencies so its interaction with the
rest gets a home for tests.

It also hosts validation artifacts that are not meant to be distributed. `kv` is
a toy line-protocol key-value server (Redis-ish) built on `without`, proving the
contract supports long-lived processor state and request/response. It splits into
`kv.core` (the pure keyspace: parse a line, fold it into an immutable `Store`,
render a reply) and `kv.shell` (a generic line-server transport plus the wiring
that runs the core over it), a small demonstration that `without` is a principled
way to write an imperative shell.

`transform` is a text-transform service built on the `without-asgi` adapters.
`POST /transform` reads the request body, uppercases/lowercases/title-cases it
per a `?mode=` query override, and caps the size, with the default mode and the
limit both read from a live `without-configmap` `Context` at request time, so a
ConfigMap reload reaches in-flight requests (`GET /modes` reports them). It shows
the FastAPI-shaped concerns (routing, a middleware, lifespan) as plain `without`
wiring, and splits into `transform.core` (pure: the `Settings` model, the `Mode`
transforms, and response rendering) and `transform.app` (the ASGI app: a `Router`
value with a middleware stack applied across every route, response-header and
access-logging layers among them, and the config-watch held for the server's
lifetime via `without-asgi`'s `make_asgi_app`).
