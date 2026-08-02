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

`durable` is a durable workflow mechanism built on `without-dag`, together with an
API server and a queue worker that make it a running service. It asks how much of
a workflow engine is left once the state is a checkpoint any process can read,
and the answer it arrives at is a dict lookup, a queue, and a handful of small
Lua scripts. Those scripts are the point: exclusion has to be enforced by
whatever holds the data, which is what Temporal builds a server for and DBOS
requires Postgres for, and here it is stated as requirements on the store seam.
The same reasoning gets exactly-once out of Redis, since a script is an atomic
commit over Redis data and the real constraint on co-committing an effect with
its checkpoint is that both live in one datastore, not that one of them is
Postgres. It is a validation artifact rather than a substitute for either:
[`durable/README.md`](src/integration/durable/README.md) is the design, where the
guarantee lives, the gaps that remain, and how it sits against those systems.

The Redis tests drive a real server rather than a fake: `just test` starts the
services in `compose.yaml` with podman, hands each published address to pytest
through the environment, and takes the stack down again from an exit trap. They
carry a `compose` mark and skip when that address is unset (no podman on the
machine, or pytest run directly), so `-m "not compose"` opts out up front.

`transform` is a text-transform service built on the `without-asgi` adapters.
`POST /transform` reads the request body, uppercases/lowercases/title-cases it
per a `?mode=` query override, and caps the size, with the default mode and the
limit both from a `without-configmap` `Context`. Each HTTP request and WebSocket
connection snapshots the config the moment it arrives, so a ConfigMap reload takes
effect on the next one rather than changing an open connection mid-flight (`GET
/modes` reports the current values, and a WebSocket `/stream` transforms each text
frame with its connect-time snapshot). It shows the
framework-shaped concerns (routing, middleware, lifespan) as plain `without`
wiring, and splits along the functional-core/imperative-shell line.
`transform.core` is pure and HTTP-unaware: the `TransformConfig` (just the default
`Mode`), the `Mode` transforms over decoded text, and the `UnknownMode` error it
raises rather than encoding a status. `transform.router` is a small
protocol-generic `Router` assembled from `without-asgi`'s routing tools, since the
adapters ship those but no router of their own. `transform.app` is the ASGI shell:
the HTTP and WebSocket handlers own the bytes (the size limit, the decode, the
query parse), call the core, and render its result or raised error as JSON; it
also holds the middleware stack applied across every route and the config-watch
kept for the server's lifetime via `without-asgi`'s `make_asgi_app`. Its config
splits in two, `Settings.transform` (the domain `TransformConfig`) and
`Settings.http` (the shell-only byte limit), so the core never sees a transport
concern. One middleware, `request_digest`, negotiates the
`http.response.trailers` extension (via `without-asgi`'s `extension` helper): it
returns the request-body digest in a response trailer when the server advertises
the extension, and falls back to a header (draining the body first so the header
is correct) when it does not.

`transform.cli` is a second shell over the *same* core, to make the portability
claim concrete. It reads lines from stdin and prints each one transformed,
drawing its config from a `without-env` `Context` rather than a ConfigMap and its
default mode and prompt prefix from the environment. The core and its
`TransformConfig` are unchanged between the two shells: only the I/O at the edge
and the config source differ, which is the narrow-waist payoff the project is
chasing.

`todos` is a user-facing exercise of the opinionated `without-web` router (where
`transform` hand-rolls one from `without-asgi`'s tools). It is the canonical
todo-list REST API, chosen because it hits the whole router design at once:
`t"/todos/{todo_id}"` is a typed path parameter (the same `todo_id` token names
the segment and is reused as the handler's `int` argument), `GET` vs `POST` on
`/todos` is method
dispatch (so a `PUT` is a `405` with `Allow`, not a `404`), `?done=` is a typed
`query_param` extractor, `/admin` is a `mount(...)` that bakes its prefix and auth
gate into the routes under it and `/legacy` an opaque `delegate(...)` (handed the
prefix-trimmed scope), `TodoNotFound`/`ValidationError` are
mapped to `404`/`422` by HTTP exception handlers, two endpoints read their input
*live* and fold it into a working list across the connection (`POST /todos/import`
as a `@post.stream` route over an NDJSON upload, and the `/todos/session`
websocket as the same fold kept open bidirectionally), and each `@get`/`@post`
handler is a plain function of typed values whose extractors also supply the
OpenAPI, so `todos_openapi()` merges the router's path/method half with each
endpoint's body/query/response half. `todos.core` is the pure, immutable
`TodoList`; `todos.app` is the `without-web` wiring, where `Router.dispatch` snaps
straight onto `make_asgi_app` because it already *is* an `HttpRouter`.
