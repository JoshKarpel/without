# CLAUDE.md

## Start here

Before doing anything else, read [`PHILOSOPHY.md`](PHILOSOPHY.md) to get the
project's mindset. It rests on one idea: an ecosystem of thin layers with narrow
interfaces, where every boundary is a value you can hold, so a user meets one
altitude, descends when they need to, and can replace a layer without rewriting
the rest. It covers what that demands (progressive disclosure, components that
travel, a layer never deciding for the layer above, a dependency only where no
real choice exists, and naming a package for its contents rather than its
position in the graph). The stateful stream processor is the *vocabulary* those
layers speak (the narrow-waist bet, the stream/processor/context substrate,
lifespan as a variable), and the doc is explicit that it is high-leverage rather
than the point: `without-html` speaks none of it and `without-async` sits below
it. The craft principles the code leans on (values over places, parse don't
validate, functional core and imperative shell) are subordinate to both, and the
doc treats them as tools rather than as the philosophy. It states all of it as
the standard new work is measured against, and marks where the code does not yet
meet it, so it is the right frame for a design decision but not the authority on
what the code currently does.

Then read the per-package guide for the area you are touching. Each package owns
a directory under [`docs/`](docs) (`docs/<package>/`) holding all of its docs:
the guide is that directory's `index.md`, its API reference is the generated
`reference.md` beside it, and any deep-dive sub-pages live alongside. So: the
asyncio primitives (`without-async/index.md`), the
substrate (`without-streams/index.md`), the ASGI boundary (`without-asgi/index.md`, with the
Server-Sent Events format and its deployment notes in `without-asgi/sse.md`), the HTTP
server and client (`without-http/index.md`, with cookie handling, connection-close
security, the in-memory test clients, and the feature register against httpx,
aiohttp, niquests, and the ASGI servers split into `without-http/cookies.md`,
`without-http/security.md`, `without-http/testing.md`, and
`without-http/alternatives.md`), the opinionated router
(`without-web/index.md`), the command-line parser (`without-cli/index.md`),
HTML as values (`without-html/index.md`), the
config sources (`without-env/index.md`, `without-configmap/index.md`), the DAG
executor (`without-dag/index.md`), and durable workflows
(`without-durability/index.md`, with the store interface in
`without-durability/guarantees.md` and the comparison to Temporal and DBOS in
`without-durability/alternatives.md`, plus one page per store in
`without-durability-redis/`, `without-durability-postgres/`, and
`without-durability-sqlite/`). Those guides carry the design narrative;
each package's `README.md` is now a short orientation that links to them, and the
`integration` toys that exercise the whole stack live under `packages/integration`.

The guides, an API reference recovered from the source docstrings, and the
derived package dependency graph are published as a documentation site (see
[`mkdocs.yml`](mkdocs.yml) and the hand-written [`docs/hooks.py`](docs/hooks.py));
build it locally with `just docs` (serve) or `just docs-build` (strict build).

For why each package directory is underscored (`packages/without_html`) and carries
`__init__.py` markers, where a `conftest.py` may live, and what a new package has to
add, see [`docs/contributing/workspace-layout.md`](docs/contributing/workspace-layout.md).

For how the packages are versioned and shipped (lockstep versioning, distribution
naming, build-time dependency pinning, and the trusted-publishing setup), see
[`docs/contributing/releasing.md`](docs/contributing/releasing.md).

For mutation testing (running `just mutate`, telling a real test hole from an
equivalent mutant, and the known-equivalent survivors that are expected to
remain), see [`docs/contributing/mutation-testing.md`](docs/contributing/mutation-testing.md)
and the `mutation-testing` skill.
