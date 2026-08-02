# CLAUDE.md

## Start here

Before doing anything else, read [`PHILOSOPHY.md`](PHILOSOPHY.md) to get the
project's mindset: the narrow-waist bet, the stream/processor/context substrate,
functional-core/imperative-shell, and values-over-places. It is the durable
rationale for why the code is shaped the way it is, and the right frame for any
new work.

Then read the per-package guide for the area you are touching. Each package owns
a directory under [`docs/`](docs) (`docs/<package>/`) holding all of its docs:
the guide is that directory's `index.md`, its API reference is the generated
`reference.md` beside it, and any deep-dive sub-pages live alongside. So: the
core (`without/index.md`), the ASGI boundary (`without-asgi/index.md`), the HTTP
server and client (`without-http/index.md`, with cookie handling split out into
`without-http/cookies.md`), the opinionated router (`without-web/index.md`), the
config sources (`without-env/index.md`, `without-configmap/index.md`), the DAG
executor (`without-dag/index.md`), and durable workflows
(`without-durability/index.md`, with one page per store in
`without-durability-redis/`, `without-durability-postgres/`, and
`without-durability-sqlite/`). Those guides carry the design narrative;
each package's `README.md` is now a short orientation that links to them, and the
`integration` toys that exercise the whole stack live under `packages/integration`.

The guides, an API reference recovered from the source docstrings, and the
derived package dependency graph are published as a documentation site (see
[`mkdocs.yml`](mkdocs.yml) and the hand-written [`docs/hooks.py`](docs/hooks.py));
build it locally with `just docs` (serve) or `just docs-build` (strict build).

For how the packages are versioned and shipped (lockstep versioning, the
`without-core` distribution name, build-time dependency pinning, and the trusted-
publishing setup), see [`docs/contributing/releasing.md`](docs/contributing/releasing.md).

For mutation testing (running `just mutate`, telling a real test hole from an
equivalent mutant, and the known-equivalent survivors that are expected to
remain), see [`docs/contributing/mutation-testing.md`](docs/contributing/mutation-testing.md)
and the `mutation-testing` skill.
