# Releasing

How the `without` workspace is versioned and published. The mechanics live in
[`.github/workflows/publish.yml`](https://github.com/JoshKarpel/without/blob/main/.github/workflows/publish.yml) and
[`scripts/prepare_release.py`](https://github.com/JoshKarpel/without/blob/main/scripts/prepare_release.py); this document is the
rationale and the runbook.

## Versioning: lockstep

The whole workspace ships at **one shared version**, taken from the release tag.
Every `without*` distribution bumps together and depends on its siblings at that
exact version. The packages are a tightly coupled substrate (a core interface
plus plugins that speak it), so a single version is simpler to reason about than
independent cadences, and it matches the single workspace-wide
[`CHANGELOG.md`](https://github.com/JoshKarpel/without/blob/main/CHANGELOG.md).

The tradeoff accepted: a plugin with no real changes still gets a version bump
on every release. That is cheap; drifting, individually-versioned intra-workspace
bounds are not.

## Distribution names

The publishable set is exactly the `packages/without*` workspace members. That
glob is the single source of truth (the publish workflow, the release stamper,
and the bootstrap script all derive it), so this document does not enumerate the
packages.

- Every `without*` member's distribution name matches its import name (with
  `-`/`_` normalization), so no member needs a `[tool.uv.build-backend]
  module-name` override. No distribution claims the bare `without` name: it is
  the project, not a package.
- **`integration`** is never published: its name sits outside the `without*`
  family the glob selects, and its `Private :: Do Not Upload` classifier makes
  PyPI reject an upload if it ever slips through.

## Intra-workspace dependencies are pinned at build

A built wheel strips `[tool.uv.sources]`, so a sibling dependency declared as a
bare name (`without-streams`) would ship with **no version bound** and could resolve
against an incompatible release. Before building,
[`scripts/prepare_release.py`](https://github.com/JoshKarpel/without/blob/main/scripts/prepare_release.py) rewrites each
publishable member's own version and pins its sibling dependencies to
`== <version>`, so every wheel requires exactly the siblings it was built
against. These edits are made only in the workflow's ephemeral checkout; they are
never committed, so the dev workspace keeps resolving siblings through the
workspace sources.

## The release process

1. Move the `## Unreleased` section of [`CHANGELOG.md`](https://github.com/JoshKarpel/without/blob/main/CHANGELOG.md) under a new
   `## <version>` heading and commit it.
2. Create a **GitHub Release** with tag `v<version>` (for example `v0.1.0`).
3. That fires [`publish.yml`](https://github.com/JoshKarpel/without/blob/main/.github/workflows/publish.yml), which derives the
   version from the tag, stamps and pins with `prepare_release.py`, builds every
   `without*` member, and uploads them with `uv publish` via trusted publishing.

No API token is stored anywhere: the workflow authenticates to PyPI over OIDC
(see below).

## Trusted publishing setup (one-time, per project)

PyPI trusted publishing is **many-to-many**: one workflow file can publish
multiple projects, and each project can trust multiple workflows. Configure the
**same** trusted publisher on **each** `without*` project on PyPI:

- Owner: `JoshKarpel`
- Repository: `without`
- Workflow filename: `publish.yml`
- Environment: `pypi`

At publish time PyPI mints one short-lived token scoped to every project whose
trusted publisher matches, so the single `uv publish` run uploads the whole
family. A **normal** trusted publisher (attached to a project that already
exists) freely shares that tuple across projects, so steady-state releases need
no further setup.

Caveats: the workflow referenced by a trusted publisher **cannot** be a reusable
workflow, and there is no per-organization publisher yet, so the registration
above is repeated once per project.

### First-time bootstrap

A project must **exist** before a normal trusted publisher can be attached to it,
and the pending-publisher shortcut does not scale to this monorepo: pending
publishers must be unique on `(owner, repo, workflow, environment)`, so the
`without*` projects sharing one workflow cannot all be pre-registered
([warehouse#16920](https://github.com/pypi/warehouse/issues/16920)).

So bootstrap the projects into existence first with the `just bootstrap-pypi`
recipe, then attach the trusted publisher above to each:

1. Create a short-lived **account-scoped** PyPI API token (Account settings →
   API tokens), and export it: `export UV_PUBLISH_TOKEN=pypi-...`.
2. Reserve the publishable `without*` members, naming each explicitly (the
   recipe confirms first, since it uploads to the real index). PyPI caps
   new-project creation at a few per day, so reserve them in batches over a few
   days rather than all at once:

   ```bash
   just bootstrap-pypi without-streams without-asgi
   ```

   It uploads an empty `0.0.0` placeholder for each named project that does not
   yet exist, skipping any already registered, so re-running to pick up where a
   batch left off is safe. To rehearse without uploading, call the script
   directly: `uv run --script scripts/bootstrap_pypi.py --test <names...>`
   (TestPyPI) or `--dry-run` (build only).
3. On each freshly created project, add the trusted publisher (owner
   `JoshKarpel`, repo `without`, workflow `publish.yml`, environment `pypi`).
4. Delete the account-scoped token. From here on, releases authenticate over
   OIDC with no stored secret; the first real release (`v0.1.0`+) supersedes the
   `0.0.0` placeholders.

The script is idempotent, so introducing a new package later is the same two
steps: `just bootstrap-pypi without-newthing` to reserve just that name, then
attach its trusted publisher.
