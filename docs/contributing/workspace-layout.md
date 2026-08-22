# Workspace Layout

Why each package directory is named with underscores and carries `__init__.py` markers,
and what a new package has to add. This page covers the repository's own structure; for
how packages are versioned and published see
[releasing](releasing.md).

## Every package keeps its tests in a directory called `tests`

pytest and mypy both derive a module name for a file from where it sits on disk, and both
check the whole workspace in one pass. Fifteen packages each holding `tests/conftest.py`
is therefore fifteen files competing for one module name, and the tools say so:

```text
_pytest.pathlib.ImportPathMismatchError: ('tests.conftest', ...)
mypy: Duplicate module named "tests"
```

The name has to come from somewhere above the `tests` directory, which means the package
directory has to be part of it. Both tools stop climbing at a directory whose name is not
a valid Python identifier, so `packages/without-html` could never contribute one:

```text
mypy: without-html contains __init__.py but is not a valid Python package name
```

Hence `packages/without_html`, with `__init__.py` at `packages/`, at the package, and in
every tests directory. A test module is then `packages.without_html.tests.test_render`,
unique across the workspace, and a conftest works at any depth: `packages/conftest.py`
for fixtures every package shares, `packages/<package>/tests/conftest.py` for one
package, and one in a subdirectory for anything narrower.

The directory name is the *only* thing that changes. A package's distribution name comes
from its own `pyproject.toml`, so `without-html` is still `without-html` on PyPI, and the
markers sit beside `pyproject.toml` rather than inside `src/`, so no wheel contains one.

`packages/__init__.py` is load-bearing rather than tidy. Without it a package's tests are
named `without_html.tests.*`, which shadows the real `without_html`, and mypy reports the
confusion somewhere else entirely as a missing attribute on the package.

## Source keeps its own names

`explicit_package_bases` names each file relative to the longest matching entry in
`mypy_path`: the repository root covers the test tree, and each package's `src` covers
its source, so `render.py` stays `without_html.render` rather than becoming
`packages.without_html.src.without_html.render`. Without the `src` entries mypy reaches a
source file under both names at once and refuses the run.

`mypy_path` takes no globs, so the entries are generated from the workspace by
[`scripts/package_bases.py`](https://github.com/JoshKarpel/without/blob/main/scripts/package_bases.py)
into a `cog` block in `pyproject.toml`, and a pre-commit hook keeps them in sync.

## Adding a package

- Name the directory for the import package, not the distribution:
  `packages/without_thing` for `without-thing`.
- Add `packages/without_thing/__init__.py` and `packages/without_thing/tests/__init__.py`,
  plus one in any tests subdirectory. They stay empty.
- Run `pre-commit` (or `uv run cog -r -I scripts pyproject.toml`) so the new package's
  `src` reaches `mypy_path`. Skipping it fails the next type check with `Source file found
  twice under different module names`.

Within a package's tests, import a helper module relatively (`from .helpers import ...`).
A bare `from helpers import ...` only resolved because pytest put the tests directory on
`sys.path`, which it no longer does once the directory is a package.
