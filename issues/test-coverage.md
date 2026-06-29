---
title: Push test coverage as close to 100% as possible
labels: [ci]
---

## Summary

Coverage is configured (`pytest-cov`, branch coverage, an `exclude_also` list) but
not measured in the default `just test` run and not gated. Drive line and branch
coverage toward 100%, closing each gap one of two ways: add a test for a genuine
untested edge case, or exclude a genuinely unreachable line via the coverage
config.

## Package(s)

Repo-wide (every package's tests, plus the coverage config in `pyproject.toml`).

## Notes

`--cov` is not in `addopts` today, so coverage is opt-in: run
`uv run pytest --cov` to get the report (`show_missing` and `skip_covered` are
already on). Work the missing-lines list package by package.

For each uncovered line, decide deliberately:

- **A real untested path** → add a focused test (one behavior per test, distinct
  non-default values, at the boundary the parser establishes). This is the
  default; prefer a test over an exclusion.
- **Genuinely unreachable** → exclude it rather than contriving a test. The
  `exclude_also` list already covers `assert_never(`, `if TYPE_CHECKING:`,
  `raise NotImplementedError`, `@abstractmethod`, and `...`; extend it for new
  always-unreachable patterns, or use a sparing `# pragma: no cover` on a
  one-off. Parse-don't-validate often makes a defensive branch genuinely
  unreachable through the public surface, which is exactly the case for an
  exclusion.

Once coverage is near 100%, wire `--cov` into the default test run and set a
`fail_under` threshold so it can't silently regress (a CI gate). Keep the
exclusion list honest: an excluded line is a claim that it *cannot* be hit, not a
shortcut around a hard-to-write test.
