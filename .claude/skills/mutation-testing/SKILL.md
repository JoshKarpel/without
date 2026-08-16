---
name: mutation-testing
description: Run and act on mutation testing (mutmut) for the without packages — drive a package's surviving mutants to zero, tell a real test hole from an equivalent mutant, and write tests that kill mutants. Use when asked to mutation-test a package, kill survivors, improve a suite's mutation score, or understand why some mutants can't be killed here. Points to docs/contributing/mutation-testing.md as the source of truth on known gaps.
---

# Mutation testing the `without` packages

Mutation testing edits the source and re-runs the tests; a *survivor* (tests still pass) is either
a test hole or an *equivalent mutant* no test can catch. The goal is **zero non-equivalent
survivors** per package, with the equivalents documented.

**Read [`docs/contributing/mutation-testing.md`](../../../docs/contributing/mutation-testing.md) first** — it is the source of truth for how mutmut is
configured here, and for the known-equivalent survivors (with concrete examples) that are expected
to remain. Don't re-litigate those; recognize them and move on.

## Run it

```console
$ just mutate <package>            # e.g. just mutate without-dag — run every mutant
$ just mutate <package> results    # list survivors from the last run
$ just mutate <package> show <id>  # show one mutant's diff (needs a warm ./mutants cache)
```

A full run on a big package (`without-http`, `without-web`) is ~15 min. Small packages are seconds.

## Method for driving a package to zero

1. **Run** `just mutate <pkg>` and read the survivor count.
2. **Group survivors by function**: `just mutate <pkg> results` → strip the `__mutmut_N` suffix and
   `sort | uniq -c`. This shows where the holes cluster.
3. **Extract the diffs** for each survivor (`mutmut show <id>`) into per-module files while the
   `./mutants` cache is warm. This is slow (~1s/mutant); background it for a large set.
4. **Triage each survivor** into: killable (write a test), or equivalent (justify precisely — see
   `docs/contributing/mutation-testing.md` categories). Be rigorous: only call it equivalent if you can state exactly why no
   observable behavior changes. Probe empirically when unsure (does h2 lowercase that header? does
   `aclose()` run the finally?).
5. **Write tests** for the killable ones (patterns below), then delete
   `packages/<pkg>/mutants/mutmut-stats.json` (see the traps) and **re-run** `just mutate <pkg>` to
   confirm. The central re-run is the source of truth, not the test passing.

For a large residue, fanning out one subagent per source module (each owning a distinct test file,
so they don't collide) parallelizes step 4-5 well; verify centrally with one serial mutmut run.

## Writing tests that kill mutants

See the "Writing tests that kill mutants" section of [`docs/contributing/mutation-testing.md`](../../../docs/contributing/mutation-testing.md) for the
patterns (assert concrete output, anchor error matches with `^...$`, hit the exact boundary, distinct
non-default values, synchronize on real signals). One mutmut-specific caveat: a test that races
teardown can pass under `-n auto` yet fail mutmut's `-n0` baseline, so synchronize on a real signal
rather than a `sleep`/`yield`-and-hope.

## Traps (each of these cost real time here)

- **`# pragma: no mutate` placement.** It is honored only as a trailing comment on a *simple
  statement line*. It does **not** work on a continuation line inside a multi-line literal (extract
  the value to a named local first), and it cannot suppress a match-case drop at all (mutmut mutates
  the whole `Match` node; the pragma gate only fires on expressions). Keep the line ≤120 chars or
  `ruff format` wraps it and detaches the comment.
- **mutmut renumbers mutants every run.** After you edit source, `__mutmut_N` ids from a prior run
  no longer correspond. Never cross-reference survivor ids across runs; re-extract.
- **Don't clear the `./mutants` cache before you've extracted the diffs you need.** `mutmut show`
  reads it. Cleaning between packages (to keep pytest collection clean) wipes it.
- **Delete `mutants/mutmut-stats.json` after adding tests.** mutmut runs only the tests its stats
  map to each mutant's function, and it *loads* that map from disk instead of recollecting. A test
  written against a function nothing previously reached is therefore never run, so the mutant
  reports `survived` however good the test is. Extract the diffs you need first (the point above),
  then delete the stats file and re-run. Symptom: a mutant survives the central run even though
  breaking that line by hand makes your new test fail.
- **Don't run two mutmut jobs at once**, and don't run one alongside subagents that run pytest: the
  30s-per-test timeout can trip a server-startup test under CPU contention.
- **The `-n0` baseline is stricter than `just test`.** A test that passes under `-n auto` can fail
  mutmut's single-process baseline (async teardown timing). If it's a genuine mutmut-trampoline
  artifact (see `docs/contributing/mutation-testing.md`), mark it `@pytest.mark.no_mutation`; otherwise fix the race.

## Configuring what mutmut skips

Two knobs, both explained in [`docs/contributing/mutation-testing.md`](../../../docs/contributing/mutation-testing.md): add a `do_not_mutate_patterns`
regex in the `mutate` recipe for a *universally* unkillable line shape (like `assert_never`), or mark
an individual test `@pytest.mark.no_mutation` for a mutmut-trampoline baseline failure. Prefer
neither — a real killing test is always better.
