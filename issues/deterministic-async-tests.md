---
title: Make async tests deterministic and remove the tick() helper
labels: [without]
---

## Summary

Several async tests lean on timing rather than a deterministic signal, which makes
them flaky. The concrete symptom is two concurrency tests in
`packages/integration/tests/kv/test_shell.py` that pass locally but flake on CI.
The shared root cause is the `tick()` test helper: there is no deterministic
"the next update has landed" signal, so a test yields control and *hopes* a
background drain has run.

## Package(s)

`without` (the `tick()` helper lives in `without.testing`, and the `sample`
behavior edge is the mechanism it papers over), with the flaky tests in
`integration`.

## Notes

`tick()` is acknowledged as a stopgap: it yields the event loop to let a `sample`
drain run before the assertion, but that is a timing guess, not a guarantee, and
it is used across several suites (`test_wiring`, `test_configmap`,
`test_env_and_configmap`, `test_transform_app`). The fix is a deterministic
"await next update" signal a test can wait on, e.g. `sample` exposing an awaitable
that resolves once it has consumed and published the next value, so a test asserts
on a known state rather than racing a background task.

Goal: introduce that signal, rewrite the tick-based and any sleep/spin-based tests
(including the kv concurrency tests) on top of it, and delete `tick()`. Until then,
decide whether to quarantine the kv flakes so CI stays green for the merge.
