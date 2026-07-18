# Mutation Testing

Mutation testing measures whether the test suite actually *constrains* behavior:
[`mutmut`](https://mutmut.readthedocs.io/) systematically edits the source (an `if a` becomes
`if not a`, a `+` becomes `-`, a literal `1` becomes `2`) and re-runs the tests. A mutant that
tests still pass against is a *survivor*: a change to production behavior that nothing detects.
A survivor is either a hole in the tests or an *equivalent mutant* (an edit that cannot change
any observable behavior, so no test could ever catch it).

The goal is to drive each package to **zero non-equivalent survivors**: close every real hole
with a test, and be left only with equivalent mutants that no test could kill. This file is the
source of truth for which survivors are equivalent and *why*, so a run that finds them does not
mistake them for test holes.

Run it per package (it must run inside the package for the `src/` layout to resolve):

```console
$ just mutate without-dag            # run every mutant
$ just mutate without-dag results    # list survivors from the last run
$ just mutate without-dag browse     # interactive TUI
```

`just mutate-all` sweeps every package with the same interface (`just mutate-all` to run,
`just mutate-all results` to list survivors), printing each package under its own header and
a final ok/FAILED summary table.

The `mutate` recipe writes the per-run mutmut `setup.cfg`; its comments explain each setting
(why `-n0`, the `no_mutation` marker filter, the `assert_never` skip pattern).

A package whose source is pure pass-through generates no mutants at all (`without-env`, whose
`EnvContext` has no operators, literals, or branches to mutate). mutmut hardcodes `exit(1)` in
that case with no config knob to allow it, so the `mutate` recipe absorbs it: a run that mutates
zero files reports success rather than `FAILED`. A run that *does* build mutants but leaves them
uncovered still fails, since that is a genuine test hole.

## Equivalent-mutant categories

When a survivor is not a test hole, it falls into one of the categories below. Each is genuinely
unkillable (or unreachable by mutmut's own suppression), with a concrete example.

### Match-case drops

The category mutmut structurally cannot suppress. mutmut's `operator_match` mutates a `match` by
*dropping one `case` at a time*, applied to the whole `Match` node. Its pragma/pattern suppression
only fires on `BaseExpression` nodes, so **no `# pragma: no mutate` can suppress a dropped case**,
and neither can the `do_not_mutate_patterns` regex (also gated to expressions).

Dropping an *exhaustiveness* default is equivalent because the default is unreachable for valid
input. Every closed `match` ends this way:

```python
match event:
    case HttpResponseStart(...): ...
    case HttpResponseBody(...): ...
    case _ as unreachable:
        assert_never(unreachable)   # dropping this case: valid input still matches a real arm above
```

`assert_never` only runs if a value outside the type reaches it, which the type system forbids.
Dropping the arm changes behavior only for input that "cannot happen", so it is equivalent. (The
`assert_never` *expression* mutation — `assert_never(unreachable)` → `assert_never(None)` — is
suppressed by the recipe's skip pattern; only the whole-case *drop* survives.)

Dropping a *redundant* case is likewise equivalent when the fallthrough does the same thing: a
trailing case whose body is a no-op (a bare `continue` at the bottom of a loop) behaves identically
whether present or dropped. Prefer deleting such a case outright, so it falls through to a documented
comment and mutmut has nothing to drop, rather than leaving it as a survivor to explain here.

### `suppress()` of a subclass alongside its base

`without-http`'s HTTP/2 path guards several defensive operations with:

```python
with suppress(h2.exceptions.ProtocolError, h2.exceptions.StreamClosedError):
    ...
```

`StreamClosedError` **is a subclass of** `ProtocolError`, so `suppress(ProtocolError)` already
catches it. Mutations that drop `StreamClosedError` or replace either argument with `None` are
equivalent: the set of caught exceptions is unchanged. The variant that drops the *base* differs
only if a non-`StreamClosed` `ProtocolError` is raised inside — but these blocks wrap a stream
reset / bad-request send / data ack that only raises on an already-doomed stream, not
deterministically reachable from a test.

### Boolean / sentinel / sort-key equivalents

A value read only in a boolean context is equivalent under `True`/`False`/`None` swaps that
preserve truthiness:

```python
# without-web/router.py — multi_segment is only read as `not multi_segment`, so None == False
def _render_value(..., *, multi_segment: bool) -> str:
    if not multi_segment and "/" in rendered:   # mutant: multi_segment=False -> None, both falsy
        ...
```

A sort key is equivalent under any change that preserves the *ordering*, not the value:

```python
# without-web/trie.py — used only as `sorted(..., key=_param_precedence)`
def _param_precedence(item) -> int:
    return 1 if converter.name == "str" else 0   # mutant: 1 -> 2. Values are {0, str}; 0 < 2 orders
                                                 # identically to 0 < 1, so the sort result is unchanged
```

Others in this class: `authority = b"" -> None` when `authority` is only read as `if authority`;
`request_done = True -> None` / `more_body=False -> None` where the flag feeds only an `if`.

### Unobservable defensive-path mutations

Defensive code can carry mutations that produce no observable difference. From `without-http`'s
HTTP/2 server:

- **Log-message text.** `logger.warning(f"...") -> logger.warning(None)` inside a
  `# pragma: no cover` branch (untracked-stream handlers h2 rejects before they run). The log line
  changes; no behavior does, and there is no caplog assertion convention.
- **Header case.** `(b":status", ...) -> (b":STATUS", ...)` on an error response. h2's
  `normalize_outbound_headers` lowercases header names on the wire, so the emitted bytes are
  identical (verified empirically).
- **`writer.write(None)`.** Crashes the per-stream task, but the buffered response already sits in
  the shared `h2.Connection` and is re-flushed on the next `receive_data`, so the client still
  gets it (verified empirically).
- **Post-close / racing internal state.** `streams.pop(None, None)` leaks a finished stream into a
  dict that only gets a harmless `window.set()`; `events = [] -> None` crashes *after* the GOAWAY
  is written and the socket closed. Neither changes what the client observes, and whether the
  internal `TypeError` survives to teardown or is pre-empted by `CancelledError` is a race with no
  timing-free kill.
- **Unreachable defaults.** `next((v for n, v in headers if n == b":method"), b"")` — the default
  is unreachable because h2 rejects a request with no `:method` before this code runs.

### Killing test excluded by the trampoline

mutmut rewrites every function into a trampoline that dispatches to the original or a mutant. That
trampoline **does not run an async generator's `aclose()`-triggered `finally`**. A test that
asserts exactly that teardown fails the mutmut baseline while passing the real suite, so it is
marked `@pytest.mark.no_mutation` and excluded. A mutant whose *only* killing test is excluded then
survives — e.g. in `without-http`'s `_with_release`, `if not fully_read:` → `if fully_read:` is a
real behavior change the excluded test would otherwise catch.

## Writing tests that kill mutants

Patterns that generalize (the `mutation-testing` skill has the full method):

- **Assert concrete output, not that it "works".** A test that asserts an exact parsed value, the
  exact response bytes, or the exact raised message kills every field/keyword/literal mutation in
  one shot. `assert head.status == 500` kills the `:status` value mutant; asserting a whole frame
  dict kills every key mutation in it.
- **Anchor error-message matches.** `pytest.raises(match="at least one sink")` still matches the
  `XX`-wrapped string mutation of that message. Use `match=r"^tee requires at least one sink$"` so
  the exact text is load-bearing.
- **Hit the exact boundary.** `>= max_age` → `> max_age` only diverges at *equality*: drive the
  injected clock to exactly `opened + max_age`. `limit < 1` → `<= 1` only diverges at `limit == 1`.
  These are deterministic, never timing-based.
- **Use distinct non-default values.** A field set to `0`, `""`, or the first enum member can make
  a broken function pass by coincidence; give each field a different, non-default value so an
  argument swap surfaces.

## Considered and rejected: the type-checker filter

mutmut's [type-checker filter](https://mutmut.readthedocs.io/en/latest/#filter-generated-mutants-with-type-checker)
(`type_check_command`) runs mypy over the whole mutated tree once and marks any mutant that
produces a type error as caught, without running it against the suite. It is deliberately **not
enabled** here, for two reasons.

It removes none of the suppression machinery. Every `# pragma: no mutate` in this codebase guards
a mutation that is *type-valid but behavior-equivalent*: codec-name case (`"ascii"` → `"ASCII"`),
truthiness (`bool(msg.get(k, False))` → `None`), a value that equals a field default, a
timing-only buffer depth, `zip(strict=True)` where lengths are always equal. mypy sees no error in
any of these, so the pragmas stay. The filter only catches *type-invalid* mutations (assigning
`None` into a non-optional slot, `cast(T, x)` → `cast(None, x)`), a set almost disjoint from what
the pragmas suppress. It would auto-catch a few hand-documented equivalent survivors (the
`multi_segment=False` → `None` on a `bool` parameter, `writer.write(None)`), but that is a small,
package-specific win, not a reduction in pragmas.

It cannot run on `without` core at all. mutmut's trampoline rewrite of the `@overload` functions in
`wiring.py` makes mypy report "an overloaded function outside a stub file must have an
implementation" on a line the filter cannot map back to any mutant, so
`filter_mutants_with_type_checker` raises and aborts the whole package run. This fires even with
every pragma intact, so the filter can't be turned on uniformly through the shared `mutate` recipe.
