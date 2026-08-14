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

### Loop control the loop condition already decides

`continue` and `break` are interchangeable where the loop's own condition is already
false, which mutmut swaps freely. `without-dag`'s scheduler ends this way, in `drive`:

```python
while sorter.is_active():
    while ready and (limit is None or len(running) < limit):
        ...spawn each ready node...
    if not running:
        continue                    # mutant: continue -> break
    done = await completed.get()
```

Reaching that guard means the fill loop drained `ready` (it exits on a full `limit`
otherwise, and then `running` is non-empty), and every node the sorter had passed out
has been marked done, so `is_active()` is false and `continue` leaves the loop
immediately. `break` leaves it too. The guard exists for the run where a checkpoint
supplies a graph's last nodes: without it, nothing is in flight and `completed.get()`
waits forever.

Note the sibling mutation one loop up, `continue -> break` on the branch that skips an
already-supplied key, is *not* equivalent: it stops filling early, so a supplied node
delays its ready siblings by a whole completion round. That one is killed by
`test_drive_keeps_filling_past_a_node_whose_result_is_supplied`, which pins the
property that resuming from a checkpoint does not serialize the work that is left.

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
    return 1 if converter.name == "str" else 0  # mutant: 1 -> 2. Values are {0, str}; 0 < 2 orders
    # identically to 0 < 1, so the sort result is unchanged
```

Others in this class: `authority = b"" -> None` when `authority` is only read as `if authority`;
`request_done = True -> None` / `more_body=False -> None` where the flag feeds only an `if`;
`_dumps`'s `json.dumps(payload, allow_nan=False) -> allow_nan=None` in `without-asgi`, since the
stdlib encoder reads `allow_nan` for truthiness and `None` rejects `nan` exactly as `False` does
(verified empirically); and `_with_release`'s `fully_read = False -> None` in `without-http`, whose
value reaches only `if not fully_read` and a boolean conjunction.

`without-http`'s in-memory `_PipeTransport` contributes three more of the same shape:
`_closing`, `_paused`, and `_eof_sent` each start `False` and are read only as conditions
(`if self._closing`, `if not self._paused`, `if self._eof_sent or ...`), so `None` behaves
identically. They cannot take a `# pragma: no mutate`, because the *other* mutation of each line
(`False -> True`) is a real behavior change that the pipe tests do kill, and a pragma would blind
both.

### Codec-name case swaps

The wire codecs are named once per module (`_ASCII = "ascii"`, `_LATIN1 = "latin-1"`) rather than
repeated at each `.encode`/`.decode` call. mutmut's `operator_string` mutates that one literal three
ways: the `"XXasciiXX"` wrap is an invalid codec (`LookupError`) and the `.decode(None)` at each call
site is a `TypeError`, both killed by any test that exercises the path. Only the *case swap*
(`"ascii"` → `"ASCII"`) survives, because codec lookup is case-insensitive so the emitted bytes are
identical. That leaves one survivor per constant, in `h11_wire`, `h2_wire`, `ws_wire`, `server`, and
`client` (`without-http`) and `files` (`without-asgi`). Hoisting the name is what keeps this to one
documented survivor per module instead of a `# pragma: no mutate` on every call site (which would
also blind the killable `LookupError`/`TypeError` mutants).

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
- **`and` → `or` on the crash-to-500 guard.** `without-http`'s `_run_request` ends with
  `if not response_done and conn.our_state is h11.SEND_RESPONSE:`. The `or` mutant differs only when
  the app already sent response headers (state `SEND_BODY`, so the `is` clause is false): it then
  calls `_send_simple`, whose `suppress(h11.ProtocolError, OSError)` swallows the illegal
  second-response send, writing nothing. `_send_simple` can only emit when the state *is*
  `SEND_RESPONSE`, exactly when `and` also fires, so `or` never produces an observable 500 that `and`
  would not. (The `is` → `is not` and `not response_done` → `response_done` mutants on the same line
  are real behavior changes, killed by the crash tests.)

### Guards asyncio's own flow control already decides

`without-http`'s in-memory `_PipeTransport` translates a paused reader into a paused peer writer,
and both halves are guarded the same way:

```python
def stall_writes(self) -> None:
    if not self._paused and self._protocol is not None:  # mutant: and -> or
        self._paused = True
        self._protocol.pause_writing()
```

The guard is load-bearing (asyncio's `FlowControlMixin.pause_writing` asserts `not self._paused`,
and `resume_writing` asserts `self._paused`), but the `or` mutant is still equivalent, because
neither clause can be false when the other is. `_protocol` is set by `link` before any use, and
`StreamReader` only calls `pause_reading` when it is not already paused and `resume_reading` only
after a pause, so `stall_writes` never runs while paused and `resume_writes` never runs while
unpaused. `resume_writes`'s `self._paused = False -> None` is likewise truthiness-only; its
`False -> True` sibling *is* a real change (backpressure that engages once and never again) and is
killed by driving the pipe through two pause/resume rounds.

### Arguments a library resolves to the same value

`pipe` builds each endpoint from the running loop, which every constructor it passes it to would
have found on its own:

```python
reader = asyncio.StreamReader(limit=limit, loop=loop)  # mutants: loop=None, and the kwarg dropped
protocol = asyncio.StreamReaderProtocol(reader, loop=loop)  # same pair
writer = asyncio.StreamWriter(transport, protocol, reader, loop)  # mutant: reader -> None
```

`StreamReader` and `StreamReaderProtocol` fall back to `get_event_loop()`, which inside a running
coroutine is the same object that was passed, so all four loop mutants are equivalent. The
`reader -> None` mutant is equivalent for a different reason: `StreamWriter` reads `_reader` only to
re-raise a reader exception from `drain()`, and nothing on a pipe ever sets one.

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
a mutation that is *type-valid but behavior-equivalent*: truthiness (`bool(msg.get(k, False))` →
`None`), a value that equals a field default, a timing-only buffer depth, `zip(strict=True)` where
lengths are always equal. mypy sees no error in
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
