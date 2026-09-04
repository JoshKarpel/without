# without-cli

Command-line parsing as values. A token is one declaration that is the parse,
the help entry, and the typed read at once; a command is a value you can pass
around; and parsing is a pure function from argv to an outcome, so nothing is
registered anywhere and nothing exits out from under you. See the
[`without_cli` API reference](../without-cli/reference.md) for the full surface.

```python
verbose = count(("-v", "--verbose"), summary="Raise log level; repeat for more.")
endpoint = option(
    "--endpoint",
    default("http://localhost:8000", STR),
    sources=(FromEnv("TODOS_ENDPOINT"),),
)


@dataclass(frozen=True, slots=True)
class Session(Streams):
    client: Client
    verbosity: int


@command("add", argument("text", once(STR)), option(("-t", "--tag"), many(STR)), summary="Add a todo.")
async def add(session: Session, text: str, tags: tuple[str, ...]) -> int:
    todo = await session.client.create(text, tags)
    session.stdout.write(f"{todo.id}\n")
    return 0


@asynccontextmanager
async def session(streams: Streams, level: int, base: str) -> AsyncIterator[Session]:
    async with Client.connect(base) as client:
        yield Session(
            stdin=streams.stdin,
            stdout=streams.stdout,
            stderr=streams.stderr,
            client=client,
            verbosity=level,
        )


app = group("todos", verbose, endpoint, state=session, commands=(add, ...))

if __name__ == "__main__":
    raise SystemExit(run(app))
```

## The bet: a parser is a value, not a decorator's side effect

The evidence for what follows, one program written four ways against `argparse`,
`click`, and `typer`, with what each parameter costs and what each type checker
catches, is on [alternatives](alternatives.md).

`click` has most of the right nouns. Its parameters really are objects you can
construct and share, `Command.main(standalone_mode=False)` really does hand the
value back instead of exiting, and its help formatter carries no styling
dependency at all. What it entangles is everything around them: `Context.obj` is
an ambient mutable place that parent callbacks write and children fish out
untyped, `ParamType.convert(value, param, ctx)` takes the context so a converter
is not a pure function, and registration mutates a group.

`typer` buys ergonomics by discarding the nouns. It derives the parser by
introspecting a *function signature*, so the annotation is the only channel and
it is doing five jobs at once (parse type, default, presence, help, completion).
That is the whole ceiling: what the annotation cannot say, you cannot say. Its
escape hatch, `typer.main.get_command(app)`, drops you into a different library
with a different model, which is a cliff rather than a descent. You cannot have
half a typer command.

So the shape here is the one `without-web` already uses for HTTP, because the
correspondence is nearly exact:

| HTTP | CLI |
|---|---|
| path segments | subcommand path |
| path parameters | positional arguments |
| query parameters | options and flags |
| headers | environment variables and files |
| request body | stdin |
| status and body | exit code and the streams |
| OpenAPI | `--help`, man pages, completions |
| lifespan state | what a group builds for the commands beneath it |

That last row is the one that pays. What typer needs `@app.callback()` plus
`ctx.obj` for is `without-web`'s `state` parameter, and the answer was already
written.

## Tokens: one declaration, several consumers

An `Extractor` is a pure `Args -> V` paired with the `Parameter` describing it.
The same value that parses `--tag` is the one usage renders and the one the
binder reads an arity from, so those three cannot disagree.

```python
tags = option(("-t", "--tag"), many(STR), summary="Repeatable.")
```

`argument` takes positionals, `option` takes a named value, and `flag` and
`count` take valueless switches (`-vvv` is `3`). Because a token is an ordinary
value, a `--verbose` shared across a dozen commands is a module-level name rather
than a decorator copied twelve times.

A **`Converter`** is a `str -> V` parser paired with the placeholder that names
it. `STR`, `INT`, `FLOAT`, `BOOL`, `UUID`, and `PATH` ship, and `choice(SomeEnum)`
builds one that accepts an enum's values and yields its members; an application
adds its own by constructing one, because there is no registry to register it in.
A converter raising `ValueError` *rejects*, which becomes a `Rejected` naming the
parameter rather than a traceback.

A **`Cardinality`** (`once`, `optional`, `default`, `many`) says how many values
a token takes *and* what they become. Keeping those together is what stops the
help text from drifting from the parser: `once(INT)` is the single place saying
the token is required, takes one value, and yields an `int`.

Positionals and options take that same vocabulary, which is why there is no
separate `rest` function and no separate optional-positional one:

```python
argument("source", once(STR))  # SOURCE
argument("target", optional(STR))  # [TARGET], a `str | None`
argument("port", default(80, INT))  # [PORT], an `int` either way
argument("paths", many(STR))  # [PATHS...]
```

Assignment is greedy and in declaration order, so a `many` argument is valid only
as the last positional and a required one may not follow an optional one; `command`
refuses either layout where it is written.

`into(make, *tokens)` combines several tokens into one that builds a typed
value, for when a command outgrows its twenty-token arity ceiling or genuinely
wants a model. It is the escape hatch, not the primary form: binding tokens
straight onto handler parameters is what keeps the common case short.

## Sources: the environment and secret mounts, at the same boundary

An option can name where else its value may come from, in precedence order:

```python
token = option(
    "--token",
    once(STR),
    sources=(FromFile(Path("/run/secrets/todos-token")), FromEnv("TODOS_TOKEN")),
)
```

The command line beats every source outright; failing that, the first source
holding a value wins. Because sources feed the *same* `cardinality.parse`, a
value from a Kubernetes secret mount and one typed at a shell are validated
identically, and `--help` annotates where each option can come from because that
is recovered from the same declaration.

`FromFile` strips trailing whitespace by default, since a projected secret almost
always carries a newline and a token with one on the end fails authentication
somewhere far away from the cause. A missing file is *absence*, not an error, so
an unmounted secret means "not configured" and the option's own cardinality
decides whether that is fatal.

Reading those files is the shell's job, not the parser's: `run` collects the
paths the tree names (`source_paths`), reads them, and hands the contents to
`parse_argv` as a value. So parsing stays pure and a test supplies a mapping
instead of a filesystem.

That means the _whole_ tree's mounts, on every invocation, including one that
selects a command sharing none of them: `todos status` reads the password the
`db` group declares. The paths are known before parsing, but which level the
command line selects is not, so reading only what an invocation needs would put
the filesystem back inside the parse. A path named by several options is read
once.

## Commands are values; assembly is explicit

`@command(name, *tokens)` returns an `Arm` and registers nothing, exactly as
`@get` returns a `Route`. Each token supplies one argument to the handler, after
the one every command receives: the state its enclosing group built. The overload
ladder ties those types, so an `argument("id", once(INT))` paired with a handler
expecting a `str` is a mypy error with no runtime introspection anywhere.

Because an arm carries its own name, parsing, usage, and behaviour, a package can
ship one and a consumer can place it anywhere in a tree in a single edit:

```python
def db_commands[T](name: str, state: ...) -> Arm[T]:
    return group(name, option("--dsn", once(STR)), state=state, commands=(migrate, vacuum))
```

## The shell is the layer above the root, so there is no root

A group's `state` is an async context manager taking its parent's state and its
own parsed options. It is entered only when something beneath it is selected, and
unwound when that command returns, so `todos db --dsn ... migrate` opens the
database and `todos status` never touches it.

**The top of a tree is an ordinary group, and its parent is the shell.** The shell
supplies `Streams`, so a program's root is an `Arm[Streams]` and `run` hands it
the streams exactly as a group hands its children what it built. There is no
`program` function and no root special case: one `group` covers both, and the
chain is `Streams -> Session -> Database` with nothing at either end that is a
different kind of thing.

Three things fall out of that, and they are why it is worth the one cost below:

- **A CLI with no shared resource declares no `state` at all**, and its commands
  receive the `Streams` directly. `async def greet(streams: Streams, name: str)`
  has no ignored parameter, where a separate root concept would have forced one.
- **`run` needs no state factory**, because it already holds the thing the root
  derives from.
- **A command takes exactly one context parameter**, not a streams parameter plus
  a state parameter.

The cost is real and lands on you: a state that wants to write output has to carry
the streams onward, either by extending `Streams` or by holding one.

```python
@dataclass(frozen=True, slots=True)
class Session(Streams):  # extend: `session.stdout` works directly
    client: Client


@dataclass(frozen=True, slots=True)
class Database:  # or hold: `db.session.stdout`
    session: Session
    dsn: str
```

Forgetting is not silent: a command under a state with no streams has no
`.stdout` to reach, which mypy reports at the handler. Build the derived state in
a `@classmethod` factory naming every field that crosses, rather than splatting,
so what carries through is visible at the point it is decided.

This is the piece that has no good equivalent in click or typer: a command gets a
live client it did not open and does not close, typed, with no ambient context
and no `ctx.call_on_close`.

The tie is checked. `U` is solved from the state factory's return *and* from the
arms beneath it, so a command wanting a `Session` cannot be assembled under a
group that builds something else, and two commands wanting different states
cannot be siblings. Both are static errors on a bare call, which is stronger than
`without-web` manages today: an unannotated `Router(routes=(...))` whose handlers
disagree joins to `Router[Any]` and the check goes quiet, so annotate the router
there.

A group with no `state` is pure namespacing: it passes its parent's state through
unchanged.

## Parsing is total, pure, and finishes before anything opens

`parse_argv(app, argv=..., env=..., files=..., answered=...)` returns
`Bound | Answered | Rejected` and never exits, prints, or raises for a bad command
line. Every input is a value, so the whole parser is testable with no process, no
`sys.argv`, no `os.environ`, and no filesystem.

Crucially, **every value is extracted at parse time**, not when the command runs.
So a `Bound` proves the whole invocation is valid, and a program never opens a
database for a command line that was never going to work. That ordering is why
extraction reads only `Args` and the state arrives as a handler parameter rather
than as a token: a `state()` token would force extraction to wait until after the
resource existed, and `Bound` would stop proving anything.

Options bind to the level they are spelled at. A level with subcommands stops
scanning at the first bare token, since that token is its subcommand's name, so
`todos --verbose db --dsn x migrate` is unambiguous without either level knowing
about the other's options. That is also why a group declares options but never
positionals, which `group` refuses at import.

`--` ends option parsing, `-abc` bundles short flags, `-tvalue` and `--tag=value`
both work, and `--help` is answered even when a required option is missing,
because you were asking how to spell it.

## Help is a value

`usage(path)` is a pure transform of the command tree into a `Usage`, merging
each level's own parameters with the ones its ancestors declared. `render` is
*one* rendering of that value, in plain text, with no colour, no width detection,
and no styling dependency. A markdown page, a man page, a completion script, or a
coloured terminal renderer are others, each chosen by whoever is doing the
rendering.

This is the direct answer to bundled `rich`: the styling library is never on the
path every program crosses, so its output quality becomes the application's
choice rather than the library's. `without-cli` depends on nothing.

## Streams are injected

`Streams` carries `stdin`, `stdout`, and `stderr`, and it is what the shell hands
the root of the tree (see above), so it reaches every command either directly or
through whatever its groups derived from it. Nothing here writes anything a
command did not write itself, and a test asserts on output by passing
`Streams.captured()` and reading the buffers, with no module global patched and no
subprocess run.

`stdin` is an `Iterable[str]` of chunks rather than a string, because input
arrives over time: a filter reading a pipe should see each line as it lands.
Iterating the real `sys.stdin` yields lines; a test supplies a list or a
generator and controls arrival order exactly. It is a consume-once *place* rather
than a parsed-once value, which is the same reason `without-web` passes an
inbound stream as an argument instead of making it an extractor. `lines` re-splits
arbitrary chunks on line boundaries for a command that means "per line".

`Writer` requires `flush`, because stdout is block-buffered down a pipe and a
long-running command's progress would otherwise appear all at once when it exits.
Only the command knows when its output should be visible.

Iterating `sys.stdin` blocks, which for most commands is right and for a command
doing concurrent work is not. That one wraps it in
[`stream_from_blocking`](../without-streams/index.md), which runs the iteration
on a worker thread and hands values across a bounded queue. Keeping the plain
iterable as the type here is what lets the common case pay nothing and
`without-cli` take no dependency for it.

## Commands are always async

A command is `async def` without exception, for the same reason a `without-web`
handler is: it must be able to `await` I/O, and a library that takes a user
function which might do I/O should not make that a special case. `run` is
synchronous and starts the one event loop this package ever starts, once a
`Bound` has proved there is something worth running, so `--help` and a usage
error never enter one.

## The shell is one function

```python
def main() -> int:
    return run(app)


if __name__ == "__main__":
    raise SystemExit(main())
```

`run` is the only place this package reads `sys.argv`, reads the environment,
touches the filesystem, or starts a loop, and each of those is an argument with a
real default. It *returns* rather than exits, so the caller keeps the
continuation.

Its policy is the obvious one, and all of it is here rather than in the parser:
help and a version to stdout with `0`, a bad
command line to stderr with `2`, otherwise the command's own code. An application
wanting different answers matches on `parse_argv`'s outcome itself, which costs
it `run` and nothing else.

## The parser stops; the shell decides

`--help` means nothing to `parse_argv`. It is `run` that names the conventional
spellings and says what each one does:

```python
ANSWERED = ("-h", "--help", "--version")

match parse_argv(app, argv=argv, answered=ANSWERED):
    case Answered(spelling) as answer if spelling in HELP:
        streams.stdout.write(render(answer.usage))
```

`answered` is the caller's list of spellings that should stop the scan and come
back as an `Answered` carrying the spelling met and the level it was addressed to.
Called without it, `parse_argv(app, argv=["--help"])` rejects `--help` as an
option nobody declared, which is the honest answer from a function that has no
opinion. So a program wanting `-?`, or a `help` subcommand, or `--license`, or
none of it, passes its own list and writes its own shell, and nothing below `run`
changes.

Stopping has to happen in the scan even though *deciding* does not, for two
reasons that are worth being explicit about, because they are what rules out
making these tokens. Only the scan knows whether a token is a flag or the value
of the option before it. And a token could not answer anyway: extraction runs
after the whole path is bound, so `todos db migrate --help` would have already
failed on the required `--dsn` you were asking how to spell.

`version` is per level, so `run` reads it off whichever level was addressed:

```python
app = group("todos", commands=(add, db), version="todos 1.4.2")
```

That is what lets `todos db --version` report the version of the package that
shipped the `db` arm and `todos --version` the application's. A level that
declares no version has not opted in, and `run` rejects the flag there; that
rejection is `run`'s rule, so `run` is what builds it. A level that declares an
*option* by one of those names keeps it, which is how a program takes `--version`
back for itself. No spelling has a standing precedence over another, since the
scan runs left to right and stops at the first one it meets.

## What is deliberately absent

- **No styling, prompts, progress bars, or spinners.** They belong beside this
  package rather than inside it, reachable because a command's output is bytes it
  writes to an injected stream.
- **No shell completion yet.** The structure to derive it from is here (`Node`
  carries every level's parameters and children), and candidates should be a
  value with the per-shell scripts as separate renderings, but none of it is
  written.
- **No abbreviation matching, no `--no-` negation, no interspersed parent
  options.** Each is a real convention and none is implemented.
- **A converter's own placeholder does not reach an option's usage line.**
  `--profile` shows as `--profile PROFILE` even when `choice(Profile)` names
  itself `[dev|prod]`, because an option's placeholder is derived from its name;
  the rejection message uses the converter's. Pass `metavar=` to override.
- **A rejection reports the leaf's usage**, even when the option that failed was
  declared by an ancestor, because extraction runs the whole path at once and
  does not record which level raised.
- **A state that wants to write must carry the streams onward**, by extending
  `Streams` or holding one. Nothing does that for you, though forgetting is a
  static error rather than a silent one.
