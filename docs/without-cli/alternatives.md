# Alternatives

This page compares `without-cli` against the three parsers a Python program
usually reaches for: [argparse](https://docs.python.org/3/library/argparse.html)
in the standard library, [click](https://click.palletsprojects.com/), and
[typer](https://typer.tiangolo.com/). It is built around one program written
four times, because a feature list hides the thing that actually decides the
experience: how much you type per parameter, what the type checker catches
before you run it, and where a shared resource can live.

Measurements below are against Python 3.14, click 8.4, and typer 0.27. Every
spelling is the shortest *equivalent* one, so a shorter spelling that also
changes behaviour (click's `--debug/--no-debug`, which adds a negation the
others do not have) is not used to win a row.

The structural difference to keep in mind while reading is what each library
treats as the declaration. argparse builds a mutable parser by calling methods
on it. click constructs parameter objects but attaches them to a function
through decorators and registration. typer derives everything by introspecting
a function *signature*, so an annotation is the only channel and it does five
jobs at once. Here a token is an ordinary value: `option(("-t", "--tag"),
many(STR))` is a thing you can name, share, and pass to `command`, and the
overload ladder ties it to the handler parameter it fills.

## The same program, four ways

A `todos` CLI with a counted `-v`, an `--endpoint` that falls back to the
environment, an `add` taking a positional and a repeatable `-t/--tag`, an `ls`
taking an int option, and a nested `db` group whose `--dsn` reaches only the
commands under it. All four talk to the same `Client`, whose `connect` is an
async context manager, because that is what a real CLI holds.

### without-cli

```python
verbose = count(("-v", "--verbose"), summary="Raise log level; repeat for more.")
endpoint = option(
    "--endpoint",
    default("http://localhost:8000", STR),
    sources=(FromEnv("TODOS_ENDPOINT"),),
    summary="Todos service base URL.",
)


@dataclass(frozen=True, slots=True)
class Session(Streams):
    client: Client
    verbosity: int


@dataclass(frozen=True, slots=True)
class Database:
    session: Session
    dsn: str


@command("add", argument("text", once(STR)), option(("-t", "--tag"), many(STR)), summary="Add a todo.")
async def add(session: Session, text: str, tags: tuple[str, ...]) -> int:
    session.stdout.write(f"{await session.client.create(text, tags)}\n")
    return 0


@command("ls", option("--limit", default(10, INT)), summary="List todos.")
async def ls(session: Session, limit: int) -> int:
    for todo in await session.client.list(limit):
        session.stdout.write(f"{todo}\n")
    return 0


@command("migrate", summary="Apply migrations.")
async def migrate(db: Database) -> int:
    db.session.stdout.write(f"migrated {db.dsn}\n")
    return 0


@asynccontextmanager
async def session(streams: Streams, verbosity: int, base: str) -> AsyncIterator[Session]:
    async with Client.connect(base) as client:
        yield Session(
            stdin=streams.stdin,
            stdout=streams.stdout,
            stderr=streams.stderr,
            client=client,
            verbosity=verbosity,
        )


@asynccontextmanager
async def database(session: Session, dsn: str) -> AsyncIterator[Database]:
    yield Database(session=session, dsn=dsn)


app = group(
    "todos",
    verbose,
    endpoint,
    state=session,
    commands=(
        add,
        ls,
        group("db", option("--dsn", once(STR)), state=database, commands=(migrate,)),
    ),
)

if __name__ == "__main__":
    raise SystemExit(run(app))
```

### click

```python
@click.group()
@click.option("-v", "--verbose", count=True, help="Raise log level; repeat for more.")
@click.option(
    "--endpoint",
    default="http://localhost:8000",
    envvar="TODOS_ENDPOINT",
    show_envvar=True,
    help="Todos service base URL.",
)
@click.pass_context
def todos(ctx: click.Context, verbose: int, endpoint: str) -> None:
    # The client cannot be opened here: `connect` is an async context manager and
    # the callback is sync, so entering it would need a loop that outlives this
    # function. Each leaf opens its own instead.
    ctx.obj = {"verbosity": verbose, "endpoint": endpoint}


@todos.command(help="Add a todo.")
@click.argument("text")
@click.option("-t", "--tag", "tags", multiple=True)
@click.pass_obj
def add(obj: dict[str, object], text: str, tags: tuple[str, ...]) -> None:
    async def go() -> None:
        base = obj["endpoint"]
        assert isinstance(base, str)
        async with Client.connect(base) as client:
            click.echo(await client.create(text, tags))

    asyncio.run(go())


@todos.command(help="List todos.")
@click.option("--limit", default=10)
@click.pass_obj
def ls(obj: dict[str, object], limit: int) -> None:
    async def go() -> None:
        base = obj["endpoint"]
        assert isinstance(base, str)
        async with Client.connect(base) as client:
            for todo in await client.list(limit):
                click.echo(todo)

    asyncio.run(go())


@todos.group()
@click.option("--dsn", required=True)
@click.pass_obj
def db(obj: dict[str, object], dsn: str) -> None:
    obj["dsn"] = dsn


@db.command(help="Apply migrations.")
@click.pass_obj
def migrate(obj: dict[str, object]) -> None:
    click.echo(f"migrated {obj['dsn']}")


if __name__ == "__main__":
    todos()
```

### typer

```python
app = typer.Typer()
db_app = typer.Typer()
app.add_typer(db_app, name="db")


@app.callback()
def todos(
    ctx: typer.Context,
    verbose: Annotated[int, typer.Option("-v", "--verbose", count=True, help="Raise log level; repeat for more.")] = 0,
    endpoint: Annotated[
        str, typer.Option(envvar="TODOS_ENDPOINT", show_envvar=True, help="Todos service base URL.")
    ] = "http://localhost:8000",
) -> None:
    ctx.obj = {"verbosity": verbose, "endpoint": endpoint}


@app.command(help="Add a todo.")
def add(
    ctx: typer.Context,
    text: str,
    tag: Annotated[list[str] | None, typer.Option("-t", "--tag")] = None,
) -> None:
    async def go() -> None:
        async with Client.connect(ctx.obj["endpoint"]) as client:
            typer.echo(await client.create(text, tuple(tag or ())))

    asyncio.run(go())


@app.command(help="List todos.")
def ls(ctx: typer.Context, limit: int = 10) -> None:
    async def go() -> None:
        async with Client.connect(ctx.obj["endpoint"]) as client:
            for todo in await client.list(limit):
                typer.echo(todo)

    asyncio.run(go())


@db_app.callback()
def db(ctx: typer.Context, dsn: Annotated[str, typer.Option()]) -> None:
    ctx.obj["dsn"] = dsn


@db_app.command(help="Apply migrations.")
def migrate(ctx: typer.Context) -> None:
    typer.echo(f"migrated {ctx.obj['dsn']}")


if __name__ == "__main__":
    app()
```

### argparse

```python
def build() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="todos")
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Raise log level; repeat for more.")
    parser.add_argument(
        "--endpoint",
        default=os.environ.get("TODOS_ENDPOINT", "http://localhost:8000"),
        help="Todos service base URL. [env: TODOS_ENDPOINT]",
    )
    subs = parser.add_subparsers(dest="command", required=True)

    add = subs.add_parser("add", help="Add a todo.")
    add.add_argument("text")
    add.add_argument("-t", "--tag", dest="tags", action="append", default=[])

    ls = subs.add_parser("ls", help="List todos.")
    ls.add_argument("--limit", type=int, default=10)

    db = subs.add_parser("db")
    db.add_argument("--dsn", required=True)
    db_subs = db.add_subparsers(dest="db_command", required=True)
    db_subs.add_parser("migrate", help="Apply migrations.")

    return parser


async def dispatch(args: argparse.Namespace, out: TextIO) -> int:
    if args.command == "db":
        out.write(f"migrated {args.dsn}\n")
        return 0
    async with Client.connect(args.endpoint) as client:
        if args.command == "add":
            out.write(f"{await client.create(args.text, tuple(args.tags))}\n")
            return 0
        for todo in await client.list(args.limit):
            out.write(f"{todo}\n")
        return 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(dispatch(build().parse_args(argv), sys.stdout))
```

The `without-cli` version is the longest file of the four, and two thirds of
what it spends the extra lines on are the `Session` and `Database` dataclasses.
That is the trade the rest of this page is about: those two types are what the
other three replace with an untyped `dict` and, in click's and typer's case, a
resource opened once per leaf instead of once per invocation.

argparse's `dispatch` is where its own cost lands. Every field on `Namespace` is
untyped, the subcommand arrives as a string to branch on, and the branch and the
declaration that produced it sit in different functions with nothing tying them
together.

## What one parameter costs

The shortest equivalent spelling of the same seven parameter kinds:

| | without-cli | click | typer | argparse |
|---|---|---|---|---|
| required `str` positional | `argument("text", once(STR))` | `@click.argument("text")` | `text: str` | `p.add_argument("text")` |
| `int` option with a default | `option("--limit", default(10, INT))` | `@click.option("--limit", default=10)` | `limit: int = 10` | `p.add_argument("--limit", type=int, default=10)` |
| boolean flag | `flag("--debug")` | `@click.option("--debug", is_flag=True)` | `debug: bool = False` | `p.add_argument("--debug", action="store_true")` |
| repeatable option, short and long | `option(("-t", "--tag"), many(STR))` | `@click.option("-t", "--tag", "tags", multiple=True)` | `tag: Annotated[list[str] \| None, typer.Option("-t", "--tag")] = None` | `p.add_argument("-t", "--tag", dest="tags", action="append", default=[])` |
| counted flag, short and long | `count(("-v", "--verbose"))` | `@click.option("-v", "--verbose", count=True)` | `verbose: Annotated[int, typer.Option("-v", "--verbose", count=True)] = 0` | `p.add_argument("-v", "--verbose", action="count", default=0)` |
| option with an environment fallback | `option("--endpoint", default("http://x", STR), sources=(FromEnv("E"),))` | `@click.option("--endpoint", default="http://x", envvar="E", show_envvar=True)` | `endpoint: Annotated[str, typer.Option(envvar="E", show_envvar=True)] = "http://x"` | `p.add_argument("--endpoint", default=os.environ.get("E", "http://x"))` |
| option with a secret-file fallback | `option("--token", once(STR), sources=(FromFile(Path("/run/secrets/t")),))` | read the file yourself | read the file yourself | read the file yourself |

Characters typed, over the six kinds all four can express:

| without-cli | click | typer | argparse |
|---|---|---|---|
| 208 | 269 | 264 | 315 |

typer is the shortest thing here and also the longest, which is the whole story
of deriving a parser from a signature. While the annotation carries no
configuration, `limit: int = 10` cannot be beaten. The moment a parameter needs
a short alias, a count, or repeatability, the configuration has nowhere to go
but `Annotated`, and the same two rows that `without-cli` writes in 34 and 26
characters cost typer 68 and 72. Rows one and two are where typer wins; rows
four and five are where it comes last.

Three rows are worth reading against their behaviour rather than their length.
The first is the price of one vocabulary: spelling a required positional
`once(STR)` rather than `STR` costs six characters and is what makes
`optional(STR)`, `default("x", STR)`, and `many(STR)` available on a positional
without a second function for each. Both click and typer get a `--no-debug`
negation from their boolean spelling, which `flag` does not have. And the last
row has no competition at all: reading a projected Kubernetes secret is a
`sources=` entry here and hand-written file handling everywhere else, validated
by a different code path from the one that validates the command line.

## What the type checker catches

Three mistakes, each a mismatch between what a command declares and what its
handler reads:

```python
# A: the token parses a str, the handler declares an int.
@command("add", argument("text", once(STR)), option(("-t", "--tag"), many(STR)))
async def add(streams: Streams, text: int, tags: tuple[str, ...]) -> int: ...


# B: the tokens are declared in the other order from the handler's parameters.
@command("swap", argument("count", once(INT)), argument("name", once(STR)))
async def swap(streams: Streams, name: str, count: int) -> int: ...


# C: a handler parameter with no token behind it.
@command("extra", argument("text", once(STR)))
async def extra(streams: Streams, text: str, missing: str) -> int: ...
```

`mypy --strict` reports all three, with no runtime introspection anywhere:

```text
probe.py:2: error: Argument 1 has incompatible type "Callable[[Streams, int, tuple[str, ...]], Coroutine[Any, Any, int]]"; expected "Callable[[Streams, str, tuple[str, ...]], Awaitable[int]]"  [arg-type]
probe.py:7: error: Argument 1 has incompatible type "Callable[[Streams, str, int], Coroutine[Any, Any, int]]"; expected "Callable[[Streams, int, str], Awaitable[int]]"  [arg-type]
probe.py:12: error: Argument 1 has incompatible type "Callable[[Streams, str, str], Coroutine[Any, Any, int]]"; expected "Callable[[Streams, str], Awaitable[int]]"  [arg-type]
```

The same mistakes written against click, typer, and argparse produce no errors
under the same checker. click's parameter declarations and its handler's
annotations are two independent statements, so an annotation there is a claim
rather than a check, and both survivable mistakes land at runtime:

```text
add   -> TypeError('can only concatenate str (not "int") to str')
extra -> TypeError("extra() missing 1 required positional argument: 'missing'")
```

typer cannot express mistakes A or B, because there is only one declaration for
the checker and the parser to disagree about. That is the real strength of
deriving from a signature, and it is why the interesting typer comparison is not
this one but the next.

argparse types nothing: `Namespace` attributes are dynamic, so `args.text + 1`
and `args.missing` both pass the checker and fail at runtime.

## Where a resource lives

This is the difference that survives every other row. A CLI usually has one
client, pool, or connection that every command under a group needs, built from
that group's own options.

Here that is the group's `state`: an async context manager taking its parent's
state and this level's parsed options, entered only when something beneath it is
selected. `todos db --dsn ... migrate` opens the database and `todos ls` never
touches it, the state arrives at the handler typed, and the chain `Streams ->
Session -> Database` is checked at assembly, so a command wanting a `Session`
cannot be placed under a group that builds something else.

Neither click nor typer has an equivalent, for the same mechanical reason:
the group callback is synchronous. click can hold a resource open for the
duration of a command with `Context.with_resource`, but its signature takes an
`AbstractContextManager`, so an async one needs a loop outliving the callback,
which means driving an `AsyncExitStack` by hand and closing it from
`Context.call_on_close`. Both versions above take the other way out, which is
what a real program does too: each leaf opens its own client and runs its own
`asyncio.run`. The invocation-scoped resource becomes a command-scoped one, and
what the callback *can* pass down goes through `ctx.obj`, an untyped mutable
mapping that a parent writes and a child fishes out.

argparse has no callback at all, so the resource and the dispatch both end up in
whatever function the author writes after `parse_args`.

## The register

| | without-cli | click | typer | argparse |
|---|---|---|---|---|
| Parser is a value, no registration | yes | parameters are values, groups register | no | no |
| Parse without exiting the process | [`parse_argv`](index.md#parsing-is-total-pure-and-finishes-before-anything-opens) returns an outcome | `main(standalone_mode=False)` | via `typer.main.get_command` | `parse_args` exits on error |
| Which flags are magic | [the shell's list](index.md#the-parser-stops-the-shell-decides), not the parser's | fixed in the library | fixed in the library | fixed in the library |
| Handler and declaration checked against each other | yes, statically | no | not applicable (one declaration) | no |
| Typed state built per group | [yes](index.md#the-shell-is-the-layer-above-the-root-so-there-is-no-root) | `ctx.obj`, untyped, sync only | `ctx.obj`, untyped, sync only | no |
| `async` commands | always | no; run a loop yourself | no; run a loop yourself | no |
| Streams injected rather than global | [yes](index.md#streams-are-injected) | `CliRunner` patches them for tests | same as click | no |
| Environment variable fallback | [`FromEnv`](index.md#sources-the-environment-and-secret-mounts-at-the-same-boundary) | `envvar=` | `envvar=` | hand-written |
| File (secret mount) fallback | [`FromFile`](index.md#sources-the-environment-and-secret-mounts-at-the-same-boundary) | no | no | no |
| Help text from the handler's docstring | yes, when `summary=` is omitted | yes | yes | no |
| Choice or enum values | `choice` over an `Enum` | `click.Choice` | `Enum` annotation | `choices=` |
| Bounded numbers, path existence checks | no; write a `Converter` | `IntRange`, `click.Path` | same as click | `type=` callable |
| Converter's own placeholder shown in usage | no; the option's name is used | yes | yes | `metavar=` |
| Shell completion | no | yes, per-shell eval | yes, plus `--install-completion` | no |
| `--version` | `version=` on a level, read by the shell | `version_option` | `--version` callback | `action="version"` |
| `--no-` negation for flags | no, by position | `--x/--no-x` | from a `bool` annotation | `BooleanOptionalAction` |
| Optional positional | `argument(name, optional(...))` | `required=False` | default value | `nargs="?"` |
| Prompts, progress bars, colour | no, by [position](index.md#help-is-a-value) | yes | yes, via rich | no |
| Runtime dependencies | none | none | click, rich, shellingham | none |

One row in that table is a `without-cli` limit rather than a position taken
deliberately. A `Converter` carries the `metavar` naming what it parses, and
`choice(Profile)` puts `[dev|prod]` there. That reaches the rejection message but
not the help, because an option's usage placeholder is derived from the option's
own name:

```text
Options:
  --profile PROFILE    [required]

app deploy: --profile: expected [dev|prod], got 'nope'
```

One declaration, two consumers, and they disagree, which is exactly what
[tokens](index.md#tokens-one-declaration-several-consumers) exist to prevent.
Both click and typer show the converter instead (`--limit INTEGER`,
`--profile [dev|prod]`); showing the option's own name is the argparse
convention.

Everything else in that table's lower half is in
[what is deliberately absent](index.md#what-is-deliberately-absent), where the
reasoning for each lives.

## What each one is better at

- **argparse** is in the standard library. That is the whole argument and it is
  often decisive. Nothing here or in click or typer is worth a dependency for a
  script with two flags.
- **click** is the mature one. Its parameter types, its testing runner, its
  completion, and its ecosystem are all things this package either lacks or has
  a thinner version of, and it takes no runtime dependency to provide them. What
  it entangles is the context: `Context.obj` is an ambient mutable place,
  `ParamType.convert(value, param, ctx)` takes the context so a converter is not
  a pure function, and registration mutates a group.
- **typer** is the shortest way to put a command line on a function that already
  exists, and its completion story is the best of the four. Its ceiling is that
  the annotation is the only channel: what the annotation cannot say, you cannot
  say, and its escape hatch, `typer.main.get_command(app)`, drops you into
  click's model rather than a lower rung of its own.
- **without-cli** is the one to reach for when commands share a typed resource,
  when values arrive from mounted files as well as the environment, or when you
  want the parser to be a value you can test, ship, and place in someone else's
  tree. It costs a longer file and a smaller feature set.
