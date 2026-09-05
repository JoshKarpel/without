# without-cli

Command-line parsing as values. A token is one declaration that is the parse, the
help entry, and the typed read at once; a command is a value you can pass around
rather than a decorator's side effect; and parsing is a pure function from argv to
an outcome, so nothing is registered anywhere and nothing exits out from under you.

```python
@dataclass(frozen=True, slots=True)
class Session(Streams):
    client: Client


@command("add", argument("text", once(STR)), option(("-t", "--tag"), many(STR)), summary="Add a todo.")
async def add(session: Session, text: str, tags: tuple[str, ...]) -> int:
    todo = await session.client.create(text, tags)
    session.stdout.write(f"{todo.id}\n")
    return 0


app = group("todos", verbose, endpoint, state=session, commands=(add, show, db))

if __name__ == "__main__":
    raise SystemExit(run(app))
```

`state` is an async context manager, entered only when something beneath it is
selected, so every command receives a live client it did not open and does not
close, typed and checked, with no ambient context object. There is no separate
root concept: the top of a tree is an ordinary group whose parent is the shell,
which supplies the `Streams` it derives from, so a CLI with no shared resource
declares no `state` and its commands take that `Streams` directly. An option can name
where else its value may come from (`sources=(FromFile(...), FromEnv(...))`),
which covers environment variables and Kubernetes or Docker secret mounts through
the same validation the command line goes through.

Help is a `Usage` *value* that plain text is one rendering of, so no styling
library sits on the path every program crosses and this package depends on
nothing. The streams are injected, so a test asserts on output by passing
`Streams.captured()`, with no subprocess and no runner.

See the
[`without-cli` guide](https://without.help/without-cli/)
(with the [API reference](https://without.help/reference/without_cli/))
for the full surface, including what is deliberately absent, and
[alternatives](https://without.help/without-cli/alternatives/) for the same
program written against argparse, click, and typer.
