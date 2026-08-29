from __future__ import annotations

from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Generic
from typing import Never
from typing import TypeVar
from typing import cast
from typing import overload

from without_cli.sources import file_paths
from without_cli.tokens import AnyExtractor
from without_cli.tokens import Args
from without_cli.tokens import Extractor
from without_cli.tokens import Option
from without_cli.tokens import Parameter
from without_cli.tokens import Positional


@dataclass(frozen=True, slots=True)
class Node:
    """
    The description of one level of a command path: what it is called, what it
    parses, and what sits under it.

    Deliberately free of types and behaviour, because three consumers need the
    shape and none of them needs the state type: the binder reads `parameters` to
    know each option's arity, usage renders the whole tree, and completion walks
    it. Keeping the description separate from the resolver is also what lets a
    group nest arms whose state type differs from its own without an existential
    type Python cannot express.

    `version` is what this level answers `--version` with, and `None` means it
    answers nothing: the flag is then an unknown option like any other. Per level
    rather than per program, so an arm shipped by another package reports that
    package's version wherever it is mounted.
    """

    name: str
    summary: str = ""
    parameters: tuple[Parameter, ...] = ()
    children: tuple[Node, ...] = ()
    version: str | None = None

    @property
    def options(self) -> tuple[Option, ...]:
        return tuple(p for p in self.parameters if isinstance(p, Option))

    @property
    def positionals(self) -> tuple[Positional, ...]:
        return tuple(p for p in self.parameters if isinstance(p, Positional))

    def child(self, name: str) -> Node | None:
        return next((child for child in self.children if child.name == name), None)


def source_paths(node: Node) -> tuple[Path, ...]:
    """
    Every file the whole tree's options name, for the shell to read before parsing.

    Recovered from the same `sources` the options parse from, so a secret mount
    is declared exactly once and adding one changes nothing else.
    """
    here = file_paths(source for option in node.options for source in option.sources)
    return here + tuple(path for child in node.children for path in source_paths(child))


@dataclass(frozen=True, slots=True)
class Level:
    """One command-path level's bound values, as the binder produced them."""

    name: str
    args: Args


# What a handler hands back: the process exit code it wants, awaited. Named
# rather than spelled out at each use because a command is always `async` (it may
# always need to await I/O), so the shape never varies.
type Returned = Awaitable[int]

type Action[T] = Callable[[T], Returned]

# Contravariant in `T`: the state appears only as an argument to the `Action` an
# arm resolves to, so `Arm[Never]` is the supertype of every arm (the type of "an
# arm, whatever state it wants") and a command reading no state at all fits
# anywhere. The legacy `TypeVar` is needed because PEP 695's inferred variance
# treats a (frozen) dataclass field as invariant; the variance is sound here.
_T_contra = TypeVar("_T_contra", contravariant=True)

type _RawAction = Callable[[object], Awaitable[int]]


@dataclass(frozen=True, slots=True)
class Arm(Generic[_T_contra]):  # noqa: UP046 - PEP 695 infers a frozen dataclass field as invariant; the contravariant TypeVar is deliberate (see above)
    """
    One selectable command path: its description, and how to turn a bound
    invocation into the thing to run.

    An arm is a self-contained value. It carries its own name, tokens, usage, and
    behaviour, so a package can ship one and a consumer can place it anywhere in
    a tree without the package knowing where it landed, and nothing has to be
    registered anywhere for it to work.

    `resolve` runs at parse time and does *all* the parsing: it returns an
    `Action` that still needs the state its enclosing group builds, so by the
    time anything is opened every value has already been proven to parse.
    """

    node: Node
    resolve: Callable[[tuple[Level, ...]], Action[_T_contra]] = field(compare=False)


class DeclarationError(ValueError):
    """
    A command tree that cannot be assembled, raised where it is written.

    Authorship decides strictness: these are mistakes in code the application
    author owns (a variadic positional that is not last, two commands with one
    name), so they fail loudly at import rather than becoming a confusing parse
    at runtime.
    """


def _positional_layout(name: str, parameters: tuple[Parameter, ...]) -> None:
    """
    Refuse a positional layout the binder could not assign unambiguously.

    The binder hands bare tokens to positionals in declaration order and greedily,
    which is what both rules here protect. A `many` argument takes everything
    left, so anything after it would never be reached; and a required argument
    after an optional one is unsatisfiable, because the single token in
    `prog VALUE` would go to the optional and leave the required one empty.
    """
    positionals = [p for p in parameters if isinstance(p, Positional)]
    variadic = [index for index, p in enumerate(positionals) if p.variadic]
    if len(variadic) > 1:
        raise DeclarationError(f"command {name!r} declares more than one `many` argument")
    if variadic and variadic[0] != len(positionals) - 1:
        raise DeclarationError(f"command {name!r} declares a `many` argument that is not its last positional")
    optional = next((index for index, p in enumerate(positionals) if not p.required), None)
    if optional is not None and any(p.required for p in positionals[optional:]):
        raise DeclarationError(f"command {name!r} declares a required argument after an optional one")
    names = [p.name for p in positionals]
    if len(set(names)) != len(names):
        raise DeclarationError(f"command {name!r} declares two positionals with the same name")


def _first_line(docstring: str | None) -> str:
    """
    A handler's docstring reduced to the one line `--help` has room for.

    The first non-blank line, which is the summary line
    [PEP 257](https://peps.python.org/pep-0257/) already asks every docstring to
    open with, so this reads an existing convention rather than imposing a new
    one. Blank-line skipping is what makes a docstring opening after its quotes
    read the same as one opening on them.

    A first line that does not stand alone is not truncated or reflowed here,
    because guessing where a summary ends is worse than the author saying: that
    command passes `summary=` instead.
    """
    if docstring is None:
        return ""
    return next((line.strip() for line in docstring.splitlines() if line.strip()), "")


def _child_names(name: str, commands: tuple[Arm[Never], ...]) -> None:
    """Refuse a level whose children cannot be told apart, or that has none."""
    if not commands:
        raise DeclarationError(f"{name!r} declares no commands")
    names = [arm.node.name for arm in commands]
    if len(set(names)) != len(names):
        raise DeclarationError(f"{name!r} declares two commands with the same name")


def _no_positionals(name: str, parameters: tuple[Parameter, ...]) -> None:
    """
    Refuse a positional on a level that has subcommands.

    A bare token after a group is its subcommand's name, so a group that also
    wanted positionals would make the two indistinguishable. Options are fine:
    they are marked, and the binder stops at the first bare token.
    """
    if any(isinstance(p, Positional) for p in parameters):
        raise DeclarationError(f"{name!r} has subcommands, so it cannot also declare positional arguments")


# [[[cog import cog; from ladders import emit; cog.outl(emit("command")) ]]]
@overload
def command[T](
    name: str,
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T], Returned]], Arm[T]]: ...


@overload
def command[T, A](
    name: str,
    a: Extractor[A],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A], Returned]], Arm[T]]: ...


@overload
def command[T, A, B](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D, E](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D, E], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D, E, F](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D, E, F], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D, E, F, G](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D, E, F, G, H](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D, E, F, G, H, J](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D, E, F, G, H, J, K](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D, E, F, G, H, J, K, N](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K, N], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D, E, F, G, H, J, K, N, P](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D, E, F, G, H, J, K, N, P, Q](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    q: Extractor[Q],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P, Q], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    q: Extractor[Q],
    r: Extractor[R],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    q: Extractor[Q],
    r: Extractor[R],
    s: Extractor[S],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    q: Extractor[Q],
    r: Extractor[R],
    s: Extractor[S],
    v: Extractor[V],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    q: Extractor[Q],
    r: Extractor[R],
    s: Extractor[S],
    v: Extractor[V],
    w: Extractor[W],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    q: Extractor[Q],
    r: Extractor[R],
    s: Extractor[S],
    v: Extractor[V],
    w: Extractor[W],
    x: Extractor[X],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X, Y](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    q: Extractor[Q],
    r: Extractor[R],
    s: Extractor[S],
    v: Extractor[V],
    w: Extractor[W],
    x: Extractor[X],
    y: Extractor[Y],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X, Y], Returned]], Arm[T]]: ...


@overload
def command[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X, Y, Z](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    q: Extractor[Q],
    r: Extractor[R],
    s: Extractor[S],
    v: Extractor[V],
    w: Extractor[W],
    x: Extractor[X],
    y: Extractor[Y],
    z: Extractor[Z],
    /,
    *,
    summary: str = ...,
    version: str | None = ...,
) -> Callable[[Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X, Y, Z], Returned]], Arm[T]]: ...
# [[[end]]]
def command(
    name: str,
    *extractors: AnyExtractor,
    summary: str = "",
    version: str | None = None,
) -> Callable[[Callable[..., Returned]], Arm[Never]]:
    """
    Bind a handler to a name and a list of tokens, producing an `Arm` value.

    The decorator *returns* the arm rather than registering it, so assembly stays
    the explicit `commands=(...)` on a program or group and a command can be
    passed around, renamed, or shipped from a package that does not know where it
    will be mounted.

    Each token supplies one argument to the handler, in declaration order, after
    the two every command receives: the `Streams` to write to and the state its
    program built. The overloads tie the token types to those parameters, so a
    `argument("id", once(INT))` paired with a handler expecting a `str` is a mypy error
    with no runtime introspection anywhere.

    `summary` is the one line `--help` shows beside the command's name. Omitting
    it falls back to the first line of the handler's docstring, so a command
    documented for the next reader of the code is documented for its user too;
    passing it explicitly wins, for when those two readers want different words.

    `version`, when given, is what `--version` answers with here. Omitting it
    means this command has none and `--version` is an unknown option.
    """
    parameters = tuple(p for extractor in extractors for p in extractor.parameters)
    _positional_layout(name, parameters)

    def bind(fn: Callable[..., Returned]) -> Arm[Never]:
        def resolve(levels: tuple[Level, ...]) -> Action[Never]:
            # Every value is parsed here, at parse time: a rejection becomes a
            # `Rejected` before the program has opened anything at all.
            values = tuple(extractor.extract(levels[0].args) for extractor in extractors)

            async def action(state: object) -> int:
                return await fn(state, *values)

            return cast(Action[Never], action)

        described = summary or _first_line(fn.__doc__)
        node = Node(name=name, summary=described, parameters=parameters, version=version)
        return Arm(node=node, resolve=resolve)

    return bind


# [[[cog import cog; from ladders import emit; cog.outl(emit("group")) ]]]
@overload
def group[T, U](
    name: str,
    /,
    *,
    state: Callable[[T], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A](
    name: str,
    a: Extractor[A],
    /,
    *,
    state: Callable[[T, A], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    /,
    *,
    state: Callable[[T, A, B], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    /,
    *,
    state: Callable[[T, A, B, C], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    /,
    *,
    state: Callable[[T, A, B, C, D], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D, E](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    /,
    *,
    state: Callable[[T, A, B, C, D, E], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D, E, F](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    /,
    *,
    state: Callable[[T, A, B, C, D, E, F], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D, E, F, G](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    /,
    *,
    state: Callable[[T, A, B, C, D, E, F, G], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D, E, F, G, H](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    /,
    *,
    state: Callable[[T, A, B, C, D, E, F, G, H], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D, E, F, G, H, J](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    /,
    *,
    state: Callable[[T, A, B, C, D, E, F, G, H, J], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D, E, F, G, H, J, K](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    /,
    *,
    state: Callable[[T, A, B, C, D, E, F, G, H, J, K], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D, E, F, G, H, J, K, N](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    /,
    *,
    state: Callable[[T, A, B, C, D, E, F, G, H, J, K, N], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D, E, F, G, H, J, K, N, P](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    /,
    *,
    state: Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D, E, F, G, H, J, K, N, P, Q](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    q: Extractor[Q],
    /,
    *,
    state: Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P, Q], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D, E, F, G, H, J, K, N, P, Q, R](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    q: Extractor[Q],
    r: Extractor[R],
    /,
    *,
    state: Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    q: Extractor[Q],
    r: Extractor[R],
    s: Extractor[S],
    /,
    *,
    state: Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    q: Extractor[Q],
    r: Extractor[R],
    s: Extractor[S],
    v: Extractor[V],
    /,
    *,
    state: Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    q: Extractor[Q],
    r: Extractor[R],
    s: Extractor[S],
    v: Extractor[V],
    w: Extractor[W],
    /,
    *,
    state: Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    q: Extractor[Q],
    r: Extractor[R],
    s: Extractor[S],
    v: Extractor[V],
    w: Extractor[W],
    x: Extractor[X],
    /,
    *,
    state: Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X, Y](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    q: Extractor[Q],
    r: Extractor[R],
    s: Extractor[S],
    v: Extractor[V],
    w: Extractor[W],
    x: Extractor[X],
    y: Extractor[Y],
    /,
    *,
    state: Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X, Y], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...


@overload
def group[T, U, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X, Y, Z](
    name: str,
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    j: Extractor[J],
    k: Extractor[K],
    n: Extractor[N],
    p: Extractor[P],
    q: Extractor[Q],
    r: Extractor[R],
    s: Extractor[S],
    v: Extractor[V],
    w: Extractor[W],
    x: Extractor[X],
    y: Extractor[Y],
    z: Extractor[Z],
    /,
    *,
    state: Callable[[T, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X, Y, Z], AbstractAsyncContextManager[U]],
    commands: tuple[Arm[U], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...
# [[[end]]]
@overload
def group[T](
    name: str,
    /,
    *,
    commands: tuple[Arm[T], ...],
    summary: str = ...,
    version: str | None = ...,
) -> Arm[T]: ...
def group(
    name: str,
    *extractors: AnyExtractor,
    state: Callable[..., AbstractAsyncContextManager[object]] | None = None,
    commands: tuple[Arm[Never], ...],
    summary: str = "",
    version: str | None = None,
) -> Arm[Never]:
    """
    Gather arms under a name, deriving their state from its parent's and this
    level's own options.

    A group is where a resource lives: `state` is an async context manager taking
    the parent's state and this group's parsed options, entered only when one of
    its children is actually selected and unwound when that child returns. So
    `prog db --dsn ... migrate` opens the database, runs `migrate`, and closes
    it, while `prog status` never touches it.

    **There is no separate root.** A tree's top level is an ordinary group whose
    parent is the shell, which supplies `Streams`, so the root of a program is an
    `Arm[Streams]` and `run` hands it the streams the same way a group hands its
    children what it built. A CLI with no shared resource declares no `state` at
    all and its commands receive that `Streams` directly; one that has a resource
    builds a state carrying the streams onward, by subclassing `Streams` or by
    holding one.

    Omitting `state` makes the group pure namespacing: nothing is built and its
    children see exactly what it was given.

    A group declares options but never positionals: a bare token after a group is
    the name of its subcommand, so the two would be indistinguishable.

    `version`, when given, is what `--version` answers with at this level. It is
    per level rather than per program, so `prog --version` and `prog db --version`
    can report the version of whatever package shipped each of them.
    """
    return _nest(name, extractors, state, commands, summary, version)


def _nest(
    name: str,
    extractors: tuple[AnyExtractor, ...],
    state: Callable[..., AbstractAsyncContextManager[object]] | None,
    commands: tuple[Arm[Never], ...],
    summary: str,
    version: str | None,
) -> Arm[Never]:
    """The body of `group`, shared by its stateful and pure-namespacing forms."""
    parameters = tuple(p for extractor in extractors for p in extractor.parameters)
    _no_positionals(name, parameters)
    _child_names(name, commands)
    if state is None and extractors:
        raise DeclarationError(f"{name!r} declares options but no `state` to parse them into")
    node = Node(
        name=name,
        summary=summary,
        parameters=parameters,
        children=tuple(arm.node for arm in commands),
        version=version,
    )
    by_name = {arm.node.name: arm for arm in commands}

    def resolve(levels: tuple[Level, ...]) -> Action[Never]:
        own = tuple(extractor.extract(levels[0].args) for extractor in extractors)
        below = levels[1:]
        # The binder only ever selects a child that exists, so this is a lookup
        # rather than a check: an unknown name became a `Rejected` before here.
        # `inner` is collected at the arms' supertype `Action[Never]`; the
        # overloads already proved it accepts what `state` builds, so the cast
        # restores a type the checker verified rather than asserting a new one.
        inner = cast(_RawAction, by_name[below[0].name].resolve(below))

        if state is None:
            # Pure namespacing: nothing to build, so nothing to enter. The
            # children see exactly what this level was given.
            async def passing(parent: object) -> int:
                return await inner(parent)

            return cast(Action[Never], passing)

        async def entering(parent: object) -> int:
            async with state(parent, *own) as built:
                return await inner(built)

        return cast(Action[Never], entering)

    return Arm(node=node, resolve=resolve)
