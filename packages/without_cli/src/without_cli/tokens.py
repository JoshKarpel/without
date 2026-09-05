from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import Generic
from typing import TypeVar
from typing import overload

from without_cli.converters import Converter
from without_cli.converters import parse_boolean
from without_cli.sources import Source


@dataclass(frozen=True, slots=True)
class Positional:
    """
    How an `argument` token appears in usage and to the binder.

    Mirrors `Option`'s `variadic`/`required` pair, because both are read off the
    same `Cardinality`: `once` is required, `optional` and `default` are not, and
    `many` is the variadic one.

    The binder assigns bare tokens to these in declaration order and greedily, so
    `variadic` (which takes everything left) is only valid as the last one, and a
    required positional may not follow an optional one: with one token to hand
    out, `[A] B` would give it to `A` and leave `B` empty.
    """

    name: str
    summary: str = ""
    variadic: bool = False
    required: bool = True

    @property
    def metavar(self) -> str:
        return _metavar_of(self.name)


@dataclass(frozen=True, slots=True)
class Option:
    """
    How an `option`/`flag`/`count` token appears in usage and to the binder.

    `metavar` doubles as the binder's arity signal: `None` means the option takes
    no value, so it bundles into `-abc` and consumes nothing after itself.
    """

    names: tuple[str, ...]
    metavar: str | None
    summary: str = ""
    sources: tuple[Source, ...] = ()
    repeatable: bool = False
    required: bool = False

    @property
    def canonical(self) -> str:
        """
        The name this option's values are stored under.

        The first long name when there is one, so `("-v", "--verbose")` and
        `("--verbose", "-v")` agree on `--verbose` and the order aliases are
        declared in stays a presentation choice.
        """
        return _canonical(self.names)


type Parameter = Positional | Option

# What one command-line occurrence of a valueless option records, since `Args`
# carries raw strings and there is no raw value to carry for a bare `--verbose`.
# Named here, beside the `metavar is None` that declares an option valueless, so
# the binder that writes it and the `flag`/`count` that read it agree by
# construction: it must stay a spelling `parse_boolean` reads as true and `int`
# reads as one, which is what lets `-vv` and `VERBOSE=2` reach a command alike.
PRESENT = "1"


@dataclass(frozen=True, slots=True)
class Args:
    """
    One command-path level's raw values, bound but not yet parsed.

    The read-only context every extractor reads, and the point where the command
    line, the environment, and files have already been merged: `options` maps an
    option's canonical name to the raw values in effect for it (its command-line
    occurrences, or failing those whichever source supplied one), and `arguments`
    maps a positional's name to the tokens the binder assigned it.

    Absent is absent: a name with no values does not appear at all, so each
    token's own `parse` decides what that means rather than the binder guessing.
    """

    options: Mapping[str, tuple[str, ...]]
    arguments: Mapping[str, tuple[str, ...]]


class ExtractionError(ValueError):
    """
    An invocation rejected while one of its typed values was being extracted.

    The reject signal a token raises when its `parse` refuses the raw input,
    gathering at the raise site what a good message needs: `parameter` names the
    thing that failed (`--port`, `text`) and `cause` carries the underlying error
    as a first-class value rather than hiding in `__cause__`.

    Making the boundary one matchable type is what keeps a `ValueError` raised
    deeper inside a command from masquerading as a usage error: `parse_argv`
    turns this into a `Rejected` and lets anything else propagate.
    """

    def __init__(self, message: str, *, parameter: str, cause: Exception | None = None) -> None:
        super().__init__(message)
        self.parameter = parameter
        self.cause = cause


# Covariant: `V` appears only in `extract`'s return, so `Extractor[int]` is an
# `Extractor[object]` and a command can collect a heterogeneous mix of tokens
# while the `@overload` ladders keep each one's precise type for the handler. The
# legacy `TypeVar` is needed because PEP 695's inferred variance treats a (frozen)
# dataclass field as invariant; the variance is sound here.
_V_co = TypeVar("_V_co", covariant=True)

type AnyExtractor = Extractor[object]


@dataclass(frozen=True, slots=True)
class Extractor(Generic[_V_co]):  # noqa: UP046 - PEP 695 infers a frozen dataclass field as invariant; the covariant TypeVar is deliberate (see above)
    """
    A typed piece of an invocation, paired with the usage it contributes.

    `extract` is a pure `Args -> V` that raises `ExtractionError` to reject; it
    never decides *which* command runs. The same value carries the `parameters`
    it is described by, so a command's usage, its completions, and the binder's
    own arity table are all *recovered* from the tokens that parse it: one
    declaration, several consumers.

    Extraction reads only `Args`, never the program's state, which is what lets
    every value be parsed before the program builds anything. A `Bound` therefore
    proves the whole invocation is valid, and no resource is opened for a command
    line that was never going to run.
    """

    extract: Callable[[Args], _V_co]
    parameters: tuple[Parameter, ...] = ()


@dataclass(frozen=True, slots=True)
class Cardinality(Generic[_V_co]):  # noqa: UP046 - PEP 695 infers a frozen dataclass field as invariant; the covariant TypeVar is deliberate
    """
    How many raw values an option accepts, and what they parse into.

    Keeping the count and the parse in one value is what stops usage from
    drifting from behaviour: `once(INT)` is the single place saying the option is
    required, takes one value, and yields an `int`, so the help text and the
    parser cannot disagree about it.
    """

    parse: Callable[[tuple[str, ...]], _V_co] = field(compare=False)
    repeatable: bool = False
    required: bool = False


def once[V](converter: Converter[V]) -> Cardinality[V]:
    """
    Exactly one value, required.

    Rejects when the option is absent everywhere and when it is repeated, since a
    repeated singleton is an ambiguity rather than a value to quietly resolve.
    """
    convert = _converting(converter)

    def parse(values: tuple[str, ...]) -> V:
        match values:
            case (value,):
                return convert(value)
            case ():
                # The rejection is already prefixed with the parameter it came
                # from, so naming the value again ("for TEXT", "got none") only
                # repeats what the reader has just been told.
                raise ValueError("expected a value")
            case _:
                raise ValueError(f"expected one value, got {len(values)}")

    return Cardinality(parse, required=True)


def optional[V](converter: Converter[V]) -> Cardinality[V | None]:
    """One value or none, yielding `None` when absent. A repeat is still a rejection."""
    convert = _converting(converter)

    def parse(values: tuple[str, ...]) -> V | None:
        match values:
            case ():
                return None
            case (value,):
                return convert(value)
            case _:
                raise ValueError(f"expected at most one value, got {len(values)}")

    return Cardinality(parse)


def default[V](value: V, converter: Converter[V]) -> Cardinality[V]:
    """One value or none, yielding `value` when absent. A repeat is still a rejection."""
    convert = _converting(converter)

    def parse(values: tuple[str, ...]) -> V:
        match values:
            case ():
                return value
            case (raw,):
                return convert(raw)
            case _:
                raise ValueError(f"expected at most one value, got {len(values)}")

    return Cardinality(parse)


def many[V](converter: Converter[V]) -> Cardinality[tuple[V, ...]]:
    """
    Every value the option was given, in order, possibly none.

    A source supplies at most one raw value, so splitting `TAGS=a,b` into two is
    this `converter`'s business rather than the source's; no separator is baked in
    anywhere below here.
    """
    convert = _converting(converter)

    def parse(values: tuple[str, ...]) -> tuple[V, ...]:
        return tuple(convert(value) for value in values)

    return Cardinality(parse, repeatable=True)


def _names_of(names: str | tuple[str, ...]) -> tuple[str, ...]:
    return (names,) if isinstance(names, str) else names


def _canonical(names: tuple[str, ...]) -> str:
    return next((name for name in names if name.startswith("--")), names[0])


def _metavar_of(name: str) -> str:
    return name.lstrip("-").replace("-", "_").upper()


def _converting[V](converter: Converter[V]) -> Callable[[str], V]:
    """
    A converter's `parse`, restating a rejection in terms of what was wanted.

    `int("abc")` says "invalid literal for int() with base 10", which describes
    CPython rather than the command line. The converter knows the shape it parses
    into, so the message says that instead, and `_parsed` adds the parameter it
    came from.
    """

    def convert(raw: str) -> V:
        try:
            return converter.parse(raw)
        except ExtractionError:
            # Already attributed and worded by the converter itself, which knows
            # more about why it refused than "expected PROFILE" does.
            raise
        except ValueError as exc:
            raise ValueError(f"expected {converter.metavar}, got {raw!r}") from exc

    return convert


def _total(values: tuple[str, ...]) -> int:
    return sum(int(value) for value in values)


def _parsed[X, V](parameter: str, parse: Callable[[X], V], value: X) -> V:
    """
    Apply `parse`, tagging a rejection with the parameter it came from.

    A token names the part of the invocation it reads, so it is the layer that
    can turn a bare `ValueError` into a `parameter`-attributed `ExtractionError`.
    One raised by `parse` itself is already rich and passes through untouched.
    """
    try:
        return parse(value)
    except ExtractionError:
        raise
    except ValueError as exc:
        raise ExtractionError(str(exc), parameter=parameter, cause=exc) from exc


def option[V](
    names: str | tuple[str, ...],
    cardinality: Cardinality[V],
    *,
    metavar: str | None = None,
    sources: tuple[Source, ...] = (),
    summary: str = "",
) -> Extractor[V]:
    """
    A named option, parsed into `V`.

    `cardinality` (`once`, `optional`, `default`, `many`) owns both how many
    values are accepted and what they become, so the usage line and the parse
    cannot disagree. `sources` are consulted in order when the command line omits
    the option entirely and the first that holds a value wins; because they feed
    the *same* `cardinality.parse`, a value from the environment and one from the
    command line are validated identically.
    """
    spelled = _names_of(names)
    spec = Option(
        names=spelled,
        # From the canonical (long) name, so `("-t", "--tag")` shows `--tag TAG`
        # rather than `T`: which alias happens to be written first is a
        # presentation choice and must not leak into the placeholder.
        metavar=metavar if metavar is not None else _metavar_of(_canonical(spelled)),
        summary=summary,
        sources=sources,
        repeatable=cardinality.repeatable,
        required=cardinality.required,
    )

    def extract(args: Args) -> V:
        return _parsed(spec.canonical, cardinality.parse, args.options.get(spec.canonical, ()))

    return Extractor(extract, parameters=(spec,))


def flag(
    names: str | tuple[str, ...],
    *,
    sources: tuple[Source, ...] = (),
    summary: str = "",
) -> Extractor[bool]:
    """
    A valueless switch: `True` when present, `False` when absent.

    A source supplies a spelling rather than a presence (`DEBUG=false`), and the
    last value decides, which is what lets an environment variable turn a flag
    *off* as well as on.
    """
    spec = Option(names=_names_of(names), metavar=None, summary=summary, sources=sources)

    def extract(args: Args) -> bool:
        values = args.options.get(spec.canonical, ())
        return _parsed(spec.canonical, parse_boolean, values[-1]) if values else False

    return Extractor(extract, parameters=(spec,))


def count(
    names: str | tuple[str, ...],
    *,
    sources: tuple[Source, ...] = (),
    summary: str = "",
) -> Extractor[int]:
    """
    How many times a valueless switch appeared, so `-vvv` is `3`.

    Each command-line occurrence contributes `1` and a source contributes its own
    number, so `-vv` and `VERBOSE=2` reach the command as the same value.
    """
    spec = Option(names=_names_of(names), metavar=None, summary=summary, sources=sources, repeatable=True)

    def extract(args: Args) -> int:
        return _parsed(spec.canonical, _total, args.options.get(spec.canonical, ()))

    return Extractor(extract, parameters=(spec,))


def argument[V](name: str, cardinality: Cardinality[V], *, summary: str = "") -> Extractor[V]:
    """
    A positional argument, parsed into `V`.

    Takes the same `cardinality` vocabulary an `option` does, so one set of words
    describes both halves of a command line: `once` is the required single value,
    `optional` and `default` say what an omitted one becomes, and `many` takes
    every remaining token (`prog cp SRC DST` against `prog rm PATHS...`).

    Positionals are assigned in declaration order, so this value is both the usage
    entry and the typed read, declared exactly once. A `many` argument consumes
    the rest of the command line, so it is valid only as the last positional, and
    a required one may not follow an optional one; `command` refuses either
    layout where it is written.
    """
    spec = Positional(
        name=name,
        summary=summary,
        variadic=cardinality.repeatable,
        required=cardinality.required,
    )

    def extract(args: Args) -> V:
        return _parsed(name, cardinality.parse, args.arguments.get(name, ()))

    return Extractor(extract, parameters=(spec,))


# [[[cog import cog; from ladders import emit; cog.outl(emit("into")) ]]]
@overload
def into[M, A](
    make: Callable[[A], M],
    a: Extractor[A],
    /,
) -> Extractor[M]: ...


@overload
def into[M, A, B](
    make: Callable[[A, B], M],
    a: Extractor[A],
    b: Extractor[B],
    /,
) -> Extractor[M]: ...


@overload
def into[M, A, B, C](
    make: Callable[[A, B, C], M],
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    /,
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D](
    make: Callable[[A, B, C, D], M],
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    /,
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E](
    make: Callable[[A, B, C, D, E], M],
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    /,
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F](
    make: Callable[[A, B, C, D, E, F], M],
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    /,
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G](
    make: Callable[[A, B, C, D, E, F, G], M],
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    /,
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G, H](
    make: Callable[[A, B, C, D, E, F, G, H], M],
    a: Extractor[A],
    b: Extractor[B],
    c: Extractor[C],
    d: Extractor[D],
    e: Extractor[E],
    f: Extractor[F],
    g: Extractor[G],
    h: Extractor[H],
    /,
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G, H, J](
    make: Callable[[A, B, C, D, E, F, G, H, J], M],
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
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G, H, J, K](
    make: Callable[[A, B, C, D, E, F, G, H, J, K], M],
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
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G, H, J, K, N](
    make: Callable[[A, B, C, D, E, F, G, H, J, K, N], M],
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
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G, H, J, K, N, P](
    make: Callable[[A, B, C, D, E, F, G, H, J, K, N, P], M],
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
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G, H, J, K, N, P, Q](
    make: Callable[[A, B, C, D, E, F, G, H, J, K, N, P, Q], M],
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
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G, H, J, K, N, P, Q, R](
    make: Callable[[A, B, C, D, E, F, G, H, J, K, N, P, Q, R], M],
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
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S](
    make: Callable[[A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S], M],
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
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V](
    make: Callable[[A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V], M],
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
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W](
    make: Callable[[A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W], M],
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
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X](
    make: Callable[[A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X], M],
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
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X, Y](
    make: Callable[[A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X, Y], M],
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
) -> Extractor[M]: ...


@overload
def into[M, A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X, Y, Z](
    make: Callable[[A, B, C, D, E, F, G, H, J, K, N, P, Q, R, S, V, W, X, Y, Z], M],
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
) -> Extractor[M]: ...
# [[[end]]]
def into[M](make: Callable[..., M], *extractors: AnyExtractor) -> Extractor[M]:
    """
    Combine several tokens into one that builds a typed value.

    The escape hatch from a command's token-arity ceiling, and the way to parse a
    group of inputs into one model: each extractor supplies one positional
    argument to `make`, in order, with the types tied so a mismatch is a mypy
    error. The constituents' usage entries carry through, so combining changes
    nothing about how the command is described.
    """

    def extract(args: Args) -> M:
        return make(*(extractor.extract(args) for extractor in extractors))

    return Extractor(extract, parameters=tuple(p for e in extractors for p in e.parameters))
