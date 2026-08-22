from __future__ import annotations


def narrow[T](value: object, expected: type[T]) -> T:
    """
    Return `value` typed as `expected`, raising `TypeError` if it isn't.

    The boundary reads ASGI scopes and messages as `Mapping[str, object]`, so
    every field arrives as an untyped `object`. `narrow` turns one such value
    into the concrete type the caller expects, failing loudly on a mismatch
    rather than letting a wrong type flow inward.
    """
    if isinstance(value, expected):
        return value
    raise TypeError(f"expected {expected.__name__}, got {type(value).__name__}")
