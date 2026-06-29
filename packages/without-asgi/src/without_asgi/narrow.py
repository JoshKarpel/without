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


def narrow_to_str(value: object) -> str:
    return narrow(value, str)


def narrow_to_bytes(value: object) -> bytes:
    return narrow(value, bytes)


def narrow_to_int(value: object) -> int:
    return narrow(value, int)
