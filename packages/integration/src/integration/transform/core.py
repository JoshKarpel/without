from __future__ import annotations

from enum import Enum
from typing import assert_never

from pydantic import BaseModel


class Mode(Enum):
    UPPER = "upper"
    LOWER = "lower"
    TITLE = "title"


class Settings(BaseModel):
    """The transform settings, validated from the ConfigMap YAML at the boundary."""

    default_mode: Mode = Mode.UPPER
    max_bytes: int = 1024


class TransformError(Exception):
    """A domain error from the core, for the shell to render. The core works in
    decoded text and never names a status code, wire format, or byte count."""


class UnknownMode(TransformError):
    def __init__(self, requested: str) -> None:
        self.requested = requested
        super().__init__(f"unknown mode: {requested}")


def apply_mode(mode: Mode, text: str) -> str:
    match mode:
        case Mode.UPPER:
            return text.upper()
        case Mode.LOWER:
            return text.lower()
        case Mode.TITLE:
            return text.title()
        case _ as unreachable:
            assert_never(unreachable)


def resolve_mode(settings: Settings, requested: str | None) -> Mode:
    """The requested mode, or the configured default when none was requested.

    Raises `UnknownMode` when `requested` names no known mode.
    """
    if requested is None:
        return settings.default_mode
    try:
        return Mode(requested)
    except ValueError:
        raise UnknownMode(requested) from None


def transform(settings: Settings, requested_mode: str | None, text: str) -> str:
    """Transform `text`, applying `settings`.

    The `mode` argument overrides `settings.default_mode`; raises `UnknownMode`
    when it names no known mode.
    """
    return apply_mode(resolve_mode(settings, requested_mode), text)
