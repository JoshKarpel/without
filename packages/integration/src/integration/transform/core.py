from __future__ import annotations

from enum import Enum
from typing import assert_never

from pydantic import BaseModel


class Mode(Enum):
    UPPER = "upper"
    LOWER = "lower"
    TITLE = "title"


class TransformConfig(BaseModel):
    """
    The domain config: the one knob the core needs, the default `Mode`.

    Deliberately free of any shell concern (no byte limit, no wire format): the
    core works in decoded text under this config and nothing else, so the same
    value serves whichever shell drives it (the ASGI app, the CLI). A shell wraps
    this in its own larger config (see `transform.app`'s `Settings`).
    """

    default_mode: Mode = Mode.UPPER


class TransformError(Exception):
    """
    A domain error from the core, for the shell to render. The core works in
    decoded text and never names a status code, wire format, or byte count.
    """


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


def resolve_mode(config: TransformConfig, requested: str | None) -> Mode:
    """
    The requested mode, or the configured default when none was requested.

    Raises `UnknownMode` when `requested` names no known mode.
    """
    if requested is None:
        return config.default_mode
    try:
        return Mode(requested)
    except ValueError:
        raise UnknownMode(requested) from None


def transform(config: TransformConfig, requested_mode: str | None, text: str) -> str:
    """
    Transform `text`, applying `config`.

    The `requested_mode` argument overrides `config.default_mode`; raises
    `UnknownMode` when it names no known mode.
    """
    return apply_mode(resolve_mode(config, requested_mode), text)
