from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Timeout:
    """
    Per-phase inactivity bounds for a client request, each disabled (`None`) by default.

    Four axes, following httpx, each bounding one phase of a request that fails for its
    own reason (see the `without-http` guide's request-lifecycle table):

    - `connect`: establishing the TCP (and TLS) connection to the origin.
    - `read`: waiting for the next chunk of the response (re-armed per chunk, so it
      bounds the *gap* between chunks, not the whole read).
    - `write`: making progress sending the next chunk of the request body (re-armed per
      write, so a lazily-fed body that pauses between chunks does not trip it).
    - `pool`: waiting to acquire a connection slot, which only bites once a per-host
      bound (or the h2 stream limit) is in force.

    Every field defaults to `None` (that axis disabled), so the default `Timeout()` bounds
    nothing: a timeout is a *policy* keyed to the caller's time budget ("fail rather than
    make slow progress so my upstream can react"), which the transport cannot know, so a
    caller opts in per axis (`Timeout(connect=10.0, read=30.0)`). There is deliberately no
    shared-default scalar: one number across four unrelated phases carries no meaning. For
    an overall wall-clock cap, compose `async with asyncio.timeout(t): pool.request(...)`.
    """

    connect: float | None = None
    read: float | None = None
    write: float | None = None
    pool: float | None = None


class HTTPTimeout(TimeoutError):
    """
    Base for a phase-specific client timeout, subclassing `TimeoutError`.

    A coarse `except TimeoutError` catches any of them; the specific type tells the caller
    *how far the request got*, which is what determines the safe recovery (the reason a
    typed per-phase error beats a bare `TimeoutError`). See each subclass.
    """


class ConnectTimeout(HTTPTimeout):
    """
    Establishing the connection to the origin took too long.

    The request never reached the server, so it is **always** safe to retry, even a
    non-idempotent one, or to fail over to another origin.
    """


class ReadTimeout(HTTPTimeout):
    """
    Waiting for the next chunk of the response took too long.

    The request was fully sent, so the server may **already have processed it**: retry
    only if the request is idempotent or carries an idempotency key, otherwise surface it.
    If it fired mid-body, the partial response already read is yours to keep or discard.
    """


class WriteTimeout(HTTPTimeout):
    """
    Making progress sending the request body took too long.

    The server saw at most a partial request. Retrying is safe for an idempotent request
    and ambiguous otherwise; the connection is discarded, so a retry gets a fresh one.
    """


class PoolTimeout(HTTPTimeout):
    """
    Acquiring a connection slot took too long (the per-host bound or h2 stream limit).

    The request never left the process, so it is **always** safe to retry, though the real
    fix is usually local backpressure rather than retrying the peer.
    """


@asynccontextmanager
async def phase(seconds: float | None, kind: type[HTTPTimeout]) -> AsyncIterator[None]:
    """
    Bound the wrapped await(s) by `seconds`, raising `kind` on expiry.

    `asyncio.timeout(None)` imposes no deadline, so a disabled axis is a no-op. An inner
    phase that already classified its own timeout is not re-wrapped: brackets nest (a read
    inside a write region), and the typed errors are themselves `TimeoutError` subclasses,
    so the innermost, most specific classification wins.
    """
    try:
        async with asyncio.timeout(seconds):
            yield
    except TimeoutError as exc:
        if isinstance(exc, HTTPTimeout):
            raise
        raise kind() from None
