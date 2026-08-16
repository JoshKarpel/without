from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta

from without import timeout


@dataclass(frozen=True, slots=True)
class Timeout:
    """
    Per-phase inactivity bounds for one client request, each disabled (`None`) by default.

    Four axes, following httpx, each bounding one phase of a request that fails for its
    own reason (see the `without-http` guide's request-lifecycle table):

    - `connect`: establishing the TCP (and TLS) connection to the origin.
    - `read`: waiting for the next chunk of the response (re-armed per chunk, so it
      bounds the *gap* between chunks, not the whole read).
    - `write`: making progress sending the next chunk of the request body (re-armed per
      write, so a lazily-fed body that pauses between chunks does not trip it).
    - `pool`: waiting to acquire a connection slot, which only bites once a per-host
      bound (or the h2 stream limit) is in force.

    Each axis is a `timedelta`, so the unit is explicit at the call site rather than an
    ambiguous bare number. Every field defaults to `None` (that axis disabled), so the
    default `Timeout()` bounds nothing: a timeout is a *policy* keyed to the caller's time
    budget ("fail rather than make slow progress so my upstream can react"), which the
    transport cannot know, so a caller opts in per axis (`Timeout(connect=timedelta(
    seconds=10), read=timedelta(seconds=30))`). There is deliberately no shared-default
    scalar: one duration across four unrelated phases carries no meaning. It rides on the
    `ClientRequest` it bounds (`deadline` fills it in for a whole client), so it is the
    caller's value rather than the connection's. For an overall wall-clock cap, compose
    `async with asyncio.timeout(t): request(...)`.

    Each axis is applied through its own bound: `connecting()`, `reading()`, `writing()`,
    and `pooling()` each return a context manager that bounds the wrapped await(s) by that
    axis and raises its typed error on expiry. The mapping from axis to typed error lives
    here, once, rather than at every call site that arms a deadline.
    """

    connect: timedelta | None = None
    read: timedelta | None = None
    write: timedelta | None = None
    pool: timedelta | None = None

    def connecting(self) -> AbstractAsyncContextManager[None]:
        """Bound establishing the connection, raising `ConnectTimeout` on expiry."""
        return _bounded(self.connect, ConnectTimeout)

    def reading(self) -> AbstractAsyncContextManager[None]:
        """Bound awaiting the next response chunk, raising `ReadTimeout` on expiry."""
        return _bounded(self.read, ReadTimeout)

    def writing(self) -> AbstractAsyncContextManager[None]:
        """Bound making progress on the request body, raising `WriteTimeout` on expiry."""
        return _bounded(self.write, WriteTimeout)

    def pooling(self) -> AbstractAsyncContextManager[None]:
        """Bound acquiring a connection slot, raising `PoolTimeout` on expiry."""
        return _bounded(self.pool, PoolTimeout)


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
async def _bounded(duration: timedelta | None, kind: type[HTTPTimeout]) -> AsyncIterator[None]:
    """
    Bound the wrapped await(s) by `duration`, raising `kind` on expiry.

    Delegates the bound to `without.timeout`, which treats `None` as no deadline, so a
    disabled axis is a no-op; this wrapper adds the typed-error classification on top. An
    inner bound that already classified its own timeout is not re-wrapped: brackets nest (a
    read inside a write region), and the typed errors are themselves `TimeoutError`
    subclasses, so the innermost, most specific classification wins.
    """
    try:
        async with timeout(duration):
            yield
    except TimeoutError as exc:
        if isinstance(exc, HTTPTimeout):
            raise
        raise kind() from None
