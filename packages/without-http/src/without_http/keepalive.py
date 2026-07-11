from __future__ import annotations

import asyncio
import socket
from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True, slots=True)
class TCPKeepalive:
    """
    Kernel TCP keepalive tuning for pooled client sockets.

    Once enabled, the OS probes an otherwise-idle connection and tears it down if the peer
    has gone away *without* a `TCP FIN`: a crashed server, a network partition, a NAT or
    firewall silently dropping the flow. A clean server-side keep-alive close sends a `FIN`,
    which the pool already notices before reuse (`_Http11Connection.usable`); keepalive
    covers the *silent* case, which matters most when request timeouts are disabled (the
    default), since nothing else would notice a dead idle socket until a request hung on it.

    - `idle`: how long a connection sits idle before the first probe.
    - `interval`: the gap between probes once they start.
    - `count`: unanswered probes before the connection is declared dead.

    So a broken idle connection is dropped roughly `idle + interval * count` after it goes
    quiet. `idle`/`interval` are `timedelta`s that MUST be a whole number of seconds, since
    the OS options carry only integer seconds (a sub-second component is rejected at
    construction rather than silently truncated); `count` is a plain probe count, not a
    duration.

    Enabling keepalive (`SO_KEEPALIVE`) is portable, but the per-probe tuning is not
    uniformly spelled or present, so `socket_options` includes each knob only where the
    running platform exposes it and omits the rest (leaving that axis at the OS default):

    - Linux: `idle`/`interval`/`count` are `TCP_KEEPIDLE`/`TCP_KEEPINTVL`/`TCP_KEEPCNT`.
    - macOS: there is no `TCP_KEEPIDLE`; the idle knob is `TCP_KEEPALIVE` (used the same
      way), with `TCP_KEEPINTVL`/`TCP_KEEPCNT` for the other two.
    - Windows: the same three names as Linux, but only on recent builds (added "when
      available", Windows 10+); older Windows exposes none of them, so only `SO_KEEPALIVE`
      is applied and the probe timing stays at the system default.
    """

    idle: timedelta = timedelta(seconds=60)
    interval: timedelta = timedelta(seconds=10)
    count: int = 6

    def __post_init__(self) -> None:
        for name, value in (("idle", self.idle), ("interval", self.interval)):
            if value.microseconds:
                raise ValueError(f"{name} must be a whole number of seconds, got {value}")

    def socket_options(self) -> list[tuple[int, int, int]]:
        """
        The `(level, option, value)` triples to `setsockopt` to realize this config.

        A pure description of *what* to set, so applying it stays a mechanical loop in the
        shell (`apply_tcp_keepalive`). Always enables `SO_KEEPALIVE`, then adds each per-probe
        knob the running platform exposes; a name it lacks is omitted (see the class
        docstring for the per-platform spelling).
        """
        # macOS has no TCP_KEEPIDLE and spells the idle knob TCP_KEEPALIVE; Linux and modern
        # Windows use TCP_KEEPIDLE, so prefer it and fall back to the macOS name.
        idle_option = getattr(socket, "TCP_KEEPIDLE", None) or getattr(socket, "TCP_KEEPALIVE", None)
        options = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]
        for option, value in (
            (idle_option, int(self.idle.total_seconds())),
            (getattr(socket, "TCP_KEEPINTVL", None), int(self.interval.total_seconds())),
            (getattr(socket, "TCP_KEEPCNT", None), self.count),
        ):
            # The skip path only fires on a platform missing a knob (e.g. older Windows),
            # which CI does not run, so branch coverage cannot see it.
            if option is not None:  # pragma: no branch
                options.append((socket.IPPROTO_TCP, option, value))
        return options


def apply_tcp_keepalive(writer: asyncio.StreamWriter, keepalive: TCPKeepalive) -> None:
    """
    Enable and tune TCP keepalive on `writer`'s underlying socket.

    Applies `keepalive.socket_options()` to the socket. A transport with no socket (a test
    double) is left untouched.
    """
    sock = writer.get_extra_info("socket")
    if sock is None:
        return
    for level, option, value in keepalive.socket_options():
        sock.setsockopt(level, option, value)
