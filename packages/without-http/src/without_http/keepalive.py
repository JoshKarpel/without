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
    duration. The three map to the Linux `TCP_KEEPIDLE`/`TCP_KEEPINTVL`/`TCP_KEEPCNT` socket
    options (`TCP_KEEPALIVE` is the macOS name for the idle one). `SO_KEEPALIVE` itself is
    portable, but a platform lacking a given per-probe knob keeps its own default for that
    axis; on Windows only enabling keepalive is portable through `setsockopt`.
    """

    idle: timedelta = timedelta(seconds=60)
    interval: timedelta = timedelta(seconds=10)
    count: int = 6

    def __post_init__(self) -> None:
        for name, value in (("idle", self.idle), ("interval", self.interval)):
            if value.microseconds:
                raise ValueError(f"{name} must be a whole number of seconds, got {value}")


def apply_tcp_keepalive(writer: asyncio.StreamWriter, keepalive: TCPKeepalive) -> None:
    """
    Enable and tune TCP keepalive on `writer`'s underlying socket.

    Sets the portable `SO_KEEPALIVE` flag, then whichever per-probe options the running
    platform exposes; an absent option is skipped, leaving the OS default for that axis. A
    transport with no socket (a test double) is left untouched.
    """
    sock = writer.get_extra_info("socket")
    if sock is None:
        return
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    idle_option = getattr(socket, "TCP_KEEPIDLE", None) or getattr(socket, "TCP_KEEPALIVE", None)
    for option, value in (
        (idle_option, int(keepalive.idle.total_seconds())),
        (getattr(socket, "TCP_KEEPINTVL", None), int(keepalive.interval.total_seconds())),
        (getattr(socket, "TCP_KEEPCNT", None), keepalive.count),
    ):
        if option is not None:
            sock.setsockopt(socket.IPPROTO_TCP, option, value)
