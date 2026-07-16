from __future__ import annotations

import socket
from datetime import timedelta

type SocketOptions = tuple[tuple[int, int, int], ...]


def apply_socket_options(sock: socket.socket | None, options: SocketOptions) -> None:
    """Apply each `(level, option, value)` triple to `sock`."""
    if sock is None:
        return
    for level, option, value in options:
        sock.setsockopt(level, option, value)


def tcp_keepalive(
    *,
    idle: timedelta = timedelta(seconds=60),
    interval: timedelta = timedelta(seconds=10),
    count: int = 6,
) -> SocketOptions:
    """
    Enable TCP keepalive and tune its probe timing.

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
    quiet. `idle`/`interval` MUST be a whole number of seconds, since the OS options carry
    only integer seconds (a sub-second component is rejected here rather than silently
    truncated); `count` is a plain probe count, not a duration.

    Enabling keepalive (`SO_KEEPALIVE`) is portable, but the per-probe tuning is not
    uniformly spelled or present, so the result includes each knob only where the running
    platform exposes it and omits the rest (leaving that axis at the OS default):

    - Linux: `idle`/`interval`/`count` are `TCP_KEEPIDLE`/`TCP_KEEPINTVL`/`TCP_KEEPCNT`.
    - macOS: there is no `TCP_KEEPIDLE`; the idle knob is `TCP_KEEPALIVE` (used the same
      way), with `TCP_KEEPINTVL`/`TCP_KEEPCNT` for the other two.
    - Windows: the same three names as Linux, but only on recent builds (added "when
      available", Windows 10+); older Windows exposes none of them, so only `SO_KEEPALIVE`
      is enabled and the probe timing stays at the system default.
    """
    for name, duration in (("idle", idle), ("interval", interval)):
        if duration.microseconds:
            raise ValueError(f"{name} must be a whole number of seconds, got {duration}")
    # macOS has no TCP_KEEPIDLE and spells the idle knob TCP_KEEPALIVE; Linux and modern
    # Windows use TCP_KEEPIDLE, so prefer it and fall back to the macOS name.
    idle_option = getattr(socket, "TCP_KEEPIDLE", None) or getattr(socket, "TCP_KEEPALIVE", None)
    options = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]
    for option, value in (
        (idle_option, int(idle.total_seconds())),
        (getattr(socket, "TCP_KEEPINTVL", None), int(interval.total_seconds())),
        (getattr(socket, "TCP_KEEPCNT", None), count),
    ):
        # The skip path only fires on a platform missing a knob (e.g. older Windows),
        # which CI does not run, so branch coverage cannot see it.
        if option is not None:  # pragma: no branch
            options.append((socket.IPPROTO_TCP, option, value))
    return tuple(options)


def send_buffer_size(size: int) -> SocketOptions:
    """
    Pin the socket's send buffer to `size` bytes (`SO_SNDBUF`).

    Pinning it is what makes the buffer a *known* size. Left alone, Linux autotunes the
    send buffer up to the `max` of the
    [`net.ipv4.tcp_wmem`](https://docs.kernel.org/networking/ip-sysctl.html) sysctl, and
    that sysctl's own documentation is the guarantee relied on here: "Calling
    `setsockopt()` with `SO_SNDBUF` disables automatic tuning of that socket's send buffer
    size, in which case this value is ignored."

    Two caveats from [`socket(7)`](https://man7.org/linux/man-pages/man7/socket.7.html),
    which make this a bound rather than an exact reservation: the value is capped at
    `net.core.wmem_max`, and "the kernel doubles this value (to allow space for bookkeeping
    overhead) when it is set using `setsockopt(2)`, and this doubled value is returned by
    `getsockopt(2)`", so reading it back does not return what was set.

    The sysctl names above are Linux's. `SO_SNDBUF` itself is POSIX and portable; what
    varies elsewhere is only which knob bounds it.
    """
    return ((socket.SOL_SOCKET, socket.SO_SNDBUF, size),)


def receive_buffer_size(size: int) -> SocketOptions:
    """
    Pin the socket's receive buffer to `size` bytes (`SO_RCVBUF`).

    The receive-side counterpart of `send_buffer_size`, with the same caveats and the same
    guarantee: autotuning is bounded by
    [`net.ipv4.tcp_rmem`](https://docs.kernel.org/networking/ip-sysctl.html), whose docs say
    "Calling `setsockopt()` with `SO_RCVBUF` disables automatic tuning of that socket's
    receive buffer size, in which case this value is ignored". The cap is
    `net.core.rmem_max` and the stored value is likewise doubled.

    Set on a *listening* socket, it is inherited by every accepted connection, which is how
    a server bounds what it will buffer from a peer whose body it has not read yet.
    """
    return ((socket.SOL_SOCKET, socket.SO_RCVBUF, size),)
