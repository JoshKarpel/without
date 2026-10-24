from __future__ import annotations

import argparse
import asyncio
import importlib.util
import signal
import subprocess
import sys
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import psutil
from without_http import ConnectionPool
from without_http import request

from benchmarks.todos.servers import DEFAULT_SERVER
from benchmarks.todos.servers import FRAMEWORKS
from benchmarks.todos.servers import SERVERS

# The async supervisor: for each rate it boots a fresh server process (optionally
# under the Tachyon sampler, so each rate gets its own profile), drives a vegeta run
# against it, and writes that run's raw results and text report into `results/`.
# Load generation and sampling both happen in separate processes (vegeta, Tachyon),
# never in this interpreter, so nothing here competes with the code under test for CPU. vegeta
# drives a constant arrival rate (open loop), so its latency distribution is
# corrected for coordinated omission. This is pure orchestration, run by hand, so
# it is out of the unit-test path.

_BODY = Path(__file__).parent / "todos" / "vegeta" / "todo.json"

# HTTP method, path on the server, and whether the request carries the JSON body.
_ENDPOINTS: dict[str, tuple[str, str, bool]] = {
    "list": ("GET", "/todos", False),
    "show": ("GET", "/todos/2", False),
    "create": ("POST", "/todos", True),
}

# Both stacks serve this identically and return 200, so it doubles as the
# readiness probe without adding an asymmetric route to either hot path.
_READINESS_PATH = "/todos"

# Tachyon, the stdlib sampling profiler (Python 3.15+), run as this interpreter's module.
_TACHYON = (sys.executable, "-m", "profiling.sampling")


async def _run(command: list[str], *, stdin: str | None = None) -> None:
    proc = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.PIPE if stdin is not None else None,
    )
    await proc.communicate(stdin.encode() if stdin is not None else None)
    if proc.returncode:
        raise subprocess.CalledProcessError(proc.returncode, command)


async def _capture(command: list[str]) -> str:
    proc = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE)
    stdout, _ = await proc.communicate()
    if proc.returncode:
        raise subprocess.CalledProcessError(proc.returncode, command)
    return stdout.decode()


async def _await_ready(url: str, timeout_seconds: float = 10.0) -> None:
    """
    Gate on a real HTTP 200 from the app, using without-http's own client.

    A TCP connect only proves the kernel accepted the connection; a successful
    `GET` proves the ASGI app and its lifespan are actually serving. Driving the
    probe through `without-http` also exercises the client against the server
    under test, which is the whole point. The probe asks for `Connection: close`:
    it needs no keep-alive, and letting the server close each probe connection
    itself avoids an abortive client-side close.
    """
    deadline = time.monotonic() + timeout_seconds
    close = ((b"connection", b"close"),)
    async with ConnectionPool() as pool:
        while time.monotonic() < deadline:
            try:
                async with asyncio.timeout(1.0), request(pool, "GET", url, headers=close) as response:
                    if response.head.status == 200:
                        return
            except OSError, TimeoutError:
                pass
            await asyncio.sleep(0.05)
    raise TimeoutError(f"server did not serve {_READINESS_PATH} within {timeout_seconds}s: {url}")


@asynccontextmanager
async def _server(
    framework: str, server_kind: str, host: str, port: int, *, profile: tuple[Path, int] | None = None
) -> AsyncIterator[asyncio.subprocess.Process]:
    server = [
        "benchmarks.todos.servers",
        framework,
        "--server",
        server_kind,
        "--host",
        host,
        "--port",
        str(port),
    ]
    # Tachyon's `run` launches the server as its own child, so sampling works under
    # Yama `ptrace_scope=1` (which forbids tracing a non-descendant) with no sudo, and
    # it brackets the whole run. `--mode gil` samples only while a thread holds the
    # GIL, so the event loop parked in `epoll_wait` doesn't count, and `--blocking`
    # freezes the threads for each read: without it about a sixth of samples fail,
    # and not uniformly across frames. Not `--mode cpu`: a frozen thread is never on
    # CPU, so with `--blocking` it records no samples at all. When not profiling, the
    # server is launched directly.
    sampler = [*_TACHYON, "run", "--blocking", "--mode", "gil", "--all-threads"]
    command = (
        [*sampler, "--sampling-rate", str(profile[1]), "--binary", "-o", str(profile[0]), "-m", *server]
        if profile is not None
        else [sys.executable, "-m", *server]
    )
    proc = await asyncio.create_subprocess_exec(*command)
    try:
        await _await_ready(f"http://{host}:{port}{_READINESS_PATH}")
        yield proc
    finally:
        # Tachyon writes its profile and stops the server on SIGINT; a SIGTERM kills
        # it with an empty profile and leaves the server running as an orphan.
        proc.send_signal(signal.SIGINT if profile is not None else signal.SIGTERM)
        await proc.wait()
        if profile is not None:
            flamegraph = profile[0].with_suffix(".flamegraph.html")
            await _run([*_TACHYON, "replay", "--flamegraph", "-o", str(flamegraph), str(profile[0])])


async def _attack(base: str, endpoint: str, rate: int, duration: int, connections: int, out: Path, *, h2c: bool) -> str:
    method, path, has_body = _ENDPOINTS[endpoint]
    target = f"{method} {base}{path}"
    attack = [
        "vegeta",
        "attack",
        f"-rate={rate}",
        f"-duration={duration}s",
        f"-max-workers={connections}",
        f"-output={out}",
    ]
    if h2c:
        # HTTP/2 cleartext (prior knowledge). without-http negotiates it by sniffing
        # the h2 preface; a server that only speaks HTTP/1.1 (uvicorn) will refuse.
        attack.append("-h2c")
    if has_body:
        # Both create routes require an idempotency key. Vegeta replays one static value
        # for every request, which is fine here: neither stack persists keys across
        # requests, so a repeat is still a full create rather than a replayed hit.
        attack += [
            "-body",
            str(_BODY),
            "-header",
            "Content-Type: application/json",
            "-header",
            "Idempotency-Key: bench-abc-123",
        ]
    await _run(attack, stdin=target)
    return await _capture(["vegeta", "report", "-type=text", str(out)])


def _server_process(proc: asyncio.subprocess.Process, profiling: bool) -> psutil.Process:
    # When profiling, `proc` is Tachyon and the server it launched is Tachyon's child;
    # measure that, not the sampler. Otherwise `proc` is the server itself.
    parent = psutil.Process(proc.pid)
    children = parent.children()
    return children[0] if profiling and children else parent


async def _record(base: str, args: argparse.Namespace, rate: int, label: str, stem: Path) -> None:
    # A fresh server per rate, so each rate gets its own profile (the hot path can
    # look different under light load versus past the knee) instead of one sample
    # stream smeared across every rate.
    profile = (stem.with_suffix(".tachyon"), args.sampling_rate) if args.profile else None
    async with _server(args.framework, args.server, args.host, args.port, profile=profile) as proc:
        server = _server_process(proc, profile is not None)
        before = server.cpu_times()
        started = time.monotonic()
        report = await _attack(
            base, args.endpoint, rate, args.duration, args.connections, stem.with_suffix(".bin"), h2c=args.h2c
        )
        # Server-process CPU seconds over the attack window, as a fraction of wall
        # time: 1.0 == one core fully saturated (every cell is single-process).
        after = server.cpu_times()
        cores = ((after.user - before.user) + (after.system - before.system)) / (time.monotonic() - started)
    stem.with_suffix(".txt").write_text(report)
    stem.with_suffix(".cpu").write_text(f"{cores:.3f}\n")
    print(
        f"[{label}] {args.framework} on {args.server} {args.endpoint} @ {rate} rps"
        f" -> {stem}.txt  ({cores:.2f} CPU cores)"
    )
    print(report)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Drive a vegeta rate sweep against one todo stack, optionally profiling the server with Tachyon.",
    )
    parser.add_argument("framework", choices=tuple(FRAMEWORKS))
    parser.add_argument(
        "--server",
        choices=tuple(SERVERS),
        default=None,
        help="server to run the framework under (default: the server the framework normally deploys under)",
    )
    parser.add_argument("--endpoint", choices=tuple(_ENDPOINTS), default="list")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--rate",
        type=int,
        action="append",
        dest="rates",
        metavar="RPS",
        help="a target request rate for the sweep; repeat to sweep several (default: 1000 2000 5000 10000)",
    )
    parser.add_argument(
        "--saturate",
        type=int,
        default=None,
        metavar="RPS",
        help="add a deliberately over-capacity ceiling run at this rate; read it as a throughput ceiling, not latency",
    )
    parser.add_argument("--duration", type=int, default=30, help="seconds per rate step")
    parser.add_argument("--connections", type=int, default=100, help="vegeta -max-workers (concurrent connections cap)")
    parser.add_argument(
        "--h2c",
        action="store_true",
        help="drive HTTP/2 cleartext instead of HTTP/1.1 (only without-http negotiates it; uvicorn will refuse)",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="run each rate's server under Tachyon (Python 3.15+) for a per-rate profile",
    )
    parser.add_argument("--sampling-rate", type=int, default=10_000, help="Tachyon samples per second")
    parser.add_argument("--results", type=Path, default=Path(__file__).parent.parent.parent / "results")
    args = parser.parse_args()
    if args.profile and importlib.util.find_spec("profiling.sampling") is None:
        parser.error("--profile needs Tachyon, Python 3.15's sampling profiler: rerun with UV_PYTHON=3.15")

    args.server = args.server or DEFAULT_SERVER[args.framework]
    rates = args.rates or [1000, 2000, 5000, 10000]
    args.results.mkdir(parents=True, exist_ok=True)
    base = f"http://{args.host}:{args.port}"
    # The (framework, server) cell, transport, duration, and profiling state are in
    # every filename, so every combination (a profiled run beside a clean one, an
    # h2c run beside an h1 one, or two durations) at the same rate coexists instead
    # of overwriting. Profiling inflates latency, so the `-prof` tag also lets the
    # plot pick only clean runs. `+` joins the pair; `-` stays the field separator.
    prefix = f"{args.framework}+{args.server}-{args.endpoint}"
    tag = ("h2c-" if args.h2c else "") + f"d{args.duration}s" + ("-prof" if args.profile else "")

    for rate in rates:
        await _record(base, args, rate, "sweep", args.results / f"{prefix}-r{rate}-{tag}")
    if args.saturate is not None:
        await _record(base, args, args.saturate, "saturate", args.results / f"{prefix}-saturate-{tag}")


if __name__ == "__main__":
    asyncio.run(main())
