from __future__ import annotations

import argparse
import asyncio
import subprocess
import sys
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import psutil
from without_http import ConnectionPool

# The async supervisor: for each rate it boots a fresh server process (optionally
# under the austin sampler, so each rate gets its own profile), drives a vegeta run
# against it, and writes that run's raw results and text report into `results/`.
# Load generation and sampling both happen in separate processes (vegeta, austin),
# never in this
# interpreter, so nothing here competes with the code under test for CPU. vegeta
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
                async with asyncio.timeout(1.0), pool.request("GET", url, headers=close) as response:
                    if response.head.status == 200:
                        return
            except OSError, TimeoutError:
                pass
            await asyncio.sleep(0.05)
    raise TimeoutError(f"server did not serve {_READINESS_PATH} within {timeout_seconds}s: {url}")


@asynccontextmanager
async def _server(
    framework: str, host: str, port: int, *, profile: tuple[Path, int] | None = None
) -> AsyncIterator[asyncio.subprocess.Process]:
    server = [sys.executable, "-m", "benchmarks.todos.servers", framework, "--host", host, "--port", str(port)]
    # austin's launch mode runs the server as austin's own child, so sampling works
    # under Yama `ptrace_scope=1` (which forbids tracing a non-descendant) with no
    # sudo, and it brackets the whole run. Attach mode (`-p PID`) would need ptrace
    # relaxation. `-c` is CPU mode: sample only on-CPU stacks, so the profile shows
    # where the core actually goes rather than counting idle waits. When not
    # profiling, the server is launched directly.
    austin = ["austin", "-c", "-i", str(profile[1]), "-o", str(profile[0])] if profile is not None else []
    command = [*austin, *server]
    proc = await asyncio.create_subprocess_exec(*command)
    try:
        await _await_ready(f"http://{host}:{port}{_READINESS_PATH}")
        yield proc
    finally:
        proc.terminate()
        await proc.wait()
        if profile is not None:
            # austin 4.0 writes the MOJO binary format; convert it to austin's text
            # format and then to speedscope, both via `austin-python`'s converters.
            mojo = profile[0]
            text = mojo.with_suffix(".austin")
            await _run(["mojo2austin", str(mojo), str(text)])
            await _run(["austin2speedscope", str(text), str(mojo.with_suffix(".speedscope.json"))])


async def _attack(base: str, endpoint: str, rate: int, duration: int, connections: int, out: Path) -> str:
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
    if has_body:
        attack += ["-body", str(_BODY), "-header", "Content-Type: application/json"]
    await _run(attack, stdin=target)
    return await _capture(["vegeta", "report", "-type=text", str(out)])


def _server_process(proc: asyncio.subprocess.Process, profiling: bool) -> psutil.Process:
    # When profiling, `proc` is austin and the server it launched is austin's child;
    # measure that, not the sampler. Otherwise `proc` is the server itself.
    parent = psutil.Process(proc.pid)
    children = parent.children()
    return children[0] if profiling and children else parent


async def _record(base: str, args: argparse.Namespace, rate: int, label: str, stem: Path) -> None:
    # A fresh server per rate, so each rate gets its own profile (the hot path can
    # look different under light load versus past the knee) instead of one sample
    # stream smeared across every rate.
    profile = (stem.with_suffix(".mojo"), args.interval) if args.profile else None
    async with _server(args.framework, args.host, args.port, profile=profile) as proc:
        server = _server_process(proc, profile is not None)
        before = server.cpu_times()
        started = time.monotonic()
        report = await _attack(base, args.endpoint, rate, args.duration, args.connections, stem.with_suffix(".bin"))
        # Server-process CPU seconds over the attack window, as a fraction of wall
        # time: 1.0 == one core fully saturated (both stacks are single-process).
        after = server.cpu_times()
        cores = ((after.user - before.user) + (after.system - before.system)) / (time.monotonic() - started)
    stem.with_suffix(".txt").write_text(report)
    stem.with_suffix(".cpu").write_text(f"{cores:.3f}\n")
    print(f"[{label}] {args.framework} {args.endpoint} @ {rate} rps -> {stem}.txt  ({cores:.2f} CPU cores)")
    print(report)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Drive a vegeta rate sweep against one todo stack, optionally profiling the server with austin.",
    )
    parser.add_argument("framework", choices=("without", "fastapi"))
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
        "--profile", action="store_true", help="run each rate's server under austin for a per-rate profile"
    )
    parser.add_argument("--interval", type=int, default=100, help="austin sampling interval in microseconds")
    parser.add_argument("--results", type=Path, default=Path(__file__).parent.parent.parent / "results")
    args = parser.parse_args()

    rates = args.rates or [1000, 2000, 5000, 10000]
    args.results.mkdir(parents=True, exist_ok=True)
    base = f"http://{args.host}:{args.port}"
    # The duration and profiling state are in every filename, so a profiled run and
    # a clean one (or two different durations) at the same rate coexist instead of
    # overwriting each other. Profiling inflates latency, so the `-prof` tag also
    # lets the plot pick only clean runs.
    prefix = f"{args.framework}-{args.endpoint}"
    tag = f"d{args.duration}s" + ("-prof" if args.profile else "")

    for rate in rates:
        await _record(base, args, rate, "sweep", args.results / f"{prefix}-r{rate}-{tag}")
    if args.saturate is not None:
        await _record(base, args, args.saturate, "saturate", args.results / f"{prefix}-saturate-{tag}")


if __name__ == "__main__":
    asyncio.run(main())
