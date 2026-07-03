from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable
from typing import cast

import uvicorn
import uvloop
from without import sleep_forever
from without_asgi import ASGIApp
from without_http import serving

from benchmarks.todos.apps import fastapi_todos
from benchmarks.todos.apps import without_todos

# The registry that turns a benchmark cell into a *long-running server process*,
# launched one per run so an external load generator drives a real socket and a
# sampling profiler can attach to the server PID. In ASGI's own vocabulary a cell
# is an (application framework, server) pair, drawn independently from `FRAMEWORKS`
# and `SERVERS`: both todo shells are plain ASGI apps and every server speaks plain
# ASGI, so any combination runs. That is the whole point of the matrix: hold the
# framework fixed to isolate the server's cost, or hold the server fixed to isolate
# the framework's. The `without-http-uvloop` server splits the event loop out from
# the HTTP server too, isolating uvloop's C socket I/O from the stdlib selector
# transport under the same h11 parser. `DEFAULT_SERVER` records the server each
# framework is conventionally deployed under (without-web on without-http over
# stdlib asyncio, FastAPI on uvicorn with uvloop + httptools), so the two
# as-deployed baselines stay the default while the cross cells are one flag away.
# These are process bootstraps, not domain logic, exercised by running a benchmark.

FRAMEWORKS: dict[str, Callable[[], ASGIApp]] = {
    "without": without_todos,
    # FastAPI is a runtime ASGI app, but its `__call__` types the scope as
    # `MutableMapping[str, Any]`, which is not assignable to without's stricter
    # `RawScope = Mapping[str, object]` (parameter contravariance). The cast bridges
    # the two ASGI type universes at this single, documented boundary.
    "fastapi": cast("Callable[[], ASGIApp]", fastapi_todos),
}


async def _serve_without_http(app: ASGIApp, host: str, port: int) -> None:
    async with serving(app, host=host, port=port) as server:
        print(f"listening on {server.host}:{server.port}", flush=True)
        await sleep_forever()


def serve_without_http(app: ASGIApp, host: str, port: int) -> None:
    asyncio.run(_serve_without_http(app, host, port))


def serve_without_http_uvloop(app: ASGIApp, host: str, port: int) -> None:
    # The same without-http server on uvloop instead of stdlib asyncio. Unlike
    # uvicorn, without-http does not own event-loop startup: the caller runs the
    # coroutine (normally `asyncio.run`), so choosing uvloop is a one-line
    # entrypoint change that touches no framework code. That is the whole point of
    # this cell: it isolates the event loop from the HTTP server, so the delta
    # against the plain `without-http` cell is *only* the loop (uvloop's C socket
    # I/O vs the stdlib selector transport's pure-Python `write`).
    with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
        runner.run(_serve_without_http(app, host, port))


def serve_uvicorn(app: ASGIApp, host: str, port: int) -> None:
    uvicorn.run(app, host=host, port=port, loop="uvloop", http="httptools", access_log=False)


SERVERS: dict[str, Callable[[ASGIApp, str, int], None]] = {
    "without-http": serve_without_http,
    "without-http-uvloop": serve_without_http_uvloop,
    "uvicorn": serve_uvicorn,
}

DEFAULT_SERVER: dict[str, str] = {
    "without": "without-http",
    "fastapi": "uvicorn",
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve one (framework, server) benchmark cell on a real socket.")
    parser.add_argument("framework", choices=tuple(FRAMEWORKS))
    parser.add_argument("--server", choices=tuple(SERVERS), default=None)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    serve = SERVERS[args.server or DEFAULT_SERVER[args.framework]]
    serve(FRAMEWORKS[args.framework](), args.host, args.port)


if __name__ == "__main__":
    main()
