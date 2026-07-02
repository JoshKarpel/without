from __future__ import annotations

import argparse
import asyncio

import uvicorn
from without import sleep_forever
from without_http import serving

from benchmarks.todos.apps import fastapi_todos
from benchmarks.todos.apps import without_todos

# The two stacks as *long-running server processes*, launched one per benchmark
# run so an external load generator drives a real socket and a sampling profiler
# can attach to the server PID. Each is deployed as it normally would be: the
# without app on `without-http` over stdlib asyncio, the FastAPI app on uvicorn
# with uvloop + httptools. These are process bootstraps, not domain logic, so
# they are exercised by running a benchmark rather than by unit tests.


async def _serve_without(host: str, port: int) -> None:
    async with serving(without_todos(), host=host, port=port) as server:
        print(f"without listening on {server.host}:{server.port}", flush=True)
        await sleep_forever()


def serve_without(host: str, port: int) -> None:
    asyncio.run(_serve_without(host, port))


def serve_fastapi(host: str, port: int) -> None:
    uvicorn.run(
        fastapi_todos(),
        host=host,
        port=port,
        loop="uvloop",
        http="httptools",
        access_log=False,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve one todo stack under test on a real socket.")
    parser.add_argument("framework", choices=("without", "fastapi"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    serve = serve_without if args.framework == "without" else serve_fastapi
    serve(args.host, args.port)


if __name__ == "__main__":
    main()
