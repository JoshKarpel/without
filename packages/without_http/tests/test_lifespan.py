from __future__ import annotations

import pytest
from without_asgi import RawScope
from without_asgi import Receive
from without_asgi import Send
from without_http import LifespanError
from without_http import run_lifespan


async def startup_failure_app(scope: RawScope, receive: Receive, send: Send) -> None:
    if scope["type"] != "lifespan":  # pragma: no cover - these apps are only ever run with a lifespan scope
        raise RuntimeError("this app serves only lifespan")
    await receive()
    await send({"type": "lifespan.startup.failed", "message": "startup exploded"})


async def shutdown_failure_app(scope: RawScope, receive: Receive, send: Send) -> None:
    if scope["type"] != "lifespan":  # pragma: no cover - these apps are only ever run with a lifespan scope
        raise RuntimeError("this app serves only lifespan")
    await receive()
    await send({"type": "lifespan.startup.complete"})
    await receive()
    await send({"type": "lifespan.shutdown.failed", "message": "shutdown exploded"})


async def test_a_reported_startup_failure_raises_before_serving() -> None:
    with pytest.raises(LifespanError, match="startup exploded"):
        async with run_lifespan(startup_failure_app):
            pass  # pragma: no cover - startup failure prevents reaching the body


async def test_a_reported_shutdown_failure_raises_on_exit() -> None:
    with pytest.raises(LifespanError, match="shutdown exploded"):
        async with run_lifespan(shutdown_failure_app):
            pass
