from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import pytest
from fastapi import FastAPI
from integration.todos.app import todos_app
from integration.todos.core import Todo
from integration.todos.core import TodoList
from starlette.testclient import TestClient
from without_asgi import ASGIApp
from without_http import Client
from without_http import request
from without_http import run_lifespan
from without_http.testing import asgi_client
from without_http.testing import loopback_client

# The interop proof, in both directions. without-http's in-memory clients speak only
# ASGI and their own request/response values, so they drive a *foreign* app (FastAPI
# over Starlette) as readily as a `without` one; and a `without` app is a plain ASGI
# app, so the ecosystem's own test tools (httpx's ASGITransport, starlette's
# TestClient) drive it unchanged.
#
# The `type: ignore[arg-type]`s below are the one seam that is not free, and it is
# static only: `without-asgi` types a scope as `Mapping[str, object]` while starlette
# and httpx type theirs as `MutableMapping[str, Any]`. A `dict` satisfies both, so every
# call here runs, but a callable parameter is contravariant, so neither side's app type
# is assignable to the other's. Each ignore is pinned to its code, so it fails the build
# if the ecosystem's annotations ever converge.


def _todos() -> TodoList:
    return TodoList({1: Todo(id=1, title="write", done=False), 2: Todo(id=2, title="ship", done=True)})


def _fastapi_app() -> FastAPI:
    started: list[str] = []

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        started.append("up")
        yield

    app = FastAPI(lifespan=lifespan)

    @app.get("/items/{item_id}")
    async def read_item(item_id: int) -> dict[str, object]:
        return {"item_id": item_id, "started": started}

    @app.post("/echo")
    async def echo(payload: dict[str, str]) -> dict[str, str]:
        return payload

    return app


@asynccontextmanager
async def _in_memory(app: ASGIApp, *, wire: bool) -> AsyncIterator[Client]:
    """One of the two in-memory clients, so a test body can run against both."""
    if wire:
        async with loopback_client(app) as client:
            yield client
        return
    async with asgi_client(app) as client:
        yield client


@pytest.mark.parametrize("wire", [False, True], ids=["asgi", "loopback"])
async def test_an_in_memory_client_drives_a_fastapi_app(wire: bool) -> None:
    app = _fastapi_app()

    async with _in_memory(app, wire=wire) as client:  # type: ignore[arg-type]
        async with request(client, "GET", "http://testserver/items/7") as (head, body):
            assert head.status == 200
            assert json.loads(await body.read()) == {"item_id": 7, "started": ["up"]}

        async with request(
            client,
            "POST",
            "http://testserver/echo",
            headers=((b"content-type", b"application/json"),),
            body=b'{"shape": "round"}',
        ) as (head, body):
            assert head.status == 200
            assert json.loads(await body.read()) == {"shape": "round"}


@pytest.mark.parametrize("wire", [False, True], ids=["asgi", "loopback"])
async def test_an_in_memory_client_drives_the_without_todos_app(wire: bool) -> None:
    async with _in_memory(todos_app(_todos()), wire=wire) as client:
        async with request(client, "GET", "http://testserver/todos/1") as (head, body):
            assert head.status == 200
            assert json.loads(await body.read())["title"] == "write"

        async with request(client, "GET", "http://testserver/todos/404") as (head, _body):
            assert head.status == 404


async def test_httpx_asgi_transport_drives_a_without_app() -> None:
    app = todos_app(_todos())
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    # ASGITransport never runs the lifespan protocol, and a `make_asgi_app` app keeps its
    # state there, so the lifespan is driven around it here. That gap is exactly what
    # `asgi_client` closes by wrapping the same `run_lifespan` a real server uses.
    async with run_lifespan(app), httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/todos/2")

    assert response.status_code == 200
    assert response.json()["title"] == "ship"


def _through_test_client(app: ASGIApp) -> tuple[int, str]:
    # The `with` block is what drives the lifespan.
    with TestClient(app) as client:  # type: ignore[arg-type]
        response = client.get("/todos/1")
        return response.status_code, response.json()["title"]


async def test_starlette_test_client_drives_a_without_app() -> None:
    # TestClient is synchronous and runs the app on its own event loop in a portal
    # thread, so it is driven from a worker thread rather than from this one.
    status, title = await asyncio.to_thread(_through_test_client, todos_app(_todos()))

    assert status == 200
    assert title == "write"
