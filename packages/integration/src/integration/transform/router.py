from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import WebsocketHandler
from without_asgi import WebsocketScope
from without_asgi.routing import Middleware

# A small router assembled from without-asgi's tools, which ship the middleware
# vocabulary but no dispatcher. It is generic over the protocol, so one
# implementation serves both HTTP and WebSocket; the differences (HTTP matches on
# method and path, WebSocket on path alone) live in the `Route` predicate, built
# by `http_route`/`ws_route`. An `Endpoint` is without-asgi's `HttpRouter` /
# `WebsocketRouter` shape: it builds the connection handler from the lifespan
# state `T` and the parsed scope `S`.
type Endpoint[T, S, H] = Callable[[T, S], H]


@dataclass(frozen=True, slots=True)
class Route[T, S, H]:
    matches: Callable[[S], bool]
    endpoint: Endpoint[T, S, H]


@dataclass(frozen=True, slots=True)
class Router[T, S, H]:
    routes: tuple[Route[T, S, H], ...]
    fallback: Endpoint[T, S, H]
    middleware: Middleware[T, H, S]

    def dispatch(self, state: T, scope: S) -> H:
        # An `HttpRouter`/`WebsocketRouter`, ready for `make_asgi_app`: pick the first
        # matching route (or the fallback), build its handler, then wrap the whole
        # thing in the middleware so every route shares it. The user pre-composes the
        # middleware (with `stack`) and hands us one; the state and scope are passed
        # alongside so a cross-cutting middleware can read the same `T` the endpoint sees.
        endpoint = next((route.endpoint for route in self.routes if route.matches(scope)), self.fallback)
        return self.middleware(endpoint(state, scope), state, scope)


def http_route[T](
    method: str, path: str, endpoint: Endpoint[T, HttpScope, HttpHandler]
) -> Route[T, HttpScope, HttpHandler]:
    return Route(lambda head: head.method == method and head.path == path, endpoint)


def ws_route[T](
    path: str, endpoint: Endpoint[T, WebsocketScope, WebsocketHandler]
) -> Route[T, WebsocketScope, WebsocketHandler]:
    return Route(lambda head: head.path == path, endpoint)
