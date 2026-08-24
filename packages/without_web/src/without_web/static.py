from __future__ import annotations

from typing import cast

from without import Stream
from without_asgi import DEFAULT_CHUNK_SIZE
from without_asgi import NOT_FOUND
from without_asgi import HttpHandler
from without_asgi import HttpScope
from without_asgi import Inbound
from without_asgi import Inventory
from without_asgi import Outbound
from without_asgi import Response
from without_asgi import serve_asset

from without_web.converters import PATH
from without_web.handlers import _emit
from without_web.patterns import CatchAll
from without_web.router import HttpEndpoint
from without_web.router import Match
from without_web.router import Route
from without_web.router import _segments


def static_files(
    prefix: str,
    assets: Inventory,
    *,
    parameter: str = "rest",
    not_found: Response = NOT_FOUND,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> Route[object]:
    """
    A `GET`/`HEAD` route serving an `Inventory` under `prefix`.

    The catch-all remainder *is* the inventory key, so a request is a dictionary lookup
    and no filesystem path is ever built from it. Everything the response says was
    computed when the inventory was walked; see `without_asgi.inventory` for what that
    settles and what it assumes.

    ```python
    assets = inventory(Path("dist/assets"))
    styles = static_files("/assets", assets)
    router = Router(routes=(styles, *api_routes), fallback=not_found)

    url_for(styles, {"rest": "app.a1b2c3d4.css"})  # -> /assets/app.a1b2c3d4.css
    ```

    The returned `Route` is an ordinary value carrying its complete `segments`, so it
    reverses through `url_for` with no router involved, which is what a template asking
    for an asset's URL needs.

    A catch-all does not match an empty remainder, so the bare `prefix` is itself a
    `404`. That is the right answer: a request for a directory is a listing request, and
    an inventory serves no listings. A single-page app's entry point is not a mount
    either, since it must also answer client-side deep links matching no asset at all;
    that is the router's `fallback`, which can call `serve_asset` for `index.html`.
    """
    endpoint = _endpoint(assets, parameter, not_found, chunk_size)
    segments = (*_segments(prefix), CatchAll(parameter, PATH))
    return Route(segments=segments, methods={"GET": endpoint, "HEAD": endpoint})


# Built by hand rather than as `handle_stream(catch_all(parameter, PATH), http_scope(), ...)`,
# which is the same composition and the shape the docs show elsewhere. That route parses an
# `HttpRequestHead` and runs the extractor set on every request; a static asset needs neither,
# and this is the one endpoint in the package whose whole job is to be cheap per request. The
# cost of the choice is that the route carries no spec, so it contributes nothing to `openapi()`,
# which is the right trade for a stylesheet.
def _endpoint(
    assets: Inventory,
    parameter: str,
    not_found: Response,
    chunk_size: int,
) -> HttpEndpoint[object]:
    def build(_state: object, match: Match[HttpScope]) -> HttpHandler:
        # `PATH.parse` is `str`, so the walk bound a string; the cast is the runtime
        # no-op the router's own extractors use for the same reason.
        key = cast(str, match.params[parameter])

        # The parameter name is part of `Processor`'s protocol, so it stays `inputs`
        # even though a static asset never reads the request body.
        def processor(inputs: Stream[Inbound]) -> Stream[Outbound]:
            return _emit(serve_asset(match.scope, assets, key, not_found=not_found, chunk_size=chunk_size))

        return processor

    return build
