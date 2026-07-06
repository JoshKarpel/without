# without-web

An opinionated HTTP and WebSocket router for
[`without-asgi`](../without-asgi). `without-asgi` deliberately ships *no* router,
only the unopinionated boundary and composition tools. `without-web` is the
opinionated layer on top: t-string patterns with typed parameters, typed request
extraction, 405-vs-404, mounting, scoped middleware, exception handlers, reverse
routing (`url_for`), and OpenAPI.

It snaps onto the boundary through nothing but the existing `HttpRouter` type:
`Router.dispatch` *is* an `HttpRouter[T]`, so `make_asgi_app(http=router.dispatch)`
just works, and bring-your-own (or no router at all) stays first-class.

```python
import json

from without_asgi import Response, make_asgi_app
from without_web import INT, Router, get, path_param

uid = path_param("id", INT)              # one token: a pattern segment AND a typed read

@get(t"/users/{uid}", uid)               # t-string pattern; `@get` returns a Route value
async def show_user(state, user_id: int):    # user_id is an int, no `assert isinstance`
    body = json.dumps({"id": user_id}).encode()
    return Response(status=200, headers=((b"content-type", b"application/json"),), body=body)

router = Router(routes=(show_user,), fallback=not_found)
app = make_asgi_app(lifespan, http=router.dispatch)
```

Matching is a pure walk of an immutable trie, so precedence, 405-vs-404, and
mounting fall out of the structure. Converters and extractors *parse, they don't
validate*: a path param arrives already typed, and OpenAPI is a *merge* of the
self-descriptions each layer recovers from structure rather than a blob declared
by hand. Encoding stays the app's choice, so the router ships no
`json_response`-style helper.

See the
[`without-web` guide](https://without.help/guides/without-web/)
(with the [API reference](https://without.help/reference/without_web/))
for the full surface: patterns, extractors, streaming input, mounting,
middleware, exception handlers, reverse routing, OpenAPI, and WebSocket routing.
