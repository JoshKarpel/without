# without-asgi

`without` adapters that turn an [ASGI](https://asgi.readthedocs.io/) application's
`receive`/`send` into typed event streams and back. This package is *only* the
boundary: it parses raw ASGI
event dicts into typed values, encodes typed values back into the dicts a server
expects, and exposes `receive` as a `Stream` and `send` as a `Sink`. Routing and
handlers are left to the application, which hooks processors together in its own
code, and so is middleware apart from the few pieces that need nothing but this
vocabulary and therefore work under any router and any server:
`limit_concurrent_requests`, `limit_request_body`, and
[`compress`](#negotiated-response-compression). The one piece of protocol the adapter does drive is
lifespan (see `make_asgi_app` below), because that is boundary work, not app
policy. See the [`without_asgi` API reference](../without-asgi/reference.md) for
the full surface.

An ASGI app is `async def app(scope, receive, send)`. The adapters let the body
of that callable read as plain `without` wiring:

```python
from without_asgi import http_inbound, http_outbound, parse_http_scope


async def app(scope, receive, send):
    head = parse_http_scope(scope)
    handler = select(head)  # your routing, your processor
    outbound = handler(http_inbound(receive))  # Stream[Inbound] -> Stream[Outbound]
    await http_outbound(send)(outbound)  # drive ASGI send
```

Because `receive` is already pull-based, `http_inbound` is a plain async
generator (no queue): the request's lifecycle *is* the stream's lifecycle, so it
ends on the final body chunk or a disconnect. A handler that wants the whole body
folds that stream with `read_body`, which joins the `RequestBody` chunks and
raises `ClientDisconnect` if the client drops before the final one. `scope`
(method, path) is known once up front, so routing is an ordinary `scope ->
Processor` choice rather than a per-event stream split.

`make_asgi_app(lifespan, http=..., websocket=...)` builds the ASGI app: it drives
the lifespan protocol around a portable `Lifespan[T] = () ->
AbstractAsyncContextManager[T]`, setting the value up on `startup`, tearing it
down on `shutdown` (reporting setup/teardown errors as `lifespan.startup.failed` /
`lifespan.shutdown.failed`). Each connection scope is parsed into a typed
`HttpScope` / `WebsocketScope` and passed to that protocol's *router* with the
value threaded in: an `HttpRouter[T] = (T, HttpScope) -> Processor[Inbound,
Outbound]` (and the websocket equivalent) selects the `Processor` that serves the
connection. `make_asgi_app` then owns the receive/send wiring around it: it wraps
`receive` into the inbound stream, runs the returned `Processor`, and drains its
outbound stream into `send`, so a router and its handler only ever see streams,
never the raw callables or the lifespan scope. It closes the inbound stream when
the handler exits (via `aclosing`), so a handler that abandons the request body
early has its `finally` run deterministically rather than leaving the generator
dangling for GC: the server-side mirror of the client folding connection release
into its response-body generator. Each protocol's router defaults to
one that refuses the connection, so an app serves a protocol only by passing its
own router to override the default; an unserved HTTP scope gets a `501 Not
Implemented` and an unserved WebSocket scope is closed before `accept` (a `403`).
The manual wiring shown above is the drill-under path for a handler that needs the
raw `receive`/`send`.

The `Lifespan` names no ASGI types on purpose, so the same value drives a non-ASGI
shell (a queue processor, a CLI, a test) unchanged; only the wrapper differs.
Interdependent resources compose inside the lifespan with nested `async with`,
which also orders teardown.

Writing a router is opinionated work (what a route matches on, how dispatch falls
back), so this package ships no router. The optional `without_asgi.routing`
submodule provides only the unopinionated tools you assemble one from: a
`Middleware` vocabulary, generic over the connection state `T`, the protocol's
handler, and scope (with `HttpMiddleware[T]` / `WebsocketMiddleware[T]` aliases),
so a middleware wraps a handler with the lifespan state and scope in hand
(`(T, handler, scope) -> handler`); state leads so a cross-cutting middleware can
read the same `T` the handler sees, while one that does not need it ignores the
argument; `stack`, which composes a sequence of middleware into one
(first outermost), so a stack of middleware is itself a `Middleware`; `wrap`, which
builds a middleware from scope-aware inbound and/or outbound stream transformers
(composing them around the handler, so a logging or header middleware is a
one-liner; `wrap` is the scope-only end, so its product ignores `T`); and
`buffered`, which adapts a `(state, scope, body) -> Response`
function into the `HttpRouter` shape for the common request/response case (it reads
as a decorator).
The `integration` package's `transform.router` shows a small
protocol-generic `Router` built from these, dispatching both an HTTP and a
WebSocket route.

For a full, opinionated router you don't have to hand-roll, the sibling
[`without-web`](../without-web/index.md) package provides trie matching with typed path
parameters, 405-vs-404, mounting, scoped middleware, exception handlers, and
OpenAPI. It snaps onto this boundary through nothing but the `HttpRouter` type
(`Router.dispatch` *is* one), so adopting it is opt-in and bring-your-own stays
first-class. The `integration` package's `todos` example is
built on it.

A `Middleware` wraps the whole handler, a `Processor[Inbound, Outbound]`, so it can
transform the inbound stream, the outbound stream, or both. The body is not a
special thing to reach for; it is the `RequestBody` events on the inbound stream
(and `ResponseBody` events on the way out), so a middleware that touches the body
just transforms those events before or after the inner handler. Two shapes:

- **Per-chunk**, which stays streaming: wrap `inputs` and re-`yield` each
  `RequestBody` with its `body` transformed and its `more_body` preserved, passing
  `Disconnect` through. The inner handler still receives the body incrementally.
- **Whole-body**, which buffers: `await read_body(inputs)` to join the chunks (it
  raises `ClientDisconnect` on a truncated body), do the work, then `yield` one
  `RequestBody(body=..., more_body=False)`. The inner handler sees a complete body
  in a single event and cannot tell it was re-synthesized; the tradeoff is that
  buffering forecloses streaming in the handler. The response body is symmetric:
  wrap the outbound stream and transform its `ResponseBody` events, the way the
  `transform` example's header middleware rewrites `ResponseStart`.

The pure half (`parse_http_scope`, `parse_inbound`, `encode_outbound`,
`encode_response`, and the lifespan equivalents) is sans-IO and tested without a
socket: build a `scope`, a scripted `receive`, and a capturing `send`, then call
`app` directly. See the `integration` package for a worked
text-transform service that reads the request body and dynamic config from a
`without-configmap` `Context`.

## `Content`: a body and what it is

Encoding a value produces two things that have to travel together, the bytes and the
`content-type` naming them, and separating them is the same mistake at every call site.
`Content` pairs them, and carries no policy of its own:

```python
from without_asgi import Response, json_content

Response.from_content(201, json_content(todo))
```

Four producers ship, each an encoding both sides of the stack kept re-deriving;
a text or msgpack encoder would be another, with equal standing:

- `json_content(payload, *, dumps=...)` encodes JSON.
- `form_content(fields)` encodes `application/x-www-form-urlencoded`, the shape
  HTML forms POST and OAuth2 token endpoints require. A mapping carries one value
  per name; pass pairs when a name repeats.
- `html_content(markup)` pairs already-rendered markup with
  `text/html; charset=utf-8`. It takes a `str`, so how the markup was produced stays
  the application's business: [`without-html`](../without-html/index.md)'s `render`,
  a template engine, or a literal all arrive here identically, and this package names
  the content type without taking on a renderer.
- `multipart_content(fields, files, *, boundary=None)` encodes
  `multipart/form-data` (RFC 7578), the shape file-upload APIs take: each field a
  text part, each `FilePart(name, filename, body, content_type)` a file part with
  its own type, the boundary named in the `content-type` that travels with the
  chunks. It produces a `StreamingContent` (below), so a `FilePart` whose body is
  a `Stream[bytes]` is re-yielded chunk by chunk and a large upload is never held
  whole. The boundary defaults to a random token; inject one for a
  byte-reproducible body.

For `json_content`, the *encoder* stays an argument, so an
app that needs sorted keys, a faster library, or one that knows its domain types passes
its own and changes nothing else:

```python
json_content(todo, dumps=lambda value: json.dumps(value, sort_keys=True))
```

The stdlib is the default because a default should add no dependency, and it is strict
where JSON is (`allow_nan=False`, so a `NaN` fails at the sender rather than at whoever
parses the response). Key order is left alone: sorting is a policy some callers want and
a cost every response would pay.

A `Content` describes what its bytes *are*; *transforming* an exchange (compressing a
request body, decoding a response) is middleware's job, in `without-http`'s client
vocabulary and in [`compress()`](#negotiated-response-compression) here, where it
streams instead of buffering and applies at whatever scope the
composition happens. A caller who wants one compressed request decorates the client
inline for that call (`gzip_compress()(client)`) rather than wrapping the value.

`Response.from_content(status, content, *, headers=())` layers the caller's headers over
the ones the content described, so a handler answering `application/problem+json` over a
JSON body says so there rather than rebuilding the body. The same value is what
`without-http`'s client takes as a request body (`request(client, "POST", url,
body=json_content(order))`), which is the point of it living here rather than in either
package above.

A `Content` is buffered by design: a value the caller holds whole, which is what
makes it replayable and freely shareable. Its one-shot sibling `StreamingContent`
carries the same headers-with-body pairing around a `Stream[bytes]`, for a body
produced as chunks; `without-http`'s `request` takes either at `body=`, framing a
buffered body with a `content-length` and a streaming one as chunked. The two
collapse in one direction: `await streaming.buffered()` consumes the chunks into
the `Content` they would have been, the request-side mirror of reading a response
body whole. Producers ship stream-first where size is unbounded (multipart), and
buffering stays a one-call convenience.

## Streaming a file

`file_response(path)` builds the outbound stream that serves a file: a
`ResponseStart` with `Content-Type` and `Content-Length` filled in, then the
`ResponseBody` chunks. It returns a `Stream[Outbound]`, so it drops straight into
a handler's output (or a `without-web` `Reply`), and `Response` stays the pure
buffered value it is; streaming is the other, already-existing arm rather than an
iterator smuggled into that value.

It exists because the offloaded-file ASGI extensions
(`http.response.pathsend`, `http.response.zerocopysend`) only pay off when the
transport can push bytes *below* Python (a native `sendfile`), which a pure-Python
asyncio server cannot do; rather than advertise an offload it can't honor, the
reusable-but-fiddly work moves here. That work is: guess the content type, compute
the length, and chunk the bytes off the event loop into the `ResponseBody` events
the framework already streams to `send`, so a large file is never slurped into one
`bytes`.

`file_response` is a coroutine, not an async generator, and the ordering is the
point. Awaiting it runs the `stat` up front, so a missing file raises
`FileNotFoundError` *before* any `ResponseStart` is emitted, while nothing is on
the wire yet and a handler can still answer a clean `404`. This is the
parse-don't-validate move, and precisely the wart `http.response.pathsend` cannot
avoid: its path is opened only *after* the status and headers have already been
sent, so a missing file there can only truncate a response that already claimed
`200`.

```python
from pathlib import Path

from without_asgi import Response, file_response


async def download(state, match) -> Reply:
    try:
        return await file_response(Path("/srv/report.pdf"))
    except FileNotFoundError:
        return Response(status=404, body=b"not found\n")
```

`Content-Type` is guessed from the suffix with `mimetypes.guess_file_type`
(falling back to `application/octet-stream`) and overridable with `content_type`;
any `headers` given are prepended, for a `content-disposition` say. The body is
read in `chunk_size` pieces via `asyncio.to_thread`, matching the package's
`pathlib.Path` + thread-offload file-I/O discipline, and the file is closed when
the stream ends or is closed early (`make_asgi_app` closes an abandoned outbound
stream, e.g. on a client disconnect mid-download). A `HEAD` request needs nothing
special here: `file_response` always streams, and the transport drops the body
chunks on the wire.

`guess_file_type` reports a content *coding* alongside the media type, and both
are used. A `logo.svgz` is gzip-encoded SVG, so it goes out as `image/svg+xml`
with `Content-Encoding: gzip`; dropping the coding is how gzip bytes come to be
labelled as an image a browser then tries to render. The coding is claimed only
where a media type came with it, so a bare `archive.gz` guesses `(None, "gzip")`
and stays an opaque `application/octet-stream` download. An explicit
`content_type` suppresses the coding entirely, since a caller naming the type is
describing the bytes as they are.

`file_response` answers `200` and nothing else, and that is its job rather than a
gap. It serves content with no cacheable identity: a report just rendered, a
temp file zipped for this one response. Such a file's validator changes on every
request, so a conditional request buys nothing, and advertising `Accept-Ranges`
invites a follow-up for a file that may already be gone. For a file that
persists, `serve_file` answers `Range` and conditional requests; for a tree of
assets, an `Inventory` does.

Reads and writes are lockstep by default: the next chunk is read only once the
consumer has drained the current one, so disk and socket never overlap. Because
the chunks are an ordinary `Stream`, read-ahead is opt-in composition rather than
a built-in, `spool` from the core:

```python
from without import spool

return spool(await file_response(path), ahead=2)
```

`spool` drives the file reads up to `ahead` chunks ahead of the socket writes
through a bounded queue on a background task, so the next `read` overlaps the
current chunk's send; `ahead` still bounds the memory held and applies
backpressure.

## Conditional and range requests

A resuming download, a media player seeking, and a cache revalidating all need
two mechanisms: byte ranges (`Range` to a `206` with a `Content-Range`, or a
`416` when unsatisfiable) and conditional requests (`If-None-Match` or
`If-Modified-Since` to a `304`).

The decision is a pure function, `selection_for`, and it is the whole of the
logic:

```python
selection_for(
    size=stat.st_size,
    method=scope.method,
    request_headers=scope.headers,
    etag=validator,
    last_modified=modified,
) -> Whole | Head | Span | NotModified | Unsatisfiable
```

Nothing in that signature mentions a file. It takes a size, two validators, and
the request's headers, so the same decision serves bytes from a disk, from
memory, or from anywhere else, and the whole of
[RFC 9110 §13](https://www.rfc-editor.org/rfc/rfc9110#section-13) and
[§14](https://www.rfc-editor.org/rfc/rfc9110#section-14) tests as a table with no
filesystem in sight. `serve_file` and `serve_asset` are the shells that do the
`stat` and the reads.

The ordering that makes `file_response` able to answer a clean `404` is what
makes this work too: the `stat` runs on the `await`, so a `304` and a `416` are
both decided while nothing is on the wire.

`Head` is why the union has five arms rather than four. A `HEAD` answered as
`Whole` reads the entire representation and streams it for the transport to
discard frame by frame, so `curl -I` and every uptime check cost a full read of
whatever they name. `Head` announces exactly what a `200` would, `Content-Length`
included (§9.3.2), and sends nothing.

`serve_file(scope, path)` is `file_response`'s request-aware sibling for one
named file. Its derived validator is _weak_ (`W/"<size>-<mtime>"`), because a
filesystem's timestamp granularity can be coarser than the interval between two
writes, so two different bodies can share a size and an `st_mtime_ns`. A weak
validator fails the strong comparison `If-Range` requires (§13.1.5), so a
resumed download correctly restarts rather than splicing bytes from two
versions. Pass `etag` when you hold something better.

Only **single** ranges are honored. A multi-range request needs a
`multipart/byteranges` body, which is most of the implementation cost for a case
almost nothing sends, and it is the shape behind both
[CVE-2011-3192](https://httpd.apache.org/security/CVE-2011-3192.txt), where
Apache built one copy of the resource per range, and
[CVE-2025-62727](https://github.com/Kludex/starlette/security/advisories/GHSA-7f5h-v6xp-fcq8),
where Starlette merged ranges in quadratic time. §14 permits a server to ignore
a `Range` it does not want to honor, so answering with the whole representation
is conformant, and the check is a scan for a comma rather than a split: a header
naming a hundred thousand ranges costs one linear pass and allocates nothing.

## Serving a tree of assets

`inventory(root)` walks a directory once and returns a mapping of request key to
`Asset`; `serve_asset(scope, assets, key)` answers out of it. The walk is the
whole of the setup, so it runs once wherever the app is assembled, and the key is
whatever the app's routing has left of the path:

```python
from collections.abc import AsyncIterator
from contextlib import nullcontext
from pathlib import Path

from without import Stream
from without_asgi import (
    HttpHandler,
    HttpScope,
    Inbound,
    Outbound,
    inventory,
    make_asgi_app,
    serve_asset,
)

PREFIX = "/assets/"
ASSETS = inventory(Path("dist"), cache_control=b"public, max-age=31536000, immutable")


def serve(state: None, scope: HttpScope) -> HttpHandler:
    key = scope.path.removeprefix(PREFIX)

    async def handler(inputs: Stream[Inbound]) -> AsyncIterator[Outbound]:
        async for event in await serve_asset(scope, ASSETS, key):
            yield event

    return handler


app = make_asgi_app(nullcontext, http=serve)
```

That app answers a `200` with the media type, validators, and `cache-control` the
walk computed, a `304` to an `If-None-Match` holding the tag it just handed out,
a `206` with a `Content-Range` to a `Range`, and the gzip variant with its _own_
strong tag to a client that accepts it.

The walk is an ordinary blocking call at import here, which is what `nullcontext`
in the lifespan slot says out loud: this app sets nothing up, so there is no state
to thread and the router ignores the argument. An app factory is the same shape
with `ASSETS` moved into a closure. Where the app has a real lifespan, build the
inventory there instead and let `make_asgi_app` hand it to the router per
connection, which is also the one place the walk needs `asyncio.to_thread`, since
by then the event loop is running and the walk hashes every file:

```python
@asynccontextmanager
async def lifespan() -> AsyncIterator[Inventory]:
    yield await asyncio.to_thread(
        inventory,
        Path("dist"),
        cache_control=b"public, max-age=31536000, immutable",
    )


def serve(assets: Inventory, scope: HttpScope) -> HttpHandler:
    # Exactly as above, closing over `assets` rather than the module-level name.
    ...


app = make_asgi_app(lifespan, http=serve)
```

What that buys beyond placement is one thing: teardown and rebuild ride the app's
own cycle, which is what a process swapping in a fresh inventory wants. What
matters either way is that the walk happens once at assembly rather than per
request.

The key is a `removeprefix` and nothing else. A path outside the prefix, a `..`, a
decoded `%2F`, or a Windows device name all arrive as keys the mapping does not
hold, which is the `not_found` response by omission rather than by check, for the
[reason below](#there-is-no-directory-traversal).

`serve_asset` is awaited before its events are iterated, which is `file_response`'s
ordering and load-bearing for the same reason: the content negotiation, the
conditional and range decision, and the `stat` that checks the recorded size all run
on the `await`, so a `304`, a `416`, a `404`, or an `AssetChanged` is settled while
nothing is on the wire. The handler's `inputs` goes unread, since a static asset has
no request body to fold, and the parameter stays because it is `Processor`'s shape.

Restricting the method is routing work and is deliberately not done here: anything
that is not `GET` or `HEAD` skips the conditional and range rules entirely and is
answered with the whole representation, so a `POST` to that router gets the asset.
[`without-web`](../without-web/index.md#static-files)'s
`static_files(prefix, assets)` is this router as a `Route` bound to `GET` and
`HEAD`, adding the prefix match and the `url_for` reversibility only a router can.

### There is no directory traversal

This is the design decision the rest follows from, so it is worth stating
plainly. The usual shape of "static file serving" derives a filesystem path from
attacker-controlled input on every request, and then tries to _prove_ the
derivation stayed inside the root. Nearly every implementation has gotten that
proof wrong at least once:

- [CVE-2023-29159](https://github.com/Kludex/starlette/security/advisories/GHSA-v5gw-mw7f-84px),
  where `os.path.commonprefix` compared characters rather than path components,
  so `/static/../static1.txt` read as inside the root.
- [CVE-2024-23334](https://github.com/aio-libs/aiohttp/security/advisories/GHSA-5h86-8mv2-jq9f),
  where a `follow_symlinks` flag skipped the containment check outright and
  `../../../etc/passwd` worked with no symlink present at all.
- [Werkzeug #1589](https://github.com/pallets/werkzeug/issues/1589), where
  `/static/c:/windows/win.ini` escaped on Windows with no `..` anywhere, because
  joining discards the base when the second component is absolute.
- [CVE-2025-66221](https://explore.alas.aws.amazon.com/CVE-2025-66221.html) and
  [CVE-2026-21860](https://advisories.gitlab.com/pkg/pypi/werkzeug/CVE-2026-21860/),
  where Windows reserved device names (`CON`, then `CON.txt`) opened
  successfully and hung on read, the second being the incomplete fix for the
  first.

An inventory never derives a path from input. The key selects among values
computed at startup, so a `..`, a decoded `%2F` or `%00`, an absolute path, a
drive letter, and a device name are all just keys that are not in a dictionary.
There is no proof to get wrong, because there is no derivation. This is
parse-don't-validate reaching the filesystem: the illegal state is not rejected,
it is unrepresentable.

Every decision that a traversal design makes per request, with an attacker in
the loop, is made here once, over a tree the operator assembled. Only regular
files are admitted. Each entry is resolved and confirmed to be inside the root,
and one that escapes raises, naming both ends; no flag relaxes that, because
that flag is precisely aiohttp's CVE. Symlinked directories are not descended
into, so a cycle cannot hang the walk. A directory that cannot be read raises
too, rather than contributing nothing: `Path.walk` ignores a failed `scandir` by
default, which would leave the inventory silently short every asset beneath it
and answer `404` for them in production. A directory has no key at all, which is
a `404` by omission rather than by check, and there is no directory listing and
none behind a flag.

### Index files and the trailing slash

`index="index.html"` aliases a directory's key to the index inside it, so
`/guide/` reaches `guide/index.html`. `without-web`'s `split_path` strips a
trailing slash, so `/guide` and `/guide/` arrive as the same key and `serve_asset`
is the only place left that can tell them apart, from `scope.path`. The
slash-less form gets a `302` to the slashed one rather than the document itself:
served there, every relative link and asset reference inside the page would
resolve against `/` instead of `/guide/`, one level too high. The `Location` is
relative (just the final segment plus a slash), because an inventory does not
know what prefix it was mounted under, and it is a `302` rather than a `301` for
the same reason: the URL shape it points at is not the inventory's to make
permanent. This is WhiteNoise's `redirect` decision, and for the same reasons.

### The assumption, and how it is checked

Nothing may write into the tree while the app is running. That is what "static"
means, and it is not enforced by file permissions: modes change, root ignores
them, and container layers get remounted, so a permission check would look like
enforcement while providing none.

It is _detected_ where it matters. When identity bytes are owed the `stat` runs
before any `ResponseStart`, and a size that disagrees with the inventory raises
`AssetChanged` while nothing is committed, rather than framing a body whose
length and validator describe different bytes.

The inventory is a value, so a development loop that wants to pick up edits
builds a new one and swaps it, on a timer or a filesystem watch, which is
control-plane work rather than anything on the request path.

### Validators

`content_hash` is the default: a digest of the bytes, strong on its own merits.
Unlike a timestamp-derived tag it does not change when a rebuild rewrites an
unchanged file, so clients do not refetch a bundle that did not change, and it
is identical across replicas and machines. `size_and_mtime` costs nothing to
compute for a tree too large to read at startup, and rests entirely on the
no-writes contract instead. Neither includes `st_ino`, which would leak a
filesystem internal into every response, as Apache's `FileETag` default did in
[CVE-2003-1418](https://www.tenable.com/plugins/nessus/88098). A custom
`etag_for` returns the opaque token and the quoting is added here, so a caller
cannot emit a malformed validator.

### Pre-compression

For each asset whose media type is worth encoding, a variant is built per coding
in `encodings`, preferring a sidecar the build system already produced
(`app.css.br`, `app.css.gz`, `app.css.zst`, the convention nginx's
`brotli_static` and WhiteNoise use) and compressing in memory only when one is
missing or older than the asset it encodes. A missing sidecar is logged at
`WARNING`, because the compression level worth using for bytes compressed once
and served forever is far slower than one worth paying per process start:
brotli at quality 11 runs at roughly a megabyte a second, so it belongs in the
build, and a startup cost is paid again by every replica.

Each coding carries its **own** strong validator. Sharing one tag across codings
is a real defect rather than an untidiness: a client holding the gzip copy would
send `If-None-Match`, receive a `304`, and go on using bytes that are a
different representation entirely. `Vary: Accept-Encoding` goes only on assets
that actually have variants, since stamping it on an already-compressed image
fragments every downstream cache key for nothing.

A `304` repeats the variant's `Content-Encoding` alongside the validators RFC
9110 §15.4.5 requires, which is a floor rather than a ceiling. Without it a
downstream [`compress`](#negotiated-response-compression) cannot tell a `304` for
an already-encoded
variant from one it would itself have encoded, so it weakens a validator that is
still exactly true of the stored bytes, and the client's next `If-Range`, which
requires strong comparison, refetches the whole asset.

A sidecar is suppressed by a **fixed** suffix set (`.gz`, `.zst`, `.br`), not by
the codings currently configured. Keying it on the live table would mean a
coding's absence exposes its sidecars: with only gzip configured an `app.css.br`
beside `app.css` becomes an asset of its own, brotli bytes labelled `text/css`
with no `Content-Encoding`, at a URL the build system's own naming makes
guessable, and with `encodings={}` every sidecar in the tree is published that
way. This is why WhiteNoise's `is_compressed_variant` tests a literal
`(".gz", ".br")` rather than anything configurable. A coding outside the table
still suppresses its own sidecars, so a custom one loses nothing.

A file whose own suffixes name a coding (`logo.svgz`, `bundle.tar.gz`) is already
encoded: it is served with that `Content-Encoding`, is not encoded again, and
carries no `Vary`, because it has one representation and negotiates nothing.

Holding the encoded bytes in memory also buys something on-the-fly compression
cannot offer at all: a `Range` over a compressed asset. The `compress`
middleware must skip every `206`, because it has no way to restate a
`Content-Range` computed over identity bytes; a pre-compressed variant has no
such problem, since the range is a slice and `Content-Range` names offsets into
the representation actually being sent.

Compressing static assets is safe from BREACH, which needs a response that both
reflects attacker-controlled input and carries a secret; a stylesheet does
neither. That is why this uses `DEFAULT_COMPRESSORS` rather than the padded
table this package ships for credential-bearing responses.

## Negotiated response compression

`compress()` is an `HttpMiddleware` that reads the request's `accept-encoding`,
picks a content coding, and encodes the response body with it. It is the
server-side mirror of `without-http`'s client `decompress()`, the same mechanism
pointed the other way, and it lives here rather than in the server for the reason
[no ASGI server implements one](../without-http/alternatives.md#operations):
the decision needs the response's media type and the request's headers, not
anything about the socket.

```python
from without_asgi.compression import compress
from without_asgi.routing import stack

middleware = stack(compress(), catching(recover))
```

`compressors` is a mapping from coding to a `Compressor` factory, defaulting to
brotli, zstd, and gzip. What is negotiated is *derived from its keys*, so what can
be produced and what is offered cannot disagree, and **its order is the server's
preference**, settling a tie between codings the client weighted equally. Best
ratio leads, which is the order to want because a client only offers what it can
decode: brotli's built-in text dictionary wins on the HTML and JSON most responses
are, zstd is the cheapest of the three to encode and decode but newer on the
client, and gzip is what everything understands.

Brotli is a dependency rather than an opt-in extra because it makes no choice for
anyone: the stdlib has none and Google's bindings are the implementation, so
declining it would buy nothing and cost every user an assembly step (see
[the philosophy on dependencies](../philosophy.md#a-dependency-is-a-choice-so-take-only-the-ones-that-arent)).
Its quality defaults to `DYNAMIC_BROTLI_QUALITY` (5) rather than the bindings' own
11: a table entry encodes a response per request, where 11 costs much more CPU
without a ratio to show for it at response sizes (on a 2 KB JSON body and a 2.7 KB
HTML one, quality 5 matched or beat 11 outright, since 11's wider window has little
to find in a few kilobytes). Raise it for bodies large enough to pay for that
window, or register a coding that does not ship, by extending the table rather than
forking:

```python
from without_asgi.compression import DEFAULT_COMPRESSORS, brotli_compressor, compress

compress(DEFAULT_COMPRESSORS | {b"br": lambda: brotli_compressor(11)})
```

### What the client can ask for

`negotiate_coding(accept_encoding, available)` is the negotiation on its own: a
pure function of the header and the codings the server has, implementing
[RFC 9110 §12.5.3](https://www.rfc-editor.org/rfc/rfc9110#section-12.5.3) whole.
Weights are the part usually skipped, and skipping them is not harmless: a client
writing `gzip;q=0` is *refusing* gzip, and a server matching on substrings reads
it as asking for it. Here `q=0` excludes, the highest non-zero weight wins,
`*` matches every coding not named, and `identity` outranking the alternatives
(named or reached through a wildcard) means no coding at all.

Two of its answers are choices rather than requirements. A request with **no**
`accept-encoding` is answered unencoded, though rule 1 of that section would
permit any coding: a request that never mentions the field is more often a client
that does not decode than one that quietly would, and an over-eager coding is a
broken response where a missed one is only a larger one. Note that an *empty*
`accept-encoding` is a different request, and that one does say identity. And a
request that marks identity unacceptable while accepting nothing the server has
is still answered with identity rather than a `406`, since failing a request over
a preference serves nobody.

### What gets compressed

Three properties of the *response* decide candidacy, before any client preference
is consulted: the status has to be one a coding can apply to, a response already
carrying `content-encoding` is already encoded, and the media type has to be worth
the CPU. That last is `is_compressible`, an allowlist (`text/*`, the `+json` /
`+json-seq` / `+xml` / `+yaml` structured syntax suffixes, and a short list of the
rest) in the shape
nginx's `gzip_types` takes, so an unrecognized type yields a larger response rather
than cycles spent re-compressing a JPEG. `text/event-stream` is excluded despite
being `text/`, for a reason about the connection rather than the bytes: an event
stream is the one response held open for as long as the client stays, so every event
on it is encoded against a window holding every event before it, and an attacker who
can inject one event reads the length of the next. That is
[BREACH](#compression-and-secrets) with as many samples as it cares to take, on a
connection it never has to re-establish. That exclusion is keyed on the media type
because a media type is all the predicate sees, while the exposure belongs to
*streaming*, which the [secrets section](#compression-and-secrets) picks up. Pass your
own predicate as `compressible` to decide differently.

Three statuses are excluded, each for its own reason. `204` and `304` carry no
content, so there is nothing to encode. A `206` does carry bytes, but they are a
range of the *identity* representation and its `content-range` names offsets into
that representation, which the middleware has no way to restate for an encoded one:
encoding the range would leave the field describing bytes the client no longer
holds, and a client reassembling several ranges would stitch them at the wrong
offsets.

Every candidate gets `Vary: Accept-Encoding` whether or not *this* client got an
encoded body, because candidacy is a property of the resource and a shared cache
has to key on the header that decides the answer; a non-candidate gets none, since
nothing about it varies. The `304` is the exception among the excluded statuses:
it updates the stored `200` it revalidates, and
[RFC 9110 §15.4.5](https://www.rfc-editor.org/rfc/rfc9110#section-15.4.5) asks it
to carry the fields that `200` would have, naming `Vary` because that is how a
shared cache picks the stored variant to update. Which stored `200` that is, the
same section leaves mostly unsaid: its field list omits `content-type` and most
`304`s arrive without one, so the middleware assumes the candidate, on the grounds
that a cache keyed on a header the representation does not really vary by shares a
little less while one missing a header it does vary by hands a client an encoding it
cannot read. A `304` that *does* say what it revalidates settles it instead, and one
naming a type no coding applies to, or a `content-encoding` the app applied itself,
is left exactly as it arrived.

A strong `etag` is weakened to `W/` when the body is encoded, since
[RFC 9110 §8.8.1](https://www.rfc-editor.org/rfc/rfc9110#section-8.8.1)
defines a strong validator as one that changes whenever the content does. Weakening
rather than dropping keeps it true in the way it still is: the two representations
stay semantically equivalent, so a conditional request still matches under weak
comparison while a `Range` request, which needs strong comparison, correctly stops.

The `304` inherits that weakening, but only for a client whose `accept-encoding`
negotiates a coding, and only where the stored `200` is one the middleware would have
encoded. The stored entry such a `304` updates is the one its `Vary` key selects, so
it is the encoded variant, and
[RFC 9111 §4.3.4](https://www.rfc-editor.org/rfc/rfc9111#section-4.3.4) has the cache
copy the `304`'s fields onto it. A strong tag landing there undoes the weakening: a
later `If-Range` matches under strong comparison, and the `206` the middleware never
encodes hands back identity bytes to stitch into an encoded body. Weakening where
nothing was encoded is the same error pointed the other way: a `video/mp4` is stored
as the identity bytes its strong tag was stated for, so a `W/` copied onto it breaks
every later range request into a full response for a re-encoding that never happened.
The size a `304` may also state, which
[RFC 9110 §8.6](https://www.rfc-editor.org/rfc/rfc9110#section-8.6) allows and reads as
the size of the selected representation, is deliberately *not* read as the same kind of
evidence, though a `200`'s own `content-length` answers the
[size floor](#how-big-is-big-enough). It would only prove the stored body went out
unencoded where nothing under `minimum_size` is ever encoded, which is
`weigh_undeclared_bodies` rather than the default, and what it buys is a strong
validator on a representation too small for anyone to ask a range of.

Where the tag is weakened, any `content-length` the `304` carried goes with it. §8.6
permits one only where it equals what a `200` to the same request would have carried,
and for this client that `200` is the encoded variant, so the identity size the app
stated is no longer that number. A client that negotiates nothing holds the identity
representation either way, so both its validator and its stated size are left exactly
as the app wrote them.

### How big is big enough

`minimum_size` (500 bytes by default) is the floor below which gzip's framing costs
more than the text saves, and it is the *only* floor. What decides how it gets
answered is what the head said, not how the body arrives:

- a declared `content-length` answers it before a single body event is read
- a body that ends in the events read so far answers it exactly, from its own bytes,
  and is re-described with an exact `content-length` for its encoded form rather than
  falling back to chunked, unless its head announced trailers over HTTP/1.x, which
  carries trailers only in the chunked coding, so a length would strand them (HTTP/2
  and HTTP/3 send trailers as a second HEADERS frame, which sits beside a length, so
  announcing them costs nothing there)
- a body still being produced behind a head that declared no length is the one case
  that cannot be answered without holding bytes the app has already made

An empty body is left alone however low the floor, since encoding nothing produces
pure framing and the head would then state *that* length. A `HEAD` response is where
that shows: its head describes the body a `GET` would carry while its own body is
empty, so the app's `content-length` would be replaced by the size of an empty encoded
stream.

`weigh_undeclared_bodies` decides that last case, and it is a policy rather than a
second floor because the only two honest answers are to hold or not to. Holding means
keeping produced bytes until `minimum_size` of them accumulate, so a feed emitting a
line a second delivers nothing for as many seconds as that takes, and how long that
is belongs to the app rather than to the middleware. The default does not hold: the
middleware commits on the first non-empty chunk, drops the `content-length` it can no
longer state, and streams the rest through the compressor chunk by chunk. That spends
framing bytes on a body too small to earn them, bounded by the floor, which is the
trade a response the app chose to stream usually wants.

An app that wants both, incremental delivery *and* the floor, declares a
`content-length`, which answers the floor for nothing. `file_response` does: it stats
the file before emitting anything and then yields it chunk by chunk, so a stylesheet
gzip could only grow is weighed exactly like a buffered one and goes out untouched.

### What a committed stream demands of a codec

Streaming past the floor means ending a block per chunk, and that is a demand on the
codec rather than a detail of the loop. What a `compress` call returns is the codec's
choice, not the caller's: fed the small pieces a streaming body arrives in, zlib
emits its 10-byte header and then nothing until the stream ends, and zstd emits
nothing at all. Left that way the whole body accumulates inside the codec and lands
on the client as one burst at the end, which round-trips perfectly and has quietly
removed the incremental delivery the response was streamed for. `StreamingCompressor`
is the `Compressor` that can be flushed without being ended, and `gzip_compressor`,
`zstd_compressor`, and `brotli_compressor` are the shipped factories that produce one.
Build a table entry from `zlib.compressobj` or `zstd.ZstdCompressor` directly and you
get the lesser protocol, since both spell a block flush as a mode argument to `flush`
rather than a method of its own. A coding whose factory produces a plain `Compressor`
still encodes responses that arrive whole, and its streaming ones go out unencoded
instead: that costs bytes, where encoding them would cost the delivery.

An offloaded body is the one shape that cannot follow a commitment. The
`http.response.zerocopysend` and `http.response.pathsend` extensions both send bytes
the middleware never sees, and `zerocopysend` carries `more_body` precisely so it can
follow body events the app has already sent. Arriving *before* any body event, an
offload passes through and the response goes out unencoded. Arriving after the head
has declared `content-encoding`, there is no correct response left to write: the
offloaded bytes are not encoded, and appending them to the encoded stream produces a
body no decoder can read. `compress` raises `OffloadedBodyAfterEncoding` rather than
writing it, so what reaches the client is a truncated response, which every transport
already signals as one. An app that means to stream a prefix and then offload the
rest sends the whole response through the offload instead.

A server push is the other event that can land mid-body, and it decides nothing about
the encoding: `http.response.push` may be sent any time after the head and before the
final body event, so one can arrive while the floor is still being weighed. It waits
with the held head rather than going out ahead of it, and is released in the order the
app sent it, whichever way the floor resolves.

### Compression and secrets

Compressing a response that mixes a secret (a CSRF token, a session identifier)
with attacker-influenced text leaks the secret through the response's *length*.
That is the BREACH attack, and it is not a gzip quirk: every coding in the default
table is an LZ77 family compressor whose output shrinks when a guess matches the
secret, which is the whole oracle. Measured against a page reflecting a guess 50
times beside a token, gzip narrowed the next character to two candidates and
brotli picked it out uniquely; brotli leaks *fastest*, because the wider window and
built-in dictionary that make it compress best also make it match best.

`PADDED_COMPRESSORS` is the mitigation, and `compress` takes it like any other
table:

```python
from without_asgi.compression import PADDED_COMPRESSORS, compress

sensitive = mount("/account", compress(PADDED_COMPRESSORS))
```

This is [Heal The Breach](https://ieeexplore.ieee.org/document/9754554) (Palacios
et al., IEEE Access 2022), the mitigation Django adopted in 4.2. Each response
carries a random-length run of bytes in a part of the container the decoder is
required to ignore, so the length stops being a function of the content alone and
an oracle has to average the noise away before it reads anything. The paper puts
the delay that imposes at roughly 500x for a 10-byte budget and 500,000x for
`MAX_RANDOM_BYTES` (100, the default here and Django's), against a cost of about
half the budget per response.

The padding is per *container*, which is why that table is shorter than
`DEFAULT_COMPRESSORS` rather than a padded copy of it. gzip has the optional
filename field after its fixed header ([RFC 1952 §2.3.1](https://www.rfc-editor.org/rfc/rfc1952#section-2.3.1)),
zstd has skippable frames ([RFC 8878 §3.1.2](https://www.rfc-editor.org/rfc/rfc8878#section-3.1.2)),
placed *after* the data rather than before it because a decoder is entitled to stop
at the end of the frame it just read, and the stdlib's `ZstdDecompressor` does:
padding that led would hand it an empty body with the whole payload stranded in
`unused_data`. Brotli's bindings expose only `process`, `flush`, and `finish`, with no
metadata block to write into and no concatenation to prepend one as. Dropping `br`
is the design rather than a gap: a table where one coding silently went unpadded
would promise a guarantee it does not keep, and it would be the coding browsers
reach for first.

Two things follow. Padding raises the sample count an attack needs rather than
removing the leak, so it is defense in depth, not a reason to stop asking which
responses reflect input back beside credentials. And because middleware coverage
is decided by *where* it is mounted (`without-web`'s `mount` and `with_middleware`
scope it to a prefix or a single route), both answers are route-scoped: pay the
padding and lose brotli where secrets and reflections meet, and keep
`DEFAULT_COMPRESSORS` everywhere else.

Streaming is its own exposure, and neither answer covers it. A committed stream ends
a block per chunk, so each chunk the app produces carries its own observable length:
an attacker reads the length of the part holding the secret rather than of the whole
response, which is a *cleaner* oracle than the buffered case, and padding does not
blunt it, since a padded container carries one random run for the whole response and
none of the chunks behind the first. That is what `is_compressible` excludes
`text/event-stream` for, but the property is the streaming rather than the media
type: a streamed `text/html` page, which is where a CSRF token most often sits, or an
`application/x-ndjson` feed is exposed the same way, and an event stream differs only
in staying open to be sampled without re-establishing. Where a streamed response
mixes a secret with reflected input, keep it off this middleware or pass a
`compressible` that rejects its type.

## The codec runs both directions

Everything above is the *app* side of the boundary: parse the dicts an ASGI
server hands an app (`parse_*`), encode the typed values the app sends back
(`encode_outbound`, `encode_lifespan_reply`). The vocabulary is also complete in
the *server* direction, which is what a transport provider needs to drive an app:

- `encode_scope` (and `encode_http_scope` / `encode_websocket_scope`) renders a
  typed scope into the dict an app expects, the dual of `parse_scope`.
- `encode_inbound` / `encode_websocket_inbound` / `encode_lifespan_event` build
  the dicts an app's `receive` returns, the duals of the `parse_*` events.
- `parse_outbound` / `parse_websocket_outbound` / `parse_lifespan_reply` classify
  the dicts an app passes to `send`, the duals of the `encode_*` reply encoders.

So the same typed vocabulary parses and encodes in both directions, and a server
that owns the wire can work in typed values at the boundary rather than raw dicts.
The sibling [`without-http`](../without-http/index.md) package is exactly that: an ASGI
server built on `h11`/`h2`/`wsproto` that uses these server-direction codecs to
talk ASGI to any app, `make_asgi_app`-built or third-party.
