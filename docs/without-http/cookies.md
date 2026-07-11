# Cookies

`without-http`'s client carries cookies with two collaborating pieces: a
`CookieJar` value you own, and the `cookies(jar)` middleware that reads and
writes it. This page covers how the jar scopes, matches, admits, and *expires*
cookies. For where the middleware sits among the others and *why* the jar is a
value you pass rather than state hidden in the pool, see the
[client middleware](index.md#client-middleware) section of the guide.

## The jar is a value you own

A `CookieJar` is the canonical "mutable identity you pass explicitly" rather
than hide in the pool: cookie scope (application identity) stays independent of
connection reuse (transport). You construct a jar, hand it to `cookies(jar)`,
and *which requests share it* decides what shares cookies, one jar per logical
session regardless of how connections are pooled. Two requests share cookies
exactly when they share a jar.

```python
from without_http import ConnectionPool, CookieJar, cookies, follow_redirects, stack

jar = CookieJar()
async with ConnectionPool() as pool:
    async with pool.request(
        "GET", url, middleware=stack(follow_redirects(), cookies(jar))
    ) as (head, body):
        ...
```

Place `cookies` *inside* `follow_redirects` in the `stack` so each redirect hop
both sends the jar's cookies and collects any the hop sets.

## Two ways in: `store` and `add`

The middleware fills the jar from responses via `store`, which parses
`Set-Cookie` off an *untrusted* response and so enforces the origin guards
below. To seed a cookie you already hold (a session token, a test fixture),
`jar.add(name, value, domain=...)` places it directly: because the caller
vouches for it, `add` skips those guards. An entry with the same
`(domain, path, name)` identity replaces any earlier one.

## Matching

A stored cookie is sent on a request only when host, path, and transport all
match:

- **Host.** A cookie set without a `Domain` is *host-only*: it matches only the
  exact host that set it. A cookie with `Domain=example.com` matches that host
  and every subdomain (`api.example.com`). For a hand-seeded cookie,
  `add(..., subdomains=True)` selects the subdomain-matching form; the default
  is host-only.
- **Path.** A cookie's `Path` matches a request path equal to it or below it,
  with a `/` boundary (so `Path=/app` matches `/app` and `/app/x` but not
  `/application`). A cookie set without a `Path` defaults to the directory of
  the request that set it (the request path up to its last `/`).
- **Transport.** A `Secure` cookie is sent only over an `https` request.

When several cookies match, they are ordered longest-`Path`-first in the
`Cookie` header, following RFC 6265.

## Origin guards (`store` only)

`store` treats the response as untrusted and rejects a `Set-Cookie` that
oversteps. `add` applies neither guard: it is the trusted path.

- **Domain scope.** A response may set only a `Domain` equal to or a parent of
  its own host (RFC 6265 §5.3), so `evil.example` cannot set a cookie for
  `victim.com`. A `Domain` with no internal dot (a bare TLD like `com`, or
  `localhost`) is rejected outright as a stand-in for a public-suffix check,
  blocking a domain-wide supercookie. Full public-suffix-list coverage
  (rejecting e.g. `co.uk`) is not yet implemented.
- **`Secure` over cleartext.** A `Secure` cookie offered over a plaintext
  (`http`) response is dropped, so a network attacker on the cleartext hop
  cannot plant a cookie the client will later send over TLS.

## Expiration

Every stored cookie carries an optional expiry, always an *aware UTC* instant so
comparisons against the jar's clock are offset-safe. The jar sets expiry when it
stores a cookie, honors it on every lookup, and prunes dead entries so a
long-lived jar stays bounded.

### From a `Set-Cookie` response

| Attribute | Effect |
| --- | --- |
| `Max-Age` ≤ 0 | Marks the cookie for deletion: `store` removes any matching entry immediately. |
| `Expires` in the past | Handled the same way: the cookie is removed rather than stored. |
| `Expires` in the future | Parsed into an aware UTC datetime and stored on the cookie; a non-UTC offset is normalized to UTC. |
| `Max-Age` present | Takes precedence over `Expires`, which is parsed only in its absence. |

A *positive* `Max-Age` does not yet set a forward expiry: such a cookie is
currently stored without an expiry (it lives for the session) rather than
scheduled to expire after the given number of seconds. Today only a future
`Expires` drives forward expiry.

### On a hand-seeded cookie

`add(..., expires=...)` accepts any `datetime`. A naive value is assumed to be
UTC; an aware value in another offset is converted to UTC. Normalizing at the
boundary keeps every stored expiry directly comparable to the jar's clock, so a
naive input cannot raise a `TypeError` at comparison time.

### Staying bounded

- On lookup, `header_for` filters out any cookie whose expiry has passed, so an
  expired cookie is never sent even if the clock advanced since it was stored.
- On every `store`, the jar first prunes all expired entries, so a jar that
  keeps receiving responses does not accumulate dead cookies.

The jar reads "now" from an injectable clock, so tests can drive expiry
deterministically without sleeping.
