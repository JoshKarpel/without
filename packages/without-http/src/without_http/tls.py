from __future__ import annotations

import ssl
from pathlib import Path

# The ALPN protocol identifiers `without-http` can serve, in server-preference
# order: HTTP/2 is preferred when a client offers it, falling back to HTTP/1.1.
# ALPN negotiation selects between them during the TLS handshake.
ALPN_PROTOCOLS = ("h2", "http/1.1")


def server_ssl_context(certfile: Path, keyfile: Path | None = None) -> ssl.SSLContext:
    """Build a server-side TLS context that serves the protocols `without-http` speaks.

    Loads the certificate chain (a combined cert+key PEM if `keyfile` is omitted)
    and advertises `ALPN_PROTOCOLS`, so a client negotiates the wire protocol
    during the handshake. Pass the result to `serving`/`serve` as `ssl_context` to
    serve `https` (and `wss`) directly.

    This is a convenience for the common case. A caller needing more control (an
    encrypted key, client-certificate verification, a custom cipher suite) builds
    its own `ssl.SSLContext` and passes that instead.
    """
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile, keyfile)
    context.set_alpn_protocols(list(ALPN_PROTOCOLS))
    return context
