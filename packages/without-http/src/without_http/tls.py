from __future__ import annotations

import ssl
from collections.abc import Iterable
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType

# The ALPN protocol identifiers `without-http` can serve, in server-preference
# order: HTTP/2 is preferred when a client offers it, falling back to HTTP/1.1.
# ALPN negotiation selects between them during the TLS handshake.
ALPN_PROTOCOLS = ("h2", "http/1.1")

# The `tls` extension reports the version as the wire value (`0x0304` for TLS 1.3),
# while `SSLObject.version()` reports the name; `ssl.TLSVersion` already carries the
# pairing, so this maps names to it rather than restating the numbers.
_TLS_VERSIONS: Mapping[str, int] = MappingProxyType(
    {
        "SSLv3": ssl.TLSVersion.SSLv3,
        "TLSv1": ssl.TLSVersion.TLSv1,
        "TLSv1.1": ssl.TLSVersion.TLSv1_1,
        "TLSv1.2": ssl.TLSVersion.TLSv1_2,
        "TLSv1.3": ssl.TLSVersion.TLSv1_3,
    }
)

# The short names RFC 4514 §3 defines for the attribute types `getpeercert` reports
# by long name. An attribute outside this table keeps the name `ssl` gave it, which
# the RFC permits for types it does not list.
_RFC4514_NAMES: Mapping[str, str] = MappingProxyType(
    {
        "commonName": "CN",
        "localityName": "L",
        "stateOrProvinceName": "ST",
        "organizationName": "O",
        "organizationalUnitName": "OU",
        "countryName": "C",
        "streetAddress": "STREET",
        "domainComponent": "DC",
        "userId": "UID",
    }
)

# The characters RFC 4514 §2.4 escapes wherever they appear in an attribute value.
# A leading `#` or space and a trailing space are escaped too, but positionally.
_RFC4514_ESCAPED = frozenset('"+,;<>\\')


def server_ssl_context(certfile: Path, keyfile: Path | None = None) -> ssl.SSLContext:
    """
    Build a server-side TLS context that serves the protocols `without-http` speaks.

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


def _escaped(value: str) -> str:
    escaped = "".join(f"\\{character}" if character in _RFC4514_ESCAPED else character for character in value)
    if value.startswith(("#", " ")):
        escaped = f"\\{escaped}"
    # A one-character value that is a space was already escaped by the leading rule.
    if value.endswith(" ") and len(value) > 1:
        escaped = f"{escaped[:-1]}\\ "
    return escaped


def distinguished_name(subject: Iterable[Iterable[tuple[str, str]]]) -> str:
    """
    Render a certificate subject as its [RFC 4514](https://datatracker.ietf.org/doc/html/rfc4514)
    string, the form the `tls` ASGI extension asks for.

    Takes the shape `ssl.SSLObject.getpeercert()` reports a subject in: a sequence of
    relative distinguished names, each a sequence of attribute pairs. RFC 4514 orders
    the output most-specific first, which is the reverse of that sequence, joins
    multi-valued names with `+`, and escapes the characters that would otherwise
    separate one attribute from the next.
    """
    return ",".join(
        "+".join(f"{_RFC4514_NAMES.get(attribute, attribute)}={_escaped(value)}" for attribute, value in name)
        for name in reversed(tuple(subject))
    )


def _subject(peer_cert: Mapping[str, object]) -> tuple[tuple[tuple[str, str], ...], ...]:
    subject = peer_cert.get("subject", ())
    if not isinstance(subject, tuple):
        raise TypeError(f"expected a tuple of relative distinguished names, got {type(subject).__name__}")
    return tuple(
        tuple((str(attribute), str(value)) for attribute, value in name) for name in subject if isinstance(name, tuple)
    )


def tls_extension(ssl_object: ssl.SSLObject) -> Mapping[str, object]:
    """
    Read the [`tls` ASGI extension](https://asgi.readthedocs.io/en/latest/specs/tls.html)
    info off a finished handshake, for a scope's `extensions` mapping.

    Two of the extension's fields are `None` here because CPython's `ssl` module does
    not surface them, rather than because the connection lacks them: an
    `ssl.SSLContext` never exposes the certificate it was loaded with (`server_cert`),
    and `SSLObject.cipher()` reports the suite by name with no IANA identifier
    (`cipher_suite`). The spec permits `None` for both. `client_cert_error` is `None`
    because a client certificate that fails verification fails the handshake, so no
    scope is ever built for it.
    """
    peer_cert = ssl_object.getpeercert()
    return MappingProxyType(
        {
            "server_cert": None,
            "client_cert_chain": tuple(ssl.DER_cert_to_PEM_cert(der) for der in ssl_object.get_verified_chain() or ()),
            "client_cert_name": None if peer_cert is None else distinguished_name(_subject(peer_cert)),
            "client_cert_error": None,
            "tls_version": _TLS_VERSIONS.get(ssl_object.version() or ""),
            "cipher_suite": None,
        }
    )


def extensions_with_tls(
    extensions: Mapping[str, Mapping[str, object]],
    tls: Mapping[str, object] | None,
) -> Mapping[str, Mapping[str, object]]:
    """
    Add the `tls` extension to a scope's extensions, or return them unchanged.

    `None` means the connection is not over TLS, which the extension's absence is how
    an application detects. Callers build this once per connection rather than per
    request, since a connection's TLS facts do not change under it.
    """
    if tls is None:
        return extensions
    return MappingProxyType({**extensions, "tls": tls})
