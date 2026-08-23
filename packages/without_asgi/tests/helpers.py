from __future__ import annotations

from dataclasses import dataclass

from without_asgi import Asgi
from without_asgi import HttpScope
from without_asgi import RawHeaders


@dataclass(frozen=True, slots=True, eq=False)
class FileDescriptor:
    """
    The whole of what `ZeroCopySend` asks of a file: something with a descriptor.

    `eq=False` so that two of these are equal only when they are the same object. A
    file is a place rather than a value, and what `ZeroCopySend` has to carry across a
    codec is the caller's own open file, so a round trip that ends holding a different
    descriptor has lost the thing it exists to hand on. Field-wise equality would make
    that round trip hold for any descriptor at all, since there are no fields.
    """

    def fileno(self) -> int:
        return 7  # pragma: no cover - only its presence satisfies the SupportsFileno protocol; never called


def a_scope(*, path: str, http_version: str = "1.1", headers: RawHeaders = (), method: str = "GET") -> HttpScope:
    """One request scope, with everything middleware does not read already filled in."""
    return HttpScope(
        asgi=Asgi(version="3.0", spec_version="2.4"),
        http_version=http_version,
        method=method,
        scheme="http",
        path=path,
        raw_path=path.encode(),
        query_string=b"",
        root_path="",
        headers=headers,
        client=None,
        server=None,
        extensions=None,
    )
