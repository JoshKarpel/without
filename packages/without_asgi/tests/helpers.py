from __future__ import annotations

from dataclasses import dataclass

from without_asgi import Asgi
from without_asgi import HttpScope
from without_asgi import RawHeaders


@dataclass(frozen=True, slots=True)
class FileDescriptor:
    """The whole of what `ZeroCopySend` asks of a file: something with a descriptor."""

    def fileno(self) -> int:
        return 7  # pragma: no cover - only its presence satisfies the SupportsFileno protocol; never called


def a_scope(*, path: str, http_version: str = "1.1", headers: RawHeaders = ()) -> HttpScope:
    """One GET scope, with everything middleware does not read already filled in."""
    return HttpScope(
        asgi=Asgi(version="3.0", spec_version="2.4"),
        http_version=http_version,
        method="GET",
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
