from __future__ import annotations

import ssl
from collections.abc import Callable
from pathlib import Path

import pytest
import trustme
from without_http import server_ssl_context

HOST = "127.0.0.1"


@pytest.fixture(scope="session")
def authority() -> trustme.CA:
    return trustme.CA()


@pytest.fixture(scope="session")
def server_context(authority: trustme.CA, tmp_path_factory: pytest.TempPathFactory) -> ssl.SSLContext:
    pem: Path = tmp_path_factory.mktemp("tls") / "server.pem"
    authority.issue_cert(HOST).private_key_and_cert_chain_pem.write_to_path(pem)
    return server_ssl_context(pem)


@pytest.fixture
def trusting_client_context_factory(authority: trustme.CA) -> Callable[[], ssl.SSLContext]:
    """A `ConnectionPool.ssl_context_factory` that trusts only the test CA."""

    def make() -> ssl.SSLContext:
        # Trust only the test CA. create_default_context() would additionally load the system
        # root store (~7ms per call), which these localhost tests never use.
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        authority.configure_trust(context)
        return context

    return make


# A single context for consumers that take one directly (httpx, WebSocketClient). They call
# `set_alpn_protocols(...)` on it, so it must stay function-scoped: a wider scope would let one
# test's ALPN choice leak into the next. ConnectionPool takes the factory above instead.
@pytest.fixture
def trusting_client_context(trusting_client_context_factory: Callable[[], ssl.SSLContext]) -> ssl.SSLContext:
    return trusting_client_context_factory()
