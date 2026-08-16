from __future__ import annotations

import h11
import pytest
from without_asgi import Disconnect
from without_asgi import EarlyHint
from without_asgi import Outbound
from without_asgi import PathSend
from without_asgi import RequestBody
from without_asgi import ResponseBody
from without_asgi import ResponseStart
from without_asgi import ServerPush
from without_asgi import extension
from without_http import h11_events_from_outbound
from without_http import inbound_from_event
from without_http import scope_from_request


def test_scope_from_request_reads_the_request_line() -> None:
    request = h11.Request(
        method="POST",
        target="/items?page=2",
        headers=[("host", "example.test"), ("content-type", "application/json")],
        http_version="1.1",
    )

    scope = scope_from_request(request, scheme="https", server=("example.test", 443), client=("198.51.100.7", 54321))

    assert scope.method == "POST"
    assert scope.path == "/items"
    assert scope.raw_path == b"/items"
    assert scope.query_string == b"page=2"
    assert scope.root_path == ""
    assert scope.http_version == "1.1"
    assert scope.scheme == "https"
    assert scope.server == ("example.test", 443)
    assert scope.client == ("198.51.100.7", 54321)
    assert scope.headers == ((b"host", b"example.test"), (b"content-type", b"application/json"))


def test_scope_from_request_percent_decodes_the_path() -> None:
    request = h11.Request(method="GET", target="/caf%C3%A9", headers=[("host", "t")], http_version="1.1")

    scope = scope_from_request(request, scheme="http", server=None, client=None)

    assert scope.path == "/café"
    assert scope.raw_path == b"/caf%C3%A9"


def test_scope_from_request_advertises_early_hints_and_no_offload_extensions() -> None:
    request = h11.Request(method="GET", target="/items", headers=[("host", "t")], http_version="1.1")

    scope = scope_from_request(request, scheme="http", server=None, client=None)

    assert extension(scope.extensions, "http.response.early_hint") is not None
    assert extension(scope.extensions, "http.response.trailers") is None


@pytest.mark.parametrize(
    ("event", "expected"),
    [
        (h11.Data(data=b"chunk"), RequestBody(body=b"chunk", more_body=True)),
        (h11.EndOfMessage(), RequestBody(body=b"", more_body=False)),
        (h11.ConnectionClosed(), Disconnect()),
    ],
)
def test_inbound_from_event_classifies_body_events(event: h11.Event, expected: object) -> None:
    assert inbound_from_event(event) == expected


def test_inbound_from_event_skips_a_non_body_event() -> None:
    request = h11.Request(method="GET", target="/", headers=[("host", "t")], http_version="1.1")

    assert inbound_from_event(request) is None


def test_h11_events_from_outbound_renders_a_response_start() -> None:
    events = h11_events_from_outbound(ResponseStart(status=201, headers=((b"content-type", b"text/plain"),)))

    assert len(events) == 1
    response = events[0]
    assert isinstance(response, h11.Response)
    assert response.status_code == 201


def test_h11_events_from_outbound_splits_a_final_body_into_data_then_end() -> None:
    events = h11_events_from_outbound(ResponseBody(body=b"hello", more_body=False))

    assert events == [h11.Data(data=b"hello"), h11.EndOfMessage()]


def test_h11_events_from_outbound_emits_only_data_for_a_continuing_body() -> None:
    assert h11_events_from_outbound(ResponseBody(body=b"part", more_body=True)) == [h11.Data(data=b"part")]


def test_h11_events_from_outbound_closes_an_empty_final_body() -> None:
    assert h11_events_from_outbound(ResponseBody(body=b"", more_body=False)) == [h11.EndOfMessage()]


def test_h11_events_from_outbound_renders_early_hints_as_informational() -> None:
    events = h11_events_from_outbound(EarlyHint(links=(b"</style.css>; rel=preload",)))

    assert len(events) == 1
    hint = events[0]
    assert isinstance(hint, h11.InformationalResponse)
    assert hint.status_code == 103
    assert list(hint.headers.raw_items()) == [(b"link", b"</style.css>; rel=preload")]


@pytest.mark.parametrize(
    ("outbound", "type_name"),
    [
        (ServerPush(path="/x", headers=()), "ServerPush"),
        (PathSend(path="/var/www/big.iso"), "PathSend"),
    ],
)
def test_h11_events_from_outbound_rejects_an_unsupported_extension(outbound: Outbound, type_name: str) -> None:
    with pytest.raises(NotImplementedError, match=rf"^{type_name} is not supported over HTTP/1\.1$"):
        h11_events_from_outbound(outbound)
