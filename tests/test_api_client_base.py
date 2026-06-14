"""Facade-parity tests for the strangled ApiClientBase.

The public surface must match the legacy requests-based implementation:
return shapes (parsed JSON / {"status": "success"} / {"status": "success",
"text": ...}), the generic ``API error: <status> - <body>`` exception on
HTTP >= 400, ``last_etag`` capture, and auth header injection. All transport
via httpx.MockTransport — no live REST Proxy.
"""

import httpx
import pytest

from kafka_mcp.api.api_client_base import ApiClientBase

BASE = "http://rest-proxy.test:8082"


def _client(handler, **kwargs) -> ApiClientBase:
    return ApiClientBase(BASE, transport=httpx.MockTransport(handler), **kwargs)


def test_json_response_returned_directly():
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == f"{BASE}/v3/clusters"
        return httpx.Response(200, json={"data": [{"cluster_id": "c1"}]})

    client = _client(handler)
    assert client.request("GET", "v3/clusters") == {"data": [{"cluster_id": "c1"}]}


def test_empty_body_returns_success_envelope():
    client = _client(lambda request: httpx.Response(204))
    assert client.request("DELETE", "v3/clusters/c1/topics/t") == {"status": "success"}


def test_non_json_body_returns_success_text():
    client = _client(
        lambda request: httpx.Response(
            200, text="OK", headers={"Content-Type": "text/plain"}
        )
    )
    assert client.request("GET", "ping") == {"status": "success", "text": "OK"}


def test_http_error_raises_legacy_message():
    client = _client(
        lambda request: httpx.Response(
            404, text="topic not found", headers={"Content-Type": "text/plain"}
        )
    )
    with pytest.raises(Exception, match="API error: 404 - topic not found"):
        client.request("GET", "v3/clusters/c1/topics/missing")


def test_etag_captured_per_request():
    client = _client(lambda request: httpx.Response(200, json={}, headers={"ETag": "v7"}))
    client.request("GET", "v3/clusters")
    assert client.last_etag == "v7"


def test_bearer_token_header_injected():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer tok"
        return httpx.Response(200, json={})

    _client(handler, token="tok").request("GET", "v3/clusters")


def test_basic_auth_header_injected():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"].startswith("Basic ")
        return httpx.Response(200, json={})

    _client(handler, username="u", password="p").request("GET", "v3/clusters")


def test_versioned_media_types_passed_through():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Content-Type"] == "application/vnd.kafka.json.v2+json"
        assert request.headers["Accept"] == "application/vnd.kafka.v2+json"
        return httpx.Response(200, json={})

    _client(handler).request(
        "POST",
        "topics/t",
        json={"records": [{"value": 1}]},
        content_type="application/vnd.kafka.json.v2+json",
        accept="application/vnd.kafka.v2+json",
    )


def test_absolute_url_endpoint_passes_through():
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "http://other.test/consumers/i1"
        return httpx.Response(200, json={})

    _client(handler).request("GET", "http://other.test/consumers/i1")
