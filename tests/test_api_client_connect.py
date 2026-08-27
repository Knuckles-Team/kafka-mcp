"""Focused tests for the Kafka Connect REST client (mocked transport, no live worker)."""

import httpx
import pytest

from kafka_mcp.api.api_client_connect import ConnectApi

BASE = "http://connect.test:8083"


def _client(handler, **kwargs) -> ConnectApi:
    return ConnectApi(BASE, transport=httpx.MockTransport(handler), **kwargs)


def test_list_connectors_no_expand():
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == f"{BASE}/connectors"
        return httpx.Response(200, json=["ca51pilot"])

    assert _client(handler).list_connectors() == ["ca51pilot"]


def test_list_connectors_with_expand():
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == f"{BASE}/connectors?expand=status"
        return httpx.Response(200, json={"ca51pilot": {"status": {}}})

    assert _client(handler).list_connectors(expand="status") == {
        "ca51pilot": {"status": {}}
    }


def test_get_connector():
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == f"{BASE}/connectors/ca51pilot"
        return httpx.Response(200, json={"name": "ca51pilot"})

    assert _client(handler).get_connector("ca51pilot") == {"name": "ca51pilot"}


def test_create_connector_success():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert str(request.url) == f"{BASE}/connectors"
        body = request.read()
        assert b'"name": "ca51pilot"' in body or b'"name":"ca51pilot"' in body
        return httpx.Response(201, json={"name": "ca51pilot", "config": {}})

    result = _client(handler).create_connector("ca51pilot", {"connector.class": "x"})
    assert result["name"] == "ca51pilot"


def test_create_connector_duplicate_name_surfaces_409():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={
                "error_code": 409,
                "message": "Connector ca51pilot already exists",
            },
        )

    client = _client(handler)
    with pytest.raises(Exception, match="API error: 409"):
        client.create_connector("ca51pilot", {"connector.class": "x"})


def test_update_connector_config_is_put():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "PUT"
        assert str(request.url) == f"{BASE}/connectors/ca51pilot/config"
        return httpx.Response(200, json={"name": "ca51pilot"})

    _client(handler).update_connector_config("ca51pilot", {"connector.class": "x"})


def test_get_connector_config():
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == f"{BASE}/connectors/ca51pilot/config"
        return httpx.Response(200, json={"connector.class": "x", "slot.name": "s1"})

    assert _client(handler).get_connector_config("ca51pilot")["slot.name"] == "s1"


def test_delete_connector():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "DELETE"
        assert str(request.url) == f"{BASE}/connectors/ca51pilot"
        return httpx.Response(204)

    assert _client(handler).delete_connector("ca51pilot") == {"status": "success"}


def test_delete_connector_missing_is_not_swallowed():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error_code": 404, "message": "not found"})

    with pytest.raises(Exception, match="API error: 404"):
        _client(handler).delete_connector("ghost")


def test_get_connector_status():
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == f"{BASE}/connectors/ca51pilot/status"
        return httpx.Response(200, json={"connector": {"state": "RUNNING"}})

    result = _client(handler).get_connector_status("ca51pilot")
    assert result["connector"]["state"] == "RUNNING"


def test_restart_connector_default_no_params():
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == f"{BASE}/connectors/ca51pilot/restart"
        return httpx.Response(204)

    _client(handler).restart_connector("ca51pilot")


def test_restart_connector_with_tasks_and_only_failed():
    def handler(request: httpx.Request) -> httpx.Response:
        assert "includeTasks=true" in str(request.url)
        assert "onlyFailed=true" in str(request.url)
        return httpx.Response(204)

    _client(handler).restart_connector("ca51pilot", include_tasks=True, only_failed=True)


def test_pause_connector_is_put():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "PUT"
        assert str(request.url) == f"{BASE}/connectors/ca51pilot/pause"
        return httpx.Response(202)

    _client(handler).pause_connector("ca51pilot")


def test_resume_connector_is_put():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "PUT"
        assert str(request.url) == f"{BASE}/connectors/ca51pilot/resume"
        return httpx.Response(202)

    _client(handler).resume_connector("ca51pilot")


def test_get_connector_offsets():
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == f"{BASE}/connectors/ca51pilot/offsets"
        return httpx.Response(200, json={"offsets": []})

    assert _client(handler).get_connector_offsets("ca51pilot") == {"offsets": []}


def test_list_connector_plugins():
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == f"{BASE}/connector-plugins"
        return httpx.Response(
            200,
            json=[{"class": "io.debezium.connector.postgresql.PostgresConnector"}],
        )

    plugins = _client(handler).list_connector_plugins()
    assert plugins[0]["class"] == "io.debezium.connector.postgresql.PostgresConnector"
