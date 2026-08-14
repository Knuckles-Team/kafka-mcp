"""Tests for the /health custom route in kafka_mcp.mcp_server.

CONCEPT:AU-ECO.mcp.fastmcp-middleware

/health previously returned a hardcoded {"status": "OK"} regardless of
backend state -- the exact gap that let a dead KAFKA_URL env key mask a
fully non-functional REST-proxy client for weeks. It now proves a real
round-trip against the native (direct-to-broker) client.
"""

from unittest.mock import MagicMock, patch

import pytest
from starlette.responses import JSONResponse

from kafka_mcp.mcp_server import get_mcp_instance


def _capture_health_fn():
    """Build the MCP instance and return its decorated /health handler."""
    health_fn = None

    def mock_custom_route(path, methods):
        def decorator(fn):
            nonlocal health_fn
            if path == "/health":
                health_fn = fn
            return fn

        return decorator

    with patch("fastmcp.FastMCP.custom_route", side_effect=mock_custom_route):
        get_mcp_instance()
    assert health_fn is not None
    return health_fn


@pytest.mark.asyncio
async def test_health_check_backend_reachable():
    mock_native = MagicMock()
    mock_native.list_topics.return_value = {"topics": [{"name": "a", "partitions": 1}]}
    with patch("kafka_mcp.mcp_server.get_native_client", return_value=mock_native):
        health_fn = _capture_health_fn()
        response = await health_fn(MagicMock())

    mock_native.list_topics.assert_called_with(timeout=2.0)
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    assert b'"backend":"reachable"' in response.body
    assert b'"topics_visible":1' in response.body


@pytest.mark.asyncio
async def test_health_check_backend_unreachable():
    mock_native = MagicMock()
    mock_native.list_topics.side_effect = RuntimeError("no brokers available")
    with patch("kafka_mcp.mcp_server.get_native_client", return_value=mock_native):
        health_fn = _capture_health_fn()
        response = await health_fn(MagicMock())

    assert isinstance(response, JSONResponse)
    assert response.status_code == 503
    assert b'"status":"degraded"' in response.body
    assert b'"backend":"unreachable"' in response.body


@pytest.mark.asyncio
async def test_health_check_native_extra_missing():
    with patch.dict("sys.modules", {"confluent_kafka": None, "confluent_kafka.admin": None}):
        health_fn = _capture_health_fn()
        response = await health_fn(MagicMock())

    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    assert b'"backend":"unverified"' in response.body
