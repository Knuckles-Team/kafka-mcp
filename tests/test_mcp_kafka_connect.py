"""Tests for the ``kafka_connect``/``kafka_connect_plugins`` MCP tool dispatch."""

import json

import httpx
import pytest
from fastmcp import FastMCP

from kafka_mcp.api.api_client_connect import ConnectApi
from kafka_mcp.mcp.mcp_kafka_connect import register_kafka_connect_tools

BASE = "http://connect.test:8083"


def _mcp(handler) -> FastMCP:
    mcp = FastMCP("test")
    register_kafka_connect_tools(mcp)
    client = ConnectApi(BASE, transport=httpx.MockTransport(handler))
    return mcp, client


@pytest.fixture(autouse=True)
def _patch_client(monkeypatch):
    """Point ``get_connect_client`` at a client built per-test with its own handler."""
    holder: dict = {}
    monkeypatch.setattr(
        "kafka_mcp.mcp.mcp_kafka_connect.get_connect_client", lambda: holder["client"]
    )
    yield holder


async def _call(mcp: FastMCP, tool: str, **kwargs):
    return await mcp.call_tool(tool, kwargs)


@pytest.mark.asyncio
async def test_list_action(_patch_client):
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == f"{BASE}/connectors?expand=status"
        return httpx.Response(200, json={"ca51pilot": {"status": {}}})

    mcp = FastMCP("test")
    register_kafka_connect_tools(mcp)
    _patch_client["client"] = ConnectApi(BASE, transport=httpx.MockTransport(handler))

    result = await _call(
        mcp, "kafka_connect", action="list", params_json=json.dumps({"expand": "status"})
    )
    assert result.structured_content == {"ca51pilot": {"status": {}}}


@pytest.mark.asyncio
async def test_create_action_sends_name_and_config(_patch_client):
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.read())
        return httpx.Response(201, json={"name": "ca51pilot"})

    mcp = FastMCP("test")
    register_kafka_connect_tools(mcp)
    _patch_client["client"] = ConnectApi(BASE, transport=httpx.MockTransport(handler))

    await _call(
        mcp,
        "kafka_connect",
        action="create",
        params_json=json.dumps(
            {"name": "ca51pilot", "config": {"connector.class": "x"}}
        ),
    )
    assert seen["body"] == {"name": "ca51pilot", "config": {"connector.class": "x"}}


@pytest.mark.asyncio
async def test_create_action_duplicate_name_surfaces_error(_patch_client):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"message": "already exists"})

    mcp = FastMCP("test")
    register_kafka_connect_tools(mcp)
    _patch_client["client"] = ConnectApi(BASE, transport=httpx.MockTransport(handler))

    with pytest.raises(Exception, match="409"):
        await _call(
            mcp,
            "kafka_connect",
            action="create",
            params_json=json.dumps({"name": "ca51pilot", "config": {}}),
        )


@pytest.mark.asyncio
async def test_delete_action(_patch_client):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "DELETE"
        return httpx.Response(204)

    mcp = FastMCP("test")
    register_kafka_connect_tools(mcp)
    _patch_client["client"] = ConnectApi(BASE, transport=httpx.MockTransport(handler))

    result = await _call(
        mcp, "kafka_connect", action="delete", params_json=json.dumps({"name": "ca51pilot"})
    )
    assert result.structured_content == {"status": "success"}


@pytest.mark.asyncio
async def test_unknown_action_raises(_patch_client):
    mcp = FastMCP("test")
    register_kafka_connect_tools(mcp)
    _patch_client["client"] = ConnectApi(
        BASE, transport=httpx.MockTransport(lambda r: httpx.Response(200, json={}))
    )

    with pytest.raises(Exception, match="Unknown connect action"):
        await _call(mcp, "kafka_connect", action="bogus", params_json="{}")


@pytest.mark.asyncio
async def test_connect_plugins(_patch_client):
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == f"{BASE}/connector-plugins"
        return httpx.Response(200, json=[{"class": "x"}])

    mcp = FastMCP("test")
    register_kafka_connect_tools(mcp)
    _patch_client["client"] = ConnectApi(BASE, transport=httpx.MockTransport(handler))

    result = await mcp.call_tool("kafka_connect_plugins", {})
    assert json.loads(result.content[0].text) == [{"class": "x"}]
