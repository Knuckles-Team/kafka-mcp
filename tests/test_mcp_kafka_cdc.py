"""Tests for the CDC read tools (``kafka_cdc_topic_map``/``_lag``/``_slot_health``)."""

import json

import httpx
import pytest
from fastmcp import FastMCP

from kafka_mcp.api.api_client_connect import ConnectApi
from kafka_mcp.api.api_client_kafka import KafkaApi
from kafka_mcp.mcp.mcp_kafka_cdc import _debezium_topic_map, register_kafka_cdc_tools


def test_debezium_topic_map_derives_topic_names():
    config = {
        "topic.prefix": "cdc.ca51pilot",
        "table.include.list": "public.orders, public.customers",
    }
    resolved = _debezium_topic_map(config)
    assert resolved["topic_prefix"] == "cdc.ca51pilot"
    assert resolved["topics"] == [
        "cdc.ca51pilot.public.orders",
        "cdc.ca51pilot.public.customers",
    ]
    assert resolved["tables"][0] == {
        "table": "public.orders",
        "schema": "public",
        "table_name": "orders",
        "topic": "cdc.ca51pilot.public.orders",
    }


def test_debezium_topic_map_no_prefix_yields_no_topics():
    resolved = _debezium_topic_map({"table.include.list": "public.orders"})
    assert resolved["topic_prefix"] is None
    assert resolved["topics"] == []
    assert resolved["tables"][0]["topic"] is None


def test_debezium_topic_map_empty_config():
    resolved = _debezium_topic_map({})
    assert resolved == {"topic_prefix": None, "tables": [], "topics": []}


@pytest.fixture(autouse=True)
def _patch_clients(monkeypatch):
    holder: dict = {}
    monkeypatch.setattr(
        "kafka_mcp.mcp.mcp_kafka_cdc.get_connect_client",
        lambda: holder.get("connect_client"),
    )
    monkeypatch.setattr(
        "kafka_mcp.mcp.mcp_kafka_cdc.get_client", lambda: holder.get("kafka_client")
    )
    yield holder


@pytest.mark.asyncio
async def test_kafka_cdc_topic_map_tool(_patch_clients):
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "http://connect.test:8083/connectors/ca51pilot/config"
        return httpx.Response(
            200,
            json={
                "topic.prefix": "cdc.ca51pilot",
                "table.include.list": "public.orders",
            },
        )

    _patch_clients["connect_client"] = ConnectApi(
        "http://connect.test:8083", transport=httpx.MockTransport(handler)
    )
    mcp = FastMCP("test")
    register_kafka_cdc_tools(mcp)

    result = await mcp.call_tool(
        "kafka_cdc_topic_map", {"params_json": json.dumps({"connector": "ca51pilot"})}
    )
    assert result.structured_content["map"] == [
        {
            "table": "public.orders",
            "topic": "cdc.ca51pilot.public.orders",
            "ontology_class": "Topic",
            "handler": "kafka_ingest_cdc_topology",
        }
    ]


@pytest.mark.asyncio
async def test_kafka_cdc_lag_tool_filters_topics(_patch_clients):
    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url).endswith("/lag-summary"):
            return httpx.Response(200, json={"total_lag": 5})
        return httpx.Response(
            200,
            json={
                "data": [
                    {"topic_name": "cdc.ca51pilot.public.orders", "lag": 3},
                    {"topic_name": "other-topic", "lag": 2},
                ]
            },
        )

    kafka_client = KafkaApi("http://rest.test:8082", transport=httpx.MockTransport(handler))
    kafka_client._cluster_id = "c1"  # skip lazy cluster resolution for this test
    _patch_clients["kafka_client"] = kafka_client
    mcp = FastMCP("test")
    register_kafka_cdc_tools(mcp)

    result = await mcp.call_tool(
        "kafka_cdc_lag",
        {
            "params_json": json.dumps(
                {"group": "cdc-consumers", "topics": ["cdc.ca51pilot.public.orders"]}
            )
        },
    )
    body = result.structured_content
    assert body["group"] == "cdc-consumers"
    assert body["lags"]["data"] == [
        {"topic_name": "cdc.ca51pilot.public.orders", "lag": 3}
    ]


@pytest.mark.asyncio
async def test_kafka_cdc_slot_health_tool(_patch_clients):
    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url).endswith("/status"):
            return httpx.Response(200, json={"connector": {"state": "RUNNING"}})
        return httpx.Response(
            200, json={"slot.name": "ca_ca51pilot", "database.dbname": "ca51pilot"}
        )

    _patch_clients["connect_client"] = ConnectApi(
        "http://connect.test:8083", transport=httpx.MockTransport(handler)
    )
    mcp = FastMCP("test")
    register_kafka_cdc_tools(mcp)

    result = await mcp.call_tool(
        "kafka_cdc_slot_health", {"params_json": json.dumps({"connector": "ca51pilot"})}
    )
    body = result.structured_content
    assert body["declared_slot_name"] == "ca_ca51pilot"
    assert body["see_also"]["tool"] == "sql-mcp:sql_query"
    assert "ca_ca51pilot" in body["see_also"]["hint"]
