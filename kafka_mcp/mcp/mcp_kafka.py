"""Thin MCP wrappers around the Apache Kafka API clients.

Each tool is a thin shim: it parses params, calls the corresponding
``KafkaApi``/``NativeKafkaClient`` method, and returns the result. All API
surface lives in ``kafka_mcp.api`` — these tools add no business logic.
"""

import json
from typing import Any

from fastmcp import FastMCP
from pydantic import Field

from kafka_mcp.auth import get_client, get_native_client


def register_kafka_tools(mcp: FastMCP) -> None:
    """Register the native-client tools and the topology ingest tool."""

    @mcp.tool(tags={"native"})
    async def kafka_native_produce(
        topic: str = Field(description="Topic to produce to."),
        value: str = Field(description="Message value."),
        key: str | None = Field(default=None, description="Message key."),
    ) -> Any:
        """Produce one message directly to a broker (native client; needs the native extra)."""
        return get_native_client().produce(topic, value, key=key)

    @mcp.tool(tags={"native"})
    async def kafka_native_consume(
        topic: str = Field(description="Topic to read from."),
        group: str = Field(description="Consumer group id."),
        max_messages: int = Field(default=10, description="Maximum messages to return."),
        timeout: float = Field(default=5.0, description="Seconds to wait."),
    ) -> Any:
        """Consume messages directly from a broker (native client)."""
        return get_native_client().consume(
            topic, group, max_messages=max_messages, timeout=timeout
        )

    @mcp.tool(tags={"native"})
    async def kafka_native_create_topic(
        topic: str = Field(description="Topic name to create."),
        partitions_count: int = Field(default=1, description="Partition count."),
        replication_factor: int = Field(default=1, description="Replication factor."),
    ) -> Any:
        """Create a topic directly on the brokers (native client)."""
        return get_native_client().create_topics(
            [topic],
            partitions_count=partitions_count,
            replication_factor=replication_factor,
        )

    @mcp.tool(tags={"native"})
    async def kafka_native_delete_topic(
        topic: str = Field(description="Topic name to delete."),
    ) -> Any:
        """Delete a topic directly on the brokers (native client). Destructive."""
        return get_native_client().delete_topics([topic])

    @mcp.tool(tags={"native"})
    async def kafka_native_list_topics(
        timeout: float = Field(default=10.0, description="Seconds to wait."),
    ) -> Any:
        """List topics straight from the broker metadata (native client)."""
        return get_native_client().list_topics(timeout=timeout)

    @mcp.tool(tags={"native"})
    async def kafka_native_list_groups(
        timeout: float = Field(default=10.0, description="Seconds to wait."),
    ) -> Any:
        """List consumer groups straight from the brokers (native client)."""
        return get_native_client().list_consumer_groups(timeout=timeout)

    @mcp.tool(tags={"kg"})
    async def kafka_ingest_catalog(
        params_json: str = Field(
            default="{}",
            description=(
                "JSON of arguments. All optional: "
                '{"cluster_id": "<id>", "topics": true, "groups": true, '
                '"brokers": true, "partitions_for": ["events"]}. Defaults to '
                "topics+groups+brokers for the resolved cluster."
            ),
        ),
    ) -> Any:
        """Ingest the live Kafka topology into the knowledge graph (Wire-First).

        Lists topics, consumer groups, and brokers via the REST Proxy and pushes
        them as typed ``:Topic`` / ``:ConsumerGroup`` / ``:Broker`` / ``:KafkaCluster``
        nodes (+ ``:inCluster`` / ``:partitionOf`` links). Native-ingest failures
        propagate to the caller.
        """
        from kafka_mcp.kg_ingest import (
            ingest_brokers,
            ingest_consumer_groups,
            ingest_partitions,
            ingest_topics,
        )

        client = get_client()
        p = json.loads(params_json) if params_json else {}
        cid = p.get("cluster_id")
        result: dict[str, Any] = {"ingested": {}}

        if p.get("topics", True):
            topics = client.list_topics(cluster_id=cid)
            result["ingested"]["topics"] = await ingest_topics(topics, cluster_id=cid)
        if p.get("groups", True):
            groups = client.list_consumer_groups(cluster_id=cid)
            result["ingested"]["groups"] = await ingest_consumer_groups(
                groups, cluster_id=cid
            )
        if p.get("brokers", True):
            brokers = client.list_brokers(cluster_id=cid)
            result["ingested"]["brokers"] = await ingest_brokers(
                brokers, cluster_id=cid
            )
        for topic in p.get("partitions_for", []) or []:
            parts = client.list_partitions(topic, cluster_id=cid)
            result["ingested"].setdefault("partitions", {})[
                topic
            ] = await ingest_partitions(parts, topic=topic, cluster_id=cid)
        return result
