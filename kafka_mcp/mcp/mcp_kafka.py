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
    """Register topic, record, group, cluster, and native tools for Kafka."""

    @mcp.tool(tags={"topics"})
    async def kafka_topics(
        action: str = Field(
            description=(
                "Topic action. One of: 'list', 'create', 'describe', 'delete', "
                "'configs' (list configs), 'update_config' (alter configs)."
            )
        ),
        params_json: str = Field(
            default="{}",
            description=(
                "JSON of arguments, e.g. "
                '{"topic": "events"} for describe/delete/configs, '
                '{"topic": "events", "partitions_count": 3, '
                '"replication_factor": 1, "configs": {"retention.ms": "60000"}} '
                "for create, "
                '{"topic": "events", "configs": {"retention.ms": "120000"}} '
                "for update_config. Optional 'cluster_id' overrides resolution."
            ),
        ),
    ) -> Any:
        """Manage Kafka topics via the Confluent REST Proxy."""
        client = get_client()
        p = json.loads(params_json) if params_json else {}
        cid = p.get("cluster_id")
        if action == "list":
            topics = client.list_topics(cluster_id=cid)
            # Wire-First: mirror the topic catalog into the authoritative KG.
            from kafka_mcp.kg_ingest import ingest_topics

            await ingest_topics(topics, cluster_id=cid)
            return topics
        if action == "create":
            return client.create_topic(
                p["topic"],
                partitions_count=p.get("partitions_count", 1),
                replication_factor=p.get("replication_factor", 1),
                configs=p.get("configs"),
                cluster_id=cid,
            )
        if action == "describe":
            return client.get_topic(p["topic"], cluster_id=cid)
        if action == "delete":
            return client.delete_topic(p["topic"], cluster_id=cid)
        if action == "configs":
            return client.list_topic_configs(p["topic"], cluster_id=cid)
        if action == "update_config":
            return client.update_topic_configs(p["topic"], p["configs"], cluster_id=cid)
        raise ValueError(f"Unknown topics action: {action!r}.")

    @mcp.tool(tags={"partitions"})
    async def kafka_partitions(
        action: str = Field(description="Partition action. One of: 'list', 'get'."),
        params_json: str = Field(
            default="{}",
            description=(
                'JSON of arguments, e.g. {"topic": "events"} for list, '
                '{"topic": "events", "partition_id": 0} for get.'
            ),
        ),
    ) -> Any:
        """List partitions for a topic or get a single partition."""
        client = get_client()
        p = json.loads(params_json) if params_json else {}
        cid = p.get("cluster_id")
        if action == "list":
            return client.list_partitions(p["topic"], cluster_id=cid)
        if action == "get":
            return client.get_partition(p["topic"], p["partition_id"], cluster_id=cid)
        raise ValueError(f"Unknown partitions action: {action!r}.")

    @mcp.tool(tags={"records"})
    async def kafka_records(
        action: str = Field(
            description=(
                "Record action. One of: 'produce' (v3 produce), "
                "'create_consumer', 'subscribe', 'consume' (poll), 'commit', "
                "'delete_consumer' (v2 consumer instance lifecycle)."
            )
        ),
        params_json: str = Field(
            default="{}",
            description=(
                "JSON of arguments. produce: "
                '{"topic": "events", "value": {...}, "key": "k", '
                '"value_format": "JSON"}. create_consumer: '
                '{"group": "g", "name": "c1", "format": "json"}. subscribe: '
                '{"group": "g", "instance": "c1", "topics": ["events"]}. '
                'consume: {"group": "g", "instance": "c1", "timeout": 1000}. '
                'commit/delete_consumer: {"group": "g", "instance": "c1"}.'
            ),
        ),
    ) -> Any:
        """Produce or consume records via the Confluent REST Proxy."""
        client = get_client()
        p = json.loads(params_json) if params_json else {}
        if action == "produce":
            return client.produce_record(
                p["topic"],
                p["value"],
                key=p.get("key"),
                value_format=p.get("value_format", "JSON"),
                key_format=p.get("key_format"),
                partition_id=p.get("partition_id"),
                headers=p.get("headers"),
                cluster_id=p.get("cluster_id"),
            )
        if action == "create_consumer":
            return client.create_consumer(
                p["group"],
                name=p.get("name"),
                fmt=p.get("format", "json"),
                auto_offset_reset=p.get("auto_offset_reset", "earliest"),
            )
        if action == "subscribe":
            return client.subscribe_consumer(p["group"], p["instance"], p["topics"])
        if action == "consume":
            return client.consume_records(
                p["group"],
                p["instance"],
                timeout=p.get("timeout", 1000),
                max_bytes=p.get("max_bytes"),
                fmt=p.get("format", "json"),
            )
        if action == "commit":
            return client.commit_offsets(p["group"], p["instance"])
        if action == "delete_consumer":
            return client.delete_consumer(p["group"], p["instance"])
        raise ValueError(f"Unknown records action: {action!r}.")

    @mcp.tool(tags={"groups"})
    async def kafka_groups(
        action: str = Field(
            description=(
                "Consumer group action. One of: 'list', 'describe', "
                "'consumers' (list members), 'lag_summary', 'lags'."
            )
        ),
        params_json: str = Field(
            default="{}",
            description=(
                'JSON of arguments, e.g. {} for list, {"group": "g"} for '
                "describe/consumers/lag_summary/lags. Optional 'cluster_id'."
            ),
        ),
    ) -> Any:
        """Inspect Kafka consumer groups and their lag."""
        client = get_client()
        p = json.loads(params_json) if params_json else {}
        cid = p.get("cluster_id")
        if action == "list":
            return client.list_consumer_groups(cluster_id=cid)
        if action == "describe":
            return client.get_consumer_group(p["group"], cluster_id=cid)
        if action == "consumers":
            return client.list_consumers(p["group"], cluster_id=cid)
        if action == "lag_summary":
            return client.get_group_lag_summary(p["group"], cluster_id=cid)
        if action == "lags":
            return client.get_group_lags(p["group"], cluster_id=cid)
        raise ValueError(f"Unknown groups action: {action!r}.")

    @mcp.tool(tags={"cluster"})
    async def kafka_cluster(
        action: str = Field(
            description=(
                "Cluster/broker/ACL action. One of: 'list_clusters', "
                "'get_cluster', 'list_brokers', 'get_broker', 'broker_configs', "
                "'list_acls', 'create_acl', 'delete_acls'."
            )
        ),
        params_json: str = Field(
            default="{}",
            description=(
                "JSON of arguments. get_broker/broker_configs: "
                '{"broker_id": 1}. list_acls/delete_acls: filter fields like '
                '{"resource_type": "TOPIC", "resource_name": "events", '
                '"pattern_type": "LITERAL", "principal": "User:alice", '
                '"operation": "READ", "permission": "ALLOW"}. create_acl: '
                "the same fields plus 'host'. Optional 'cluster_id'."
            ),
        ),
    ) -> Any:
        """Inspect clusters/brokers and manage ACLs via the REST Proxy."""
        client = get_client()
        p = json.loads(params_json) if params_json else {}
        cid = p.get("cluster_id")
        if action == "list_clusters":
            return client.list_clusters()
        if action == "get_cluster":
            return client.get_cluster(cluster_id=cid)
        if action == "list_brokers":
            return client.list_brokers(cluster_id=cid)
        if action == "get_broker":
            return client.get_broker(p["broker_id"], cluster_id=cid)
        if action == "broker_configs":
            return client.list_broker_configs(p["broker_id"], cluster_id=cid)
        if action == "list_acls":
            return client.list_acls(
                resource_type=p.get("resource_type"),
                resource_name=p.get("resource_name"),
                pattern_type=p.get("pattern_type"),
                principal=p.get("principal"),
                operation=p.get("operation"),
                permission=p.get("permission"),
                cluster_id=cid,
            )
        if action == "create_acl":
            return client.create_acl(
                resource_type=p["resource_type"],
                resource_name=p["resource_name"],
                pattern_type=p["pattern_type"],
                principal=p["principal"],
                host=p["host"],
                operation=p["operation"],
                permission=p["permission"],
                cluster_id=cid,
            )
        if action == "delete_acls":
            return client.delete_acls(
                resource_type=p.get("resource_type"),
                resource_name=p.get("resource_name"),
                pattern_type=p.get("pattern_type"),
                principal=p.get("principal"),
                host=p.get("host"),
                operation=p.get("operation"),
                permission=p.get("permission"),
                cluster_id=cid,
            )
        raise ValueError(f"Unknown cluster action: {action!r}.")

    @mcp.tool(tags={"native"})
    async def kafka_native(
        action: str = Field(
            description=(
                "Native (direct-to-broker) action. One of: 'produce', "
                "'consume', 'create_topic', 'delete_topic', 'list_topics', "
                "'list_groups'. Requires the kafka-mcp[native] extra."
            )
        ),
        topic: str | None = Field(
            default=None,
            description="Topic (produce, consume, create_topic, delete_topic).",
        ),
        value: str | None = Field(default=None, description="produce: message value."),
        key: str | None = Field(default=None, description="produce: message key."),
        group: str | None = Field(default=None, description="consume: consumer group."),
        max_messages: int | None = Field(
            default=None, description="consume: max messages (default 10)."
        ),
        timeout: float | None = Field(
            default=None, description="consume/list_topics/list_groups: seconds."
        ),
        partitions_count: int | None = Field(
            default=None, description="create_topic: partitions (default 1)."
        ),
        replication_factor: int | None = Field(
            default=None, description="create_topic: replication factor (default 1)."
        ),
    ) -> Any:
        """Produce/consume/admin directly against brokers (native client)."""
        client = get_native_client()
        if action == "produce":
            return client.produce(topic, value, key=key)
        if action == "consume":
            return client.consume(
                topic,
                group,
                max_messages=10 if max_messages is None else max_messages,
                timeout=5.0 if timeout is None else timeout,
            )
        if action == "create_topic":
            return client.create_topics(
                [topic],
                partitions_count=1 if partitions_count is None else partitions_count,
                replication_factor=1
                if replication_factor is None
                else replication_factor,
            )
        if action == "delete_topic":
            return client.delete_topics([topic])
        if action == "list_topics":
            return client.list_topics(timeout=10.0 if timeout is None else timeout)
        if action == "list_groups":
            return client.list_consumer_groups(
                timeout=10.0 if timeout is None else timeout
            )
        raise ValueError(f"Unknown native action: {action!r}.")

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
