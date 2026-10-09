"""Native epistemic-graph typed-node ingestion — Wire-First coverage.

Exercises the real ``ingest_entities`` / ``ingest_topics`` / ``ingest_partitions`` /
``ingest_consumer_groups`` / ``ingest_brokers`` / ``ingest_cdc_connectors`` seams
against a fake transport boundary (one level below the SDK's own request
builder), asserting the Kafka REST-Proxy record ->
:Topic/:Partition/:ConsumerGroup/:Broker/:KafkaCluster/:CdcConnector mapping.

The transport receives the real generated ``SourceIngestionRequest`` built by
``agent_connector_sdk.ingest.request.build_request``: entities surface as
``SourceRecord`` (``record_id`` + ``payload`` + a ``mapping_reference`` that
encodes the node_type), and relationships as ``SourceRelationship``
(``source``/``target`` are ``SourceEntityRef`` objects with ``record_id``;
the relationship name + the *source* entity's node_type are encoded in
``relation_reference``, not on flat attributes).
CONCEPT:AU-KG.ingest.enterprise-source-extractor.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from agent_connector_sdk.ingest import IngestError, KnowledgeIngest

from kafka_mcp.kg_ingest import (
    ingest_brokers,
    ingest_cdc_connectors,
    ingest_consumer_groups,
    ingest_entities,
    ingest_partitions,
    ingest_topics,
)

_CONNECTOR = "kafka-mcp"


class _FakeTransport:
    def __init__(self) -> None:
        self.requests: list[Any] = []

    async def source_status(self, connector: str, stream: str) -> Any:
        return SimpleNamespace(accepted_checkpoint=None)

    async def submit(self, request: Any) -> Any:
        self.requests.append(request)
        return SimpleNamespace(
            affected_count=len(request.records),
            relationship_count=len(request.relationships),
        )

    async def store_blob(self, data: Any) -> Any:
        raise AssertionError("this connector's ingestion carries no media")


@pytest.fixture
def ingest():
    transport = _FakeTransport()
    return KnowledgeIngest(transport, loop=None), transport


def _records_by_id(request: Any) -> dict[str, Any]:
    return {r.record_id: r for r in request.records}


def _mapping_reference(connector: str, node_type: str) -> str:
    return f"manifest:{connector}#schema_mappings/{node_type}"


def _relation_reference(connector: str, source_node_type: str, relationship: str) -> str:
    return f"manifest:{connector}#resources/{source_node_type}/relations/{relationship}"


@pytest.mark.asyncio
async def test_ingest_entities_writes_nodes_and_edges(ingest):
    service, transport = ingest
    res = await ingest_entities(
        [
            {"id": "a", "node_type": "Topic", "name": "events"},
            {"id": "b", "node_type": "KafkaCluster"},
        ],
        [{"source": "a", "target": "b", "relationship": "inCluster"}],
        ingest=service,
    )
    assert res == {"nodes": 2, "edges": 1}
    request = transport.requests[0]
    records = _records_by_id(request)
    assert set(records) == {"a", "b"}
    assert records["a"].mapping_reference == _mapping_reference(_CONNECTOR, "Topic")
    assert records["a"].payload["name"] == "events"

    rel = request.relationships[0]
    assert rel.source.record_id == "a"
    assert rel.target.record_id == "b"
    assert rel.relation_reference == _relation_reference(_CONNECTOR, "Topic", "inCluster")


@pytest.mark.asyncio
async def test_ingest_topics_maps_topic_and_cluster(ingest):
    service, transport = ingest
    res = await ingest_topics(
        {
            "data": [
                {
                    "topic_name": "events",
                    "cluster_id": "clstr-1",
                    "partitions_count": 3,
                    "replication_factor": 2,
                    "is_internal": False,
                }
            ]
        },
        ingest=service,
    )
    assert res == {"nodes": 2, "edges": 1}
    request = transport.requests[0]
    records = _records_by_id(request)
    topic = records["kafka:topic:clstr-1:events"]
    assert topic.mapping_reference == _mapping_reference(_CONNECTOR, "Topic")
    assert topic.payload["partitionsCount"] == 3
    assert topic.payload["replicationFactor"] == 2
    assert topic.payload["externalToolId"] == "events"
    cluster = records["kafka:cluster:clstr-1"]
    assert cluster.mapping_reference == _mapping_reference(_CONNECTOR, "KafkaCluster")

    rel = request.relationships[0]
    assert rel.source.record_id == "kafka:topic:clstr-1:events"
    assert rel.target.record_id == "kafka:cluster:clstr-1"
    assert rel.relation_reference == _relation_reference(_CONNECTOR, "Topic", "inCluster")


@pytest.mark.asyncio
async def test_ingest_partitions_maps_partition_of_topic(ingest):
    service, transport = ingest
    res = await ingest_partitions(
        {
            "data": [
                {"cluster_id": "clstr-1", "topic_name": "events", "partition_id": 0},
                {"cluster_id": "clstr-1", "topic_name": "events", "partition_id": 1},
            ]
        },
        topic="events",
        ingest=service,
    )
    assert res == {"nodes": 2, "edges": 2}
    request = transport.requests[0]
    records = _records_by_id(request)
    p0 = records["kafka:partition:clstr-1:events:0"]
    assert p0.mapping_reference == _mapping_reference(_CONNECTOR, "Partition")
    assert p0.payload["partitionId"] == 0
    endpoints = {(r.source.record_id, r.target.record_id) for r in request.relationships}
    assert ("kafka:partition:clstr-1:events:0", "kafka:topic:clstr-1:events") in endpoints


@pytest.mark.asyncio
async def test_ingest_consumer_groups_maps_group_and_cluster(ingest):
    service, transport = ingest
    res = await ingest_consumer_groups(
        {
            "data": [
                {
                    "cluster_id": "clstr-1",
                    "consumer_group_id": "analytics",
                    "state": "STABLE",
                }
            ]
        },
        ingest=service,
    )
    assert res == {"nodes": 2, "edges": 1}
    request = transport.requests[0]
    grp = _records_by_id(request)["kafka:group:clstr-1:analytics"]
    assert grp.mapping_reference == _mapping_reference(_CONNECTOR, "ConsumerGroup")
    assert grp.payload["groupState"] == "STABLE"


@pytest.mark.asyncio
async def test_ingest_brokers_maps_broker_and_cluster(ingest):
    service, transport = ingest
    res = await ingest_brokers(
        {
            "data": [
                {"cluster_id": "clstr-1", "broker_id": 1, "host": "b1", "port": 9092}
            ]
        },
        ingest=service,
    )
    assert res == {"nodes": 2, "edges": 1}
    request = transport.requests[0]
    brk = _records_by_id(request)["kafka:broker:clstr-1:1"]
    assert brk.mapping_reference == _mapping_reference(_CONNECTOR, "Broker")
    assert brk.payload["brokerHost"] == "b1"
    assert brk.payload["brokerPort"] == 9092


@pytest.mark.asyncio
async def test_ingest_cdc_connectors_maps_connector_and_slot(ingest):
    service, transport = ingest
    res = await ingest_cdc_connectors(
        [
            {
                "name": "ca51pilot",
                "connector_class": "io.debezium.connector.postgresql.PostgresConnector",
                "state": "RUNNING",
                "tasks_max": 1,
                "topics": ["cdc.ca51pilot.public.orders"],
                "slot_name": "ca_ca51pilot",
                "cluster_id": "clstr-1",
            }
        ],
        ingest=service,
    )
    assert res == {"nodes": 2, "edges": 2}
    request = transport.requests[0]
    records = _records_by_id(request)
    connector = records["kafka:cdcconnector:clstr-1:ca51pilot"]
    assert connector.mapping_reference == _mapping_reference(_CONNECTOR, "CdcConnector")
    assert connector.payload["connectorState"] == "RUNNING"
    endpoints = {
        (r.source.record_id, r.target.record_id, r.relation_reference)
        for r in request.relationships
    }
    assert (
        "kafka:cdcconnector:clstr-1:ca51pilot",
        "kafka:topic:clstr-1:cdc.ca51pilot.public.orders",
        _relation_reference(_CONNECTOR, "CdcConnector", "cdcTracksTopic"),
    ) in endpoints
    assert (
        "kafka:cdcconnector:clstr-1:ca51pilot",
        "kafka:replicationslot:clstr-1:ca51pilot:ca_ca51pilot",
        _relation_reference(_CONNECTOR, "CdcConnector", "usesReplicationSlot"),
    ) in endpoints


@pytest.mark.asyncio
async def test_empty_native_ingest_is_rejected(ingest):
    service, _ = ingest
    with pytest.raises(IngestError, match="at least one entity"):
        await ingest_entities([], ingest=service)


@pytest.mark.asyncio
async def test_retired_node_type_alias_is_rejected(ingest):
    service, _ = ingest
    with pytest.raises(IngestError, match="needs an id and a node_type"):
        await ingest_entities(
            [{"id": "retired", "type": "RetiredAlias"}],
            ingest=service,
        )
