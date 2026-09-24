"""Native epistemic-graph typed-node ingestion — Wire-First coverage.

Exercises the real ``ingest_entities`` / ``ingest_topics`` / ``ingest_partitions`` /
``ingest_consumer_groups`` / ``ingest_brokers`` seams with a fake engine client (no
engine required), asserting the txn add_node/commit + edge calls and the Kafka
REST-Proxy record → :Topic/:Partition/:ConsumerGroup/:Broker/:KafkaCluster mapping.
CONCEPT:AU-KG.ingest.enterprise-source-extractor.
"""

from __future__ import annotations

from typing import Any

import pytest
from kafka_mcp import kg_ingest
from kafka_mcp.kg_ingest import (
    KnowledgeGraphIngestUnavailable,
    ingest_brokers,
    ingest_consumer_groups,
    ingest_entities,
    ingest_partitions,
    ingest_topics,
)


class _FakeSink:
    """Stands in for the retired native-ingest write path (SDK-GAPS.md #0)."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def __call__(self, entities, relationships=None, **kwargs):
        self.calls.append({"entities": entities, "relationships": relationships or []})
        return {"nodes": len(entities), "edges": len(relationships or [])}


def test_ingest_entities_and_documents_are_unavailable():
    """The retired primitives fail closed instead of silently no-op'ing."""
    with pytest.raises(KnowledgeGraphIngestUnavailable):
        ingest_entities([{"id": "a", "node_type": "Topic"}])
    with pytest.raises(KnowledgeGraphIngestUnavailable):
        kg_ingest.ingest_documents([{"id": "a"}])


def test_ingest_topics_maps_topic_and_cluster(monkeypatch):
    sink = _FakeSink()
    monkeypatch.setattr(kg_ingest, "ingest_entities", sink)
    res = ingest_topics(
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
    )
    assert res == {"nodes": 2, "edges": 1}
    entities = {e["id"]: e for e in sink.calls[0]["entities"]}
    topic = entities["kafka:topic:clstr-1:events"]
    assert topic["node_type"] == "Topic"
    assert topic["partitionsCount"] == 3
    assert topic["replicationFactor"] == 2
    assert topic["externalToolId"] == "events"
    assert entities["kafka:cluster:clstr-1"]["node_type"] == "KafkaCluster"
    assert sink.calls[0]["relationships"] == [
        {
            "source": "kafka:topic:clstr-1:events",
            "target": "kafka:cluster:clstr-1",
            "relationship": "inCluster",
        }
    ]


def test_ingest_partitions_maps_partition_of_topic(monkeypatch):
    sink = _FakeSink()
    monkeypatch.setattr(kg_ingest, "ingest_entities", sink)
    res = ingest_partitions(
        {
            "data": [
                {"cluster_id": "clstr-1", "topic_name": "events", "partition_id": 0},
                {"cluster_id": "clstr-1", "topic_name": "events", "partition_id": 1},
            ]
        },
        topic="events",
    )
    assert res == {"nodes": 2, "edges": 2}
    entities = {e["id"]: e for e in sink.calls[0]["entities"]}
    p0 = entities["kafka:partition:clstr-1:events:0"]
    assert p0["node_type"] == "Partition"
    assert p0["partitionId"] == 0
    assert {
        "source": "kafka:partition:clstr-1:events:0",
        "target": "kafka:topic:clstr-1:events",
        "relationship": "partitionOf",
    } in sink.calls[0]["relationships"]


def test_ingest_consumer_groups_maps_group_and_cluster(monkeypatch):
    sink = _FakeSink()
    monkeypatch.setattr(kg_ingest, "ingest_entities", sink)
    res = ingest_consumer_groups(
        {
            "data": [
                {
                    "cluster_id": "clstr-1",
                    "consumer_group_id": "analytics",
                    "state": "STABLE",
                }
            ]
        },
    )
    assert res == {"nodes": 2, "edges": 1}
    entities = {e["id"]: e for e in sink.calls[0]["entities"]}
    grp = entities["kafka:group:clstr-1:analytics"]
    assert grp["node_type"] == "ConsumerGroup"
    assert grp["groupState"] == "STABLE"


def test_ingest_brokers_maps_broker_and_cluster(monkeypatch):
    sink = _FakeSink()
    monkeypatch.setattr(kg_ingest, "ingest_entities", sink)
    res = ingest_brokers(
        {
            "data": [
                {"cluster_id": "clstr-1", "broker_id": 1, "host": "b1", "port": 9092}
            ]
        },
    )
    assert res == {"nodes": 2, "edges": 1}
    entities = {e["id"]: e for e in sink.calls[0]["entities"]}
    brk = entities["kafka:broker:clstr-1:1"]
    assert brk["node_type"] == "Broker"
    assert brk["brokerHost"] == "b1"
    assert brk["brokerPort"] == 9092
