"""Native epistemic-graph typed-node ingestion — Wire-First coverage.

Exercises the real ``ingest_entities`` / ``ingest_topics`` / ``ingest_partitions`` /
``ingest_consumer_groups`` / ``ingest_brokers`` seams with a fake engine client (no
engine required), asserting the txn add_node/commit + edge calls and the Kafka
REST-Proxy record → :Topic/:Partition/:ConsumerGroup/:Broker/:KafkaCluster mapping.
CONCEPT:AU-KG.ingest.enterprise-source-extractor.
"""

from __future__ import annotations

from typing import Any

import msgpack
import pytest
from agent_utilities.knowledge_graph.memory.native_ingest import NativeIngestError
from agent_utilities.security.brain_context import ActorContext, use_actor
from agent_utilities.models.company_brain import ActorType
from agent_utilities.knowledge_graph.core.session import GraphSession, use_session

from kafka_mcp.kg_ingest import (
    ingest_brokers,
    ingest_consumer_groups,
    ingest_entities,
    ingest_partitions,
    ingest_topics,
)


@pytest.fixture(autouse=True)
def _governed_session():
    actor = ActorContext(
        actor_id="subject:opaque:synthetic",
        actor_type=ActorType.AUTOMATED_SERVICE,
        roles=(),
        tenant_id="tenant:opaque:synthetic",
        authenticated=True,
    )
    session = GraphSession(
        actor=actor,
        tenant=actor.tenant_id,
        scopes=frozenset({"kg:write"}),
        graph="graph:opaque:synthetic",
        policy_version="policy:opaque:synthetic",
        audience="epistemic-graph",
    )
    with use_actor(actor), use_session(session):
        yield


class _FakeNodes:
    def __init__(self) -> None:
        self.values: dict[str, dict[str, Any]] = {}

    def properties(self, node_id: str) -> dict[str, Any] | None:
        return self.values.get(node_id)

    def list(self) -> list[tuple[str, dict[str, Any]]]:
        return list(self.values.items())


class _FakeChanges:
    def __init__(self, nodes: _FakeNodes) -> None:
        self.nodes = nodes
        self.edges: list[tuple[str, str, dict[str, Any]]] = []
        self.applied: list[dict[str, Any]] = []
        self.records: dict[str, dict[str, Any]] = {}
        self.versions: dict[str, dict[str, Any]] = {}

    def get(self, envelope_id: str) -> dict[str, Any] | None:
        return self.records.get(envelope_id)

    def content_version(self, object_id: str) -> dict[str, Any] | None:
        return self.versions.get(object_id)

    def cursor(self, _source: str, _partition: str = "") -> None:
        return None

    def apply(self, envelope: dict[str, Any]) -> dict[str, Any]:
        self.applied.append(envelope)
        mutation = envelope["mutation"]
        for operation in mutation["operations"]:
            method = operation["method"]
            params = method["params"]
            properties = msgpack.unpackb(params["properties_msgpack"], raw=False)
            if method["method"] == "AddNode":
                self.nodes.values[params["node_id"]] = properties
            elif method["method"] == "AddEdge":
                self.edges.append(
                    (params["source_id"], params["target_id"], properties)
                )
        version = envelope["content_version"]
        self.versions[version["object_id"]] = version
        self.records[envelope["envelope_id"]] = envelope
        return {
            "batch_id": mutation["batch_id"],
            "replayed": False,
            "projection_pending": False,
        }


class _FakeRdf:
    def validate_shacl(self, _shapes: str, _data_graph: str) -> dict[str, Any]:
        return {"conforms": True, "results": []}


class _FakeClient:
    def __init__(self) -> None:
        self.nodes = _FakeNodes()
        self.changes = _FakeChanges(self.nodes)
        self.rdf = _FakeRdf()

    @staticmethod
    def supports(operation: str) -> bool:
        return operation == "ApplyChangeEnvelope"


def test_ingest_entities_writes_nodes_and_edges():
    c = _FakeClient()
    res = ingest_entities(
        [
            {"id": "a", "node_type": "Topic", "name": "events"},
            {"id": "b", "node_type": "KafkaCluster"},
        ],
        [{"source": "a", "target": "b", "relationship": "inCluster"}],
        client=c,
    )
    assert res == {"nodes": 2, "edges": 1}
    assert len(c.changes.applied) == 1
    assert set(c.nodes.values) == {"a", "b"}
    # provenance is stamped
    assert c.nodes.values["a"]["source"] == "kafka-mcp"
    assert c.nodes.values["a"]["domain"] == "kafka"
    assert c.changes.edges == [("a", "b", {"relationship": "inCluster"})]


def test_ingest_topics_maps_topic_and_cluster():
    c = _FakeClient()
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
        client=c,
    )
    assert res == {"nodes": 2, "edges": 1}
    topic = c.nodes.values["kafka:topic:clstr-1:events"]
    assert topic["node_type"] == "Topic"
    assert topic["partitionsCount"] == 3
    assert topic["replicationFactor"] == 2
    assert topic["externalToolId"] == "events"
    assert c.nodes.values["kafka:cluster:clstr-1"]["node_type"] == "KafkaCluster"
    assert c.changes.edges == [
        ("kafka:topic:clstr-1:events", "kafka:cluster:clstr-1", {"relationship": "inCluster"})
    ]


def test_ingest_partitions_maps_partition_of_topic():
    c = _FakeClient()
    res = ingest_partitions(
        {
            "data": [
                {"cluster_id": "clstr-1", "topic_name": "events", "partition_id": 0},
                {"cluster_id": "clstr-1", "topic_name": "events", "partition_id": 1},
            ]
        },
        topic="events",
        client=c,
    )
    assert res == {"nodes": 2, "edges": 2}
    p0 = c.nodes.values["kafka:partition:clstr-1:events:0"]
    assert p0["node_type"] == "Partition"
    assert p0["partitionId"] == 0
    assert (
        "kafka:partition:clstr-1:events:0",
        "kafka:topic:clstr-1:events",
        {"relationship": "partitionOf"},
    ) in c.changes.edges


def test_ingest_consumer_groups_maps_group_and_cluster():
    c = _FakeClient()
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
        client=c,
    )
    assert res == {"nodes": 2, "edges": 1}
    grp = c.nodes.values["kafka:group:clstr-1:analytics"]
    assert grp["node_type"] == "ConsumerGroup"
    assert grp["groupState"] == "STABLE"


def test_ingest_brokers_maps_broker_and_cluster():
    c = _FakeClient()
    res = ingest_brokers(
        {
            "data": [
                {"cluster_id": "clstr-1", "broker_id": 1, "host": "b1", "port": 9092}
            ]
        },
        client=c,
    )
    assert res == {"nodes": 2, "edges": 1}
    brk = c.nodes.values["kafka:broker:clstr-1:1"]
    assert brk["node_type"] == "Broker"
    assert brk["brokerHost"] == "b1"
    assert brk["brokerPort"] == 9092


def test_retired_node_type_alias_is_rejected():
    with pytest.raises(NativeIngestError, match="canonical node_type"):
        ingest_entities(
            [{"id": "retired", "type": "RetiredAlias"}],
            client=_FakeClient(),
        )


def test_empty_native_ingest_is_rejected():
    with pytest.raises(NativeIngestError, match="at least one entity"):
        ingest_entities([], client=_FakeClient())
