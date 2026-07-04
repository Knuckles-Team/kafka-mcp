"""Native epistemic-graph typed-node ingestion — Wire-First coverage.

Exercises the real ``ingest_entities`` / ``ingest_topics`` / ``ingest_partitions`` /
``ingest_consumer_groups`` / ``ingest_brokers`` seams with a fake engine client (no
engine required), asserting the txn add_node/commit + edge calls and the Kafka
REST-Proxy record → :Topic/:Partition/:ConsumerGroup/:Broker/:KafkaCluster mapping.
CONCEPT:AU-KG.ingest.enterprise-source-extractor.
"""

from __future__ import annotations

from kafka_mcp.kg_ingest import (
    ingest_brokers,
    ingest_consumer_groups,
    ingest_entities,
    ingest_partitions,
    ingest_topics,
)


class _FakeTxn:
    def __init__(self):
        self.nodes = {}
        self.committed = False

    def begin(self, graph=None):
        self.graph = graph
        return "txn-1"

    def add_node(self, txn, node_id, props):
        self.nodes[node_id] = props

    def commit(self, txn):
        self.committed = True
        return True


class _FakeEdges:
    def __init__(self):
        self.edges = []

    def add(self, src, dst, props):
        self.edges.append((src, dst, props))


class _FakeClient:
    def __init__(self):
        self.txn = _FakeTxn()
        self.edges = _FakeEdges()


def test_ingest_entities_writes_nodes_and_edges():
    c = _FakeClient()
    res = ingest_entities(
        [
            {"id": "a", "type": "Topic", "name": "events"},
            {"id": "b", "type": "KafkaCluster"},
        ],
        [{"source": "a", "target": "b", "type": "inCluster"}],
        client=c,
        graph="__commons__",
    )
    assert res == {"nodes": 2, "edges": 1}
    assert c.txn.committed is True
    assert set(c.txn.nodes) == {"a", "b"}
    # provenance is stamped
    assert c.txn.nodes["a"]["source"] == "kafka-mcp"
    assert c.txn.nodes["a"]["domain"] == "kafka"
    assert c.edges.edges == [("a", "b", {"type": "inCluster"})]


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
        graph="__commons__",
    )
    assert res == {"nodes": 2, "edges": 1}
    topic = c.txn.nodes["kafka:topic:clstr-1:events"]
    assert topic["type"] == "Topic"
    assert topic["partitionsCount"] == 3
    assert topic["replicationFactor"] == 2
    assert topic["externalToolId"] == "events"
    assert c.txn.nodes["kafka:cluster:clstr-1"]["type"] == "KafkaCluster"
    assert c.edges.edges == [
        ("kafka:topic:clstr-1:events", "kafka:cluster:clstr-1", {"type": "inCluster"})
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
        graph="__commons__",
    )
    assert res == {"nodes": 2, "edges": 2}
    p0 = c.txn.nodes["kafka:partition:clstr-1:events:0"]
    assert p0["type"] == "Partition"
    assert p0["partitionId"] == 0
    assert (
        "kafka:partition:clstr-1:events:0",
        "kafka:topic:clstr-1:events",
        {"type": "partitionOf"},
    ) in c.edges.edges


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
        graph="__commons__",
    )
    assert res == {"nodes": 2, "edges": 1}
    grp = c.txn.nodes["kafka:group:clstr-1:analytics"]
    assert grp["type"] == "ConsumerGroup"
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
        graph="__commons__",
    )
    assert res == {"nodes": 2, "edges": 1}
    brk = c.txn.nodes["kafka:broker:clstr-1:1"]
    assert brk["type"] == "Broker"
    assert brk["brokerHost"] == "b1"
    assert brk["brokerPort"] == 9092


def test_ingest_noops_without_engine():
    # No injected client + no reachable engine -> clean no-op.
    assert ingest_entities([{"id": "a", "type": "Topic"}]) is None


def test_ingest_empty_is_noop():
    assert ingest_entities([], client=_FakeClient()) is None
    assert ingest_topics({"data": []}, client=_FakeClient()) is None
    assert ingest_partitions({"data": []}, topic="x", client=_FakeClient()) is None
    assert ingest_consumer_groups({"data": []}, client=_FakeClient()) is None
    assert ingest_brokers({"data": []}, client=_FakeClient()) is None
