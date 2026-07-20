"""Native epistemic-graph ingestion for Kafka records.

CONCEPT:AU-KG.ingest.enterprise-source-extractor. Connector-specific mappers emit
canonical node_type nodes and relationship edges. The required agent-utilities
native-ingest primitive owns the transaction and raises NativeIngestError when the
authoritative engine cannot commit.
"""

from __future__ import annotations

from typing import Any

from agent_utilities.knowledge_graph.memory.native_ingest import (
    ingest_documents as _native_ingest_documents,
)
from agent_utilities.knowledge_graph.memory.native_ingest import (
    ingest_entities as _native_ingest_entities,
)

_SOURCE = "kafka-mcp"
_DOMAIN = "kafka"


def ingest_entities(
    entities: list[dict[str, Any]],
    relationships: list[dict[str, Any]] | None = None,
    *,
    source: str = _SOURCE,
    domain: str = _DOMAIN,
    client: Any | None = None,
    graph: str | None = None,
) -> dict[str, int]:
    """Write canonical typed nodes and relationships through agent-utilities."""
    return _native_ingest_entities(
        entities,
        relationships,
        source=source,
        domain=domain,
        client=client,
        graph=graph,
    )


def ingest_documents(
    documents: list[dict[str, Any]],
    *,
    source: str = _SOURCE,
    domain: str = _DOMAIN,
    client: Any | None = None,
    graph: str | None = None,
) -> dict[str, int]:
    """Write searchable documents through the authoritative native-ingest path."""
    return _native_ingest_documents(
        documents,
        source=source,
        domain=domain,
        client=client,
        graph=graph,
    )


def _records(data: Any) -> list[dict[str, Any]]:
    """Pull the ``data`` list out of a REST Proxy v3 collection response."""
    if isinstance(data, dict):
        items = data.get("data")
        if isinstance(items, list):
            return items
    if isinstance(data, list):
        return data
    return []


def ingest_topics(
    topics: Any,
    *,
    cluster_id: str | None = None,
    client: Any | None = None,
    graph: str | None = None,
) -> dict[str, int]:
    """Map REST Proxy topic records -> ``:Topic`` (+ ``:KafkaCluster``) nodes and ingest."""
    entities: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []
    seen_clusters: set[str] = set()
    for t in _records(topics):
        name = t.get("topic_name") or t.get("name")
        if not name:
            continue
        cid = t.get("cluster_id") or cluster_id or "default"
        tid = f"kafka:topic:{cid}:{name}"
        entities.append(
            {
                "id": tid,
                "node_type": "Topic",
                "name": name,
                "partitionsCount": t.get("partitions_count"),
                "replicationFactor": t.get("replication_factor"),
                "isInternal": t.get("is_internal"),
                "externalToolId": name,
            }
        )
        cluster_node_id = f"kafka:cluster:{cid}"
        if cid not in seen_clusters:
            seen_clusters.add(cid)
            entities.append(
                {"id": cluster_node_id, "node_type": "KafkaCluster", "name": cid}
            )
        relationships.append(
            {"source": tid, "target": cluster_node_id, "relationship": "inCluster"}
        )
    return ingest_entities(entities, relationships, client=client, graph=graph)


def ingest_partitions(
    partitions: Any,
    *,
    topic: str,
    cluster_id: str | None = None,
    client: Any | None = None,
    graph: str | None = None,
) -> dict[str, int]:
    """Map partition records -> ``:Partition`` nodes linked ``:partitionOf`` a Topic."""
    entities: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []
    for p in _records(partitions):
        pid = p.get("partition_id")
        if pid is None:
            continue
        cid = p.get("cluster_id") or cluster_id or "default"
        tname = p.get("topic_name") or topic
        node_id = f"kafka:partition:{cid}:{tname}:{pid}"
        topic_id = f"kafka:topic:{cid}:{tname}"
        entities.append(
            {
                "id": node_id,
                "node_type": "Partition",
                "partitionId": pid,
                "name": f"{tname}-{pid}",
                "externalToolId": str(pid),
            }
        )
        relationships.append(
            {"source": node_id, "target": topic_id, "relationship": "partitionOf"}
        )
    return ingest_entities(entities, relationships, client=client, graph=graph)


def ingest_consumer_groups(
    groups: Any,
    *,
    cluster_id: str | None = None,
    client: Any | None = None,
    graph: str | None = None,
) -> dict[str, int]:
    """Map consumer-group records -> ``:ConsumerGroup`` (+ ``:KafkaCluster``) nodes."""
    entities: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []
    seen_clusters: set[str] = set()
    for g in _records(groups):
        gid = g.get("consumer_group_id") or g.get("group_id")
        if not gid:
            continue
        cid = g.get("cluster_id") or cluster_id or "default"
        node_id = f"kafka:group:{cid}:{gid}"
        entities.append(
            {
                "id": node_id,
                "node_type": "ConsumerGroup",
                "name": gid,
                "groupState": g.get("state"),
                "externalToolId": gid,
            }
        )
        cluster_node_id = f"kafka:cluster:{cid}"
        if cid not in seen_clusters:
            seen_clusters.add(cid)
            entities.append(
                {"id": cluster_node_id, "node_type": "KafkaCluster", "name": cid}
            )
        relationships.append(
            {"source": node_id, "target": cluster_node_id, "relationship": "inCluster"}
        )
    return ingest_entities(entities, relationships, client=client, graph=graph)


def ingest_brokers(
    brokers: Any,
    *,
    cluster_id: str | None = None,
    client: Any | None = None,
    graph: str | None = None,
) -> dict[str, int]:
    """Map broker records -> ``:Broker`` (+ ``:KafkaCluster``) nodes."""
    entities: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []
    seen_clusters: set[str] = set()
    for b in _records(brokers):
        bid = b.get("broker_id")
        if bid is None:
            continue
        cid = b.get("cluster_id") or cluster_id or "default"
        node_id = f"kafka:broker:{cid}:{bid}"
        entities.append(
            {
                "id": node_id,
                "node_type": "Broker",
                "name": f"broker-{bid}",
                "brokerHost": b.get("host"),
                "brokerPort": b.get("port"),
                "externalToolId": str(bid),
            }
        )
        cluster_node_id = f"kafka:cluster:{cid}"
        if cid not in seen_clusters:
            seen_clusters.add(cid)
            entities.append(
                {"id": cluster_node_id, "node_type": "KafkaCluster", "name": cid}
            )
        relationships.append(
            {"source": node_id, "target": cluster_node_id, "relationship": "inCluster"}
        )
    return ingest_entities(entities, relationships, client=client, graph=graph)
