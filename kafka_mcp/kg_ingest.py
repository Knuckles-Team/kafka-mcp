"""Native epistemic-graph ingestion for Kafka records.

CONCEPT:AU-KG.ingest.enterprise-source-extractor. Connector-specific mappers emit
canonical node_type nodes and relationship edges through the generated
``agent_connector_sdk.ingest`` SourceIngest client, not a local ingestion helper.
"""

from __future__ import annotations

from typing import Any

from agent_connector_sdk.ingest import (
    ChangeSet,
    Document,
    Entity,
    IngestBinding,
    IngestError,
    KnowledgeIngest,
    Relationship,
    current_ingest,
)

_BINDING = IngestBinding(connector="kafka-mcp", stream="kafka")

_ENTITY_RESERVED_KEYS = frozenset({"id", "node_type"})
_RELATIONSHIP_RESERVED_KEYS = frozenset({"source", "target", "relationship"})


def _to_entity(record: dict[str, Any]) -> Entity:
    return Entity(
        id=record.get("id"),
        node_type=record.get("node_type"),
        properties={
            key: value
            for key, value in record.items()
            if key not in _ENTITY_RESERVED_KEYS
        },
    )


def _to_relationship(record: dict[str, Any]) -> Relationship:
    properties = {
        key: value
        for key, value in record.items()
        if key not in _RELATIONSHIP_RESERVED_KEYS
    }
    return Relationship(
        source=record["source"],
        target=record["target"],
        relationship=record["relationship"],
        properties=properties or None,
    )


async def ingest_entities(
    entities: list[dict[str, Any]],
    relationships: list[dict[str, Any]] | None = None,
    *,
    ingest: KnowledgeIngest | None = None,
) -> dict[str, int]:
    """Write canonical typed nodes and relationships through the SDK ingest facade."""
    if not entities:
        raise IngestError("ingest_entities needs at least one entity")
    change_set = ChangeSet(
        entities=tuple(_to_entity(entity) for entity in entities),
        relationships=tuple(
            _to_relationship(relationship) for relationship in relationships or ()
        ),
    )
    service = ingest or current_ingest()
    receipt = await service.submit(_BINDING, change_set)
    return {"nodes": receipt.affected_count, "edges": receipt.relationship_count}


async def ingest_documents(
    documents: list[dict[str, Any]],
    *,
    ingest: KnowledgeIngest | None = None,
) -> dict[str, int]:
    """Write searchable documents through the SDK ingest facade."""
    if not documents:
        raise IngestError("ingest_documents needs at least one document")
    change_set = ChangeSet(
        documents=tuple(
            Document(
                id=doc["id"],
                text=doc["text"],
                title=doc.get("title"),
                source_uri=doc.get("source_uri"),
                properties={
                    key: value
                    for key, value in doc.items()
                    if key not in {"id", "text", "title", "source_uri"}
                },
            )
            for doc in documents
        )
    )
    service = ingest or current_ingest()
    receipt = await service.submit(_BINDING, change_set)
    return {"nodes": receipt.affected_count, "edges": receipt.relationship_count}


def _records(data: Any) -> list[dict[str, Any]]:
    """Pull the ``data`` list out of a REST Proxy v3 collection response."""
    if isinstance(data, dict):
        items = data.get("data")
        if isinstance(items, list):
            return items
    if isinstance(data, list):
        return data
    return []


async def ingest_topics(
    topics: Any,
    *,
    cluster_id: str | None = None,
    ingest: KnowledgeIngest | None = None,
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
    return await ingest_entities(entities, relationships, ingest=ingest)


async def ingest_partitions(
    partitions: Any,
    *,
    topic: str,
    cluster_id: str | None = None,
    ingest: KnowledgeIngest | None = None,
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
    return await ingest_entities(entities, relationships, ingest=ingest)


async def ingest_consumer_groups(
    groups: Any,
    *,
    cluster_id: str | None = None,
    ingest: KnowledgeIngest | None = None,
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
    return await ingest_entities(entities, relationships, ingest=ingest)


async def ingest_brokers(
    brokers: Any,
    *,
    cluster_id: str | None = None,
    ingest: KnowledgeIngest | None = None,
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
    return await ingest_entities(entities, relationships, ingest=ingest)


async def ingest_cdc_connectors(
    connectors: list[dict[str, Any]],
    *,
    ingest: KnowledgeIngest | None = None,
) -> dict[str, int]:
    """Map normalized Kafka Connect connector records -> ``:CdcConnector`` nodes.

    ``connectors`` is a list of pre-normalized dicts (assembled by the caller from
    Connect REST ``/connectors?expand=status`` + per-connector ``/config``), each
    shaped::

        {"name": str, "connector_class": str | None, "state": str | None,
         "tasks_max": int | None, "topics": list[str], "slot_name": str | None,
         "cluster_id": str | None}

    Emits one ``:CdcConnector`` node per entry, one ``:cdcTracksTopic`` edge per
    listed topic (topic nodes are NOT created here — they must already exist via
    :func:`ingest_topics`; an edge to a not-yet-ingested topic id is still written,
    matching the rest of this module's Wire-First, best-effort convention), and
    one ``:usesReplicationSlot`` edge to a ``:ReplicationSlot`` node (created here)
    when ``slot_name`` is present. Never queries a source database directly — slot
    identity comes only from the connector's own declared config.
    """
    entities: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []
    for c in connectors:
        name = c.get("name")
        if not name:
            continue
        cid = c.get("cluster_id") or "default"
        node_id = f"kafka:cdcconnector:{cid}:{name}"
        entities.append(
            {
                "id": node_id,
                "node_type": "CdcConnector",
                "name": name,
                "connectorClass": c.get("connector_class"),
                "connectorState": c.get("state"),
                "tasksMax": c.get("tasks_max"),
                "externalToolId": name,
            }
        )
        for topic in c.get("topics") or []:
            topic_id = f"kafka:topic:{cid}:{topic}"
            relationships.append(
                {
                    "source": node_id,
                    "target": topic_id,
                    "relationship": "cdcTracksTopic",
                }
            )
        slot_name = c.get("slot_name")
        if slot_name:
            slot_id = f"kafka:replicationslot:{cid}:{name}:{slot_name}"
            entities.append(
                {
                    "id": slot_id,
                    "node_type": "ReplicationSlot",
                    "name": slot_name,
                    "slotName": slot_name,
                    "externalToolId": slot_name,
                }
            )
            relationships.append(
                {
                    "source": node_id,
                    "target": slot_id,
                    "relationship": "usesReplicationSlot",
                }
            )
    return await ingest_entities(entities, relationships, ingest=ingest)
