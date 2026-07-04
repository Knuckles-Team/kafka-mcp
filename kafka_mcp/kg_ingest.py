"""Native epistemic-graph ingestion for Kafka records (typed graph nodes).

CONCEPT:AU-KG.ingest.enterprise-source-extractor. kafka-mcp natively pushes its
event-streaming topology into the ONE epistemic-graph knowledge graph as **typed OWL
nodes** (``:KafkaCluster``, ``:Topic``, ``:Partition``, ``:ConsumerGroup``, ``:Broker``)
plus their links (``:inCluster``, ``:partitionOf``, ``:subscribesTo`` …), matching the
classes federated by :mod:`kafka_mcp.ontology`.

The heavy lifting (the engine txn dance) is delegated to the shared fleet primitive
``agent_utilities.knowledge_graph.memory.native_ingest``. That primitive is not yet in
every installed ``agent_utilities``, so the import is **guarded**: when it is missing we
fall back to a self-contained txn implementation over the lightweight engine client
(``GraphComputeEngine()._client``). Either way everything is best-effort and
dependency-/engine-guarded — with no KG stack or no reachable engine every entry point
**no-ops** (returns ``None``), so the connector runs with zero KG infrastructure. Node
ids follow ``kafka:<class>:<externalId>``.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("kafka_mcp.kg")

_SOURCE = "kafka-mcp"
_DOMAIN = "kafka"
_DEFAULT_GRAPH = "__commons__"


# --------------------------------------------------------------------------- #
# Write path — prefer the shared fleet primitive; fall back to a local txn.
# --------------------------------------------------------------------------- #
def _shared() -> Any | None:
    """Return the shared ``native_ingest`` module, or ``None`` if unavailable."""
    try:
        from agent_utilities.knowledge_graph.memory import native_ingest
    except Exception as e:  # noqa: BLE001 — primitive not in installed AU yet
        logger.debug("shared native_ingest unavailable: %s", e)
        return None
    return native_ingest


def _local_client() -> tuple[Any | None, str]:
    """Fallback: build the lightweight engine client, or ``(None, "")``."""
    try:
        from agent_utilities.knowledge_graph.core.graph_compute import (
            GraphComputeEngine,
        )
    except Exception as e:  # noqa: BLE001 — KG stack absent
        logger.debug("KG ingest unavailable (import): %s", e)
        return None, ""
    try:
        engine = GraphComputeEngine()
        client = getattr(engine, "_client", None)
        if client is None:
            return None, ""
        return client, (getattr(engine, "graph_name", None) or _DEFAULT_GRAPH)
    except Exception as e:  # noqa: BLE001 — engine unreachable
        logger.debug("KG ingest: engine unreachable: %s", e)
        return None, ""


def _local_write_nodes(
    nodes: list[dict[str, Any]],
    relationships: list[dict[str, Any]] | None,
    *,
    source: str,
    domain: str,
    client: Any | None,
    graph: str | None,
) -> dict[str, int] | None:
    """Self-contained txn write used when the shared primitive is absent."""
    nodes = [n for n in nodes if n.get("id")]
    if not nodes:
        return None
    if client is None:
        client, graph = _local_client()
    if client is None:
        return None
    graph = graph or _DEFAULT_GRAPH
    try:
        txn = client.txn.begin(graph=graph)
        for node in nodes:
            props = {k: v for k, v in node.items() if k != "id" and v is not None}
            props.setdefault("source", source)
            props.setdefault("domain", domain)
            client.txn.add_node(txn, node["id"], props)
        committed = client.txn.commit(txn)
    except Exception as e:  # noqa: BLE001 — engine/txn failure is non-fatal
        logger.warning("KG ingest: txn failed: %s", e)
        return None
    if not committed:
        logger.warning("KG ingest: txn not committed (conflict)")
        return None

    edges = 0
    for rel in relationships or []:
        try:
            client.edges.add(
                rel["source"], rel["target"], {"type": rel.get("type", "RELATED")}
            )
            edges += 1
        except Exception as e:  # noqa: BLE001 — pure edge link, best-effort
            logger.debug("KG ingest: edge skipped: %s", e)
    logger.info("KG ingest[kafka]: wrote %d nodes, %d edges", len(nodes), edges)
    return {"nodes": len(nodes), "edges": edges}


def ingest_entities(
    entities: list[dict[str, Any]],
    relationships: list[dict[str, Any]] | None = None,
    *,
    source: str = _SOURCE,
    domain: str = _DOMAIN,
    client: Any | None = None,
    graph: str | None = None,
) -> dict[str, int] | None:
    """Write typed OWL nodes (+ edges) into epistemic-graph.

    ``entities``: ``[{"id":..., "type":<owl:Class>, ...props}]``.
    ``relationships``: ``[{"source":id, "target":id, "type":<link>}]``.
    Returns ``{"nodes":n, "edges":m}`` or ``None`` (no engine / failure; never raises).
    """
    entities = [e for e in (entities or []) if e.get("id")]
    if not entities:
        return None
    shared = _shared()
    if shared is not None and client is None:
        return shared.ingest_entities(
            entities, relationships, source=source, domain=domain, graph=graph
        )
    return _local_write_nodes(
        entities,
        relationships,
        source=source,
        domain=domain,
        client=client,
        graph=graph,
    )


def ingest_documents(
    docs: list[dict[str, Any]],
    *,
    source: str = _SOURCE,
    domain: str = _DOMAIN,
    client: Any | None = None,
    graph: str | None = None,
) -> dict[str, int] | None:
    """Write text records as ``:Document`` nodes (semantic-search fodder)."""
    docs = [
        d for d in (docs or []) if d.get("id") and (d.get("text") or d.get("content"))
    ]
    if not docs:
        return None
    shared = _shared()
    if shared is not None and client is None:
        return shared.ingest_documents(docs, source=source, domain=domain, graph=graph)
    nodes: list[dict[str, Any]] = []
    for doc in docs:
        node = {k: v for k, v in doc.items() if k != "content" and v is not None}
        node["id"] = doc["id"]
        node["type"] = "Document"
        node["text"] = doc.get("text") or doc.get("content")
        nodes.append(node)
    return _local_write_nodes(
        nodes, None, source=source, domain=domain, client=client, graph=graph
    )


# --------------------------------------------------------------------------- #
# Record mappers — Confluent REST Proxy v3 shapes -> typed entity/edge dicts.
# --------------------------------------------------------------------------- #
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
) -> dict[str, int] | None:
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
                "type": "Topic",
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
                {"id": cluster_node_id, "type": "KafkaCluster", "name": cid}
            )
        relationships.append(
            {"source": tid, "target": cluster_node_id, "type": "inCluster"}
        )
    return ingest_entities(entities, relationships, client=client, graph=graph)


def ingest_partitions(
    partitions: Any,
    *,
    topic: str,
    cluster_id: str | None = None,
    client: Any | None = None,
    graph: str | None = None,
) -> dict[str, int] | None:
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
                "type": "Partition",
                "partitionId": pid,
                "name": f"{tname}-{pid}",
                "externalToolId": str(pid),
            }
        )
        relationships.append(
            {"source": node_id, "target": topic_id, "type": "partitionOf"}
        )
    return ingest_entities(entities, relationships, client=client, graph=graph)


def ingest_consumer_groups(
    groups: Any,
    *,
    cluster_id: str | None = None,
    client: Any | None = None,
    graph: str | None = None,
) -> dict[str, int] | None:
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
                "type": "ConsumerGroup",
                "name": gid,
                "groupState": g.get("state"),
                "externalToolId": gid,
            }
        )
        cluster_node_id = f"kafka:cluster:{cid}"
        if cid not in seen_clusters:
            seen_clusters.add(cid)
            entities.append(
                {"id": cluster_node_id, "type": "KafkaCluster", "name": cid}
            )
        relationships.append(
            {"source": node_id, "target": cluster_node_id, "type": "inCluster"}
        )
    return ingest_entities(entities, relationships, client=client, graph=graph)


def ingest_brokers(
    brokers: Any,
    *,
    cluster_id: str | None = None,
    client: Any | None = None,
    graph: str | None = None,
) -> dict[str, int] | None:
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
                "type": "Broker",
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
                {"id": cluster_node_id, "type": "KafkaCluster", "name": cid}
            )
        relationships.append(
            {"source": node_id, "target": cluster_node_id, "type": "inCluster"}
        )
    return ingest_entities(entities, relationships, client=client, graph=graph)
