"""CDC-specific read tools and KG ingest wiring for the Kafka Connect surface.

Resolves topic <-> ontology mappings from a connector's own declared config
(never inferred/guessed for an unmapped topic — mirrors DEC-CA-03's quarantine
rule), wraps the existing ``kafka_groups`` lag calls scoped to a caller-supplied
consumer group, and reports replication-slot identity as declared by the
connector's own config — never by querying a source database directly. Slot
*health* is read through sql-mcp's ``sql_query`` tool, not embedded here.
"""

import json
from typing import Any

from fastmcp import FastMCP
from pydantic import Field

from kafka_mcp.auth import get_client, get_connect_client


def _debezium_topic_map(config: dict[str, Any]) -> dict[str, Any]:
    """Derive the connector's declared source tables and their CDC topic names.

    Reads Debezium's standard ``table.include.list`` (comma-separated
    ``schema.table`` entries) and ``topic.prefix`` (falling back to the legacy
    ``database.server.name``) config keys. Debezium's default topic-naming
    convention is ``<topic.prefix>.<schema>.<table>`` — this is read from the
    connector's own config, never guessed for a table not declared there.
    """
    prefix = config.get("topic.prefix") or config.get("database.server.name")
    tables_raw = config.get("table.include.list") or ""
    tables = [t.strip() for t in tables_raw.split(",") if t.strip()]
    mapped: list[dict[str, str]] = []
    for table in tables:
        parts = table.split(".", 1)
        schema, table_name = (parts[0], parts[1]) if len(parts) == 2 else ("", parts[0])
        topic = f"{prefix}.{table}" if prefix else None
        mapped.append(
            {"table": table, "schema": schema, "table_name": table_name, "topic": topic}
        )
    return {
        "topic_prefix": prefix,
        "tables": mapped,
        "topics": [m["topic"] for m in mapped if m["topic"]],
    }


def register_kafka_cdc_tools(mcp: FastMCP) -> None:
    """Register CDC topic-mapping, lag, slot-health, and KG-ingest tools."""

    @mcp.tool(tags={"cdc"})
    async def kafka_cdc_topic_map(
        params_json: str = Field(
            description=(
                'JSON of arguments: {"connector": "ca51pilot"} (required). Reads '
                "the connector's own 'table.include.list'/'topic.prefix' config "
                "via Kafka Connect REST and resolves each declared table to its "
                "CDC topic name and this package's Topic ontology class — never "
                "infers a mapping for a table the connector does not declare."
            )
        ),
    ) -> Any:
        """Resolve a Debezium connector's declared source tables to CDC topics.

        Returns ``{"connector": ..., "topic_prefix": ..., "tables": [...], "map":
        [{"table", "topic", "ontology_class": "Topic", "handler":
        "kafka_ingest_cdc_topology"}]}``. An empty ``tables`` list means the
        connector's config declares no ``table.include.list`` — this is
        surfaced as-is, not papered over with a guess.
        """
        client = get_connect_client()
        p = json.loads(params_json) if params_json else {}
        name = p["connector"]
        config = client.get_connector_config(name)
        resolved = _debezium_topic_map(config)
        return {
            "connector": name,
            "topic_prefix": resolved["topic_prefix"],
            "map": [
                {
                    "table": m["table"],
                    "topic": m["topic"],
                    # This package's own Topic resource is the only ontology
                    # class a CDC topic resolves to today; the
                    # ChangeEnvelope-handler mapping downstream of the topic
                    # (CA-21/CA-22) is out of this lane's scope.
                    "ontology_class": "Topic" if m["topic"] else None,
                    "handler": "kafka_ingest_cdc_topology" if m["topic"] else None,
                }
                for m in resolved["tables"]
            ],
        }

    @mcp.tool(tags={"cdc"})
    async def kafka_cdc_lag(
        params_json: str = Field(
            description=(
                'JSON of arguments: {"group": "cdc-consumers"} (required) — the '
                "consumer group reading the CDC topics (au's own consumer group "
                'name; not created by this package). Optional {"topics": '
                '["cdc.ca51pilot.public.orders"]} to filter the per-partition '
                "lag response to only those topics."
            )
        ),
    ) -> Any:
        """Report consumer-group lag for a CDC-reading group.

        Thin wrapper over ``kafka_groups``'s existing ``lag_summary``/``lags``
        REST-Proxy calls — no new lag-computation logic. Requires a Confluent
        REST Proxy reachable at ``KAFKA_REST_URL``; Kafka Connect's own REST API
        (``KAFKA_CONNECT_URL``) is not involved here since Debezium source
        connectors do not read through a consumer group themselves — this
        reports lag for whatever downstream consumer group *reads* the CDC
        topics.
        """
        client = get_client()
        p = json.loads(params_json) if params_json else {}
        group = p["group"]
        summary = client.get_group_lag_summary(group)
        lags = client.get_group_lags(group)
        topics = p.get("topics")
        if topics:
            items = lags.get("data") if isinstance(lags, dict) else lags
            if isinstance(items, list):
                filtered = [
                    item
                    for item in items
                    if isinstance(item, dict) and item.get("topic_name") in topics
                ]
                if isinstance(lags, dict):
                    lags = {**lags, "data": filtered}
                else:
                    lags = filtered
        return {"group": group, "lag_summary": summary, "lags": lags}

    @mcp.tool(tags={"cdc"})
    async def kafka_cdc_slot_health(
        params_json: str = Field(
            description=(
                'JSON of arguments: {"connector": "ca51pilot"} (required).'
            )
        ),
    ) -> Any:
        """Report what Kafka Connect exposes about a source connector's slot.

        Returns the connector's live ``status`` (from Connect REST) plus its
        declared ``slot.name``/``database.dbname`` config, and a ``see_also``
        hint pointing at sql-mcp's ``sql_query`` tool for the actual
        ``pg_replication_slots`` row — this package never embeds a Postgres
        client and never claims slot health it cannot observe through Connect
        REST alone.
        """
        client = get_connect_client()
        p = json.loads(params_json) if params_json else {}
        name = p["connector"]
        status = client.get_connector_status(name)
        config = client.get_connector_config(name)
        slot_name = config.get("slot.name")
        database = config.get("database.dbname")
        return {
            "connector": name,
            "connect_status": status,
            "declared_slot_name": slot_name,
            "declared_database": database,
            "see_also": {
                "tool": "sql-mcp:sql_query",
                "hint": (
                    "SELECT * FROM pg_replication_slots WHERE slot_name = "
                    f"'{slot_name}'" if slot_name else
                    "SELECT * FROM pg_replication_slots"
                ),
                "reason": (
                    "kafka-mcp never embeds a database client (DEC-CA-08); "
                    "cross-check live slot health (active/restart_lsn/"
                    "confirmed_flush_lsn) through sql-mcp."
                ),
            },
        }

    @mcp.tool(tags={"cdc", "kg", "kg_ingest"})
    async def kafka_ingest_cdc_topology(
        params_json: str = Field(
            default="{}",
            description=(
                'JSON of arguments, all optional: {"connectors": ["ca51pilot"], '
                '"cluster_id": "<id>"}. Defaults to every connector Kafka '
                "Connect currently lists."
            ),
        ),
    ) -> Any:
        """Ingest the live Kafka Connect CDC topology into the knowledge graph.

        Lists connectors (or a caller-supplied subset) via Connect REST, reads
        each one's config to resolve its CDC topics and declared replication
        slot, and pushes them as typed ``:CdcConnector`` (+ ``:ReplicationSlot``
        when a slot is declared) nodes with ``:cdcTracksTopic`` /
        ``:usesReplicationSlot`` links (Wire-First). Native-ingest failures
        propagate to the caller.
        """
        from kafka_mcp.kg_ingest import ingest_cdc_connectors

        client = get_connect_client()
        p = json.loads(params_json) if params_json else {}
        cid = p.get("cluster_id")
        names = p.get("connectors")
        if not names:
            listed = client.list_connectors()
            names = listed if isinstance(listed, list) else list(listed or [])

        normalized: list[dict[str, Any]] = []
        for name in names:
            status = client.get_connector_status(name)
            config = client.get_connector_config(name)
            resolved = _debezium_topic_map(config)
            connector_block = status.get("connector") if isinstance(status, dict) else {}
            normalized.append(
                {
                    "name": name,
                    "connector_class": config.get("connector.class"),
                    "state": (connector_block or {}).get("state"),
                    "tasks_max": (
                        int(config["tasks.max"]) if config.get("tasks.max") else None
                    ),
                    "topics": resolved["topics"],
                    "slot_name": config.get("slot.name"),
                    "cluster_id": cid,
                }
            )
        result = ingest_cdc_connectors(normalized, graph=None)
        return {"connectors_ingested": len(normalized), "ingested": result}
