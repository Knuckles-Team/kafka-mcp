---
name: kafka-connect-cdc-operations
description: >-
  Drive Kafka Connect connector lifecycle and read CDC topic maps, lag and declared replication-slot names via the kafka-mcp server. Use to create or check a Debezium connector or find a source table's topic. Do NOT use for producing or consuming (kafka-streaming-io) or topic provisioning (kafka-topic-administration).
---
# Kafka Connect + CDC Operations

Scope: Drive Kafka Connect connector lifecycle (list/get/create/update/delete/status/ restart/pause/resume/offsets) and CDC-specific reads (topic mapping, consumer- group lag for a CDC-reading group, replication-slot identity) via the kafka-mcp MCP server. Use when the agent must create or check a Debezium/CDC connector, find which Kafka topic a source table publishes to, see how far a CDC-reading consumer group trails, or ingest the live Connect topology into the knowledge graph. Do NOT use for producing/consuming records (use kafka-streaming-io), topic/partition provisioning (use kafka-topic-administration), or reading live PostgreSQL replication-slot health directly (use sql-mcp's sql_query — this package never embeds a database client).

Domain-typed control of the **Kafka Connect** worker REST API (connector
lifecycle) and read-only **CDC** helpers (topic mapping, lag, declared slot
identity) through the `kafka-mcp` MCP server. Prefer these tools over raw HTTP —
they carry the correct request/response shapes and never guess a mapping the
connector itself does not declare.

## When to use
- Create, update, or delete a Kafka Connect connector (e.g. a Debezium source).
- Check a connector's/task's live status, restart it, pause/resume it.
- Read a connector's committed offsets.
- List connector plugins available on the Connect worker.
- Resolve which CDC topic(s) a connector's declared source tables publish to.
- Check lag for the consumer group reading CDC topics.
- Get a connector's declared replication-slot name/database (then cross-check
  live slot health through sql-mcp).
- Ingest the live Connect topology (`CdcConnector`/`ReplicationSlot` nodes) into
  the knowledge graph.

## When NOT to use
- Producing/consuming records or driving a consumer instance → `kafka-streaming-io`.
- Topic/partition provisioning or config → `kafka-topic-administration`.
- General consumer-group lag/membership (not CDC-specific) → `kafka-consumer-group-lag`.
- Live PostgreSQL replication-slot health (`active`, `restart_lsn`,
  `confirmed_flush_lsn`) → sql-mcp's `sql_query` against `pg_replication_slots`.
  This package's `kafka_cdc_slot_health` only reports what Connect REST exposes
  plus the connector's *declared* slot name — never a live database read.

## Steps

1. List connectors with `kafka_connect_list` (`expand` set to `status`). Verify the connector you need appears with a state.
2. Create or change one with `kafka_connect_create` (`name`, `config`) or `kafka_connect_update`. Verify `kafka_connect_get` returns the config you sent.
3. Check runtime health with `kafka_connect_status` (`name`). Verify the connector and every task report `RUNNING`.
4. Control a failed connector with `kafka_connect_restart`, `kafka_connect_pause` or `kafka_connect_resume`. Verify `kafka_connect_status` shows the expected state afterwards.
5. Resolve CDC reads with `kafka_cdc_topic_map` (`connector`) and `kafka_cdc_lag` (`group`, `topics`). Verify the topics match the declared `table.include.list` and the lag figures are present.
6. Mirror the topology on request with `kafka_ingest_cdc_topology`. Verify the result reports connector nodes written.

## Failure recovery

| Failure | Cause | Recovery |
|---------|-------|----------|
| 409 Conflict on create | A connector with that name exists | Use `kafka_connect_update` with the full config instead |
| 404 on delete or status | Wrong name or already deleted | List names with `kafka_connect_list` and pass the exact name |
| `kafka_cdc_lag` fails | `KAFKA_REST_URL` not set | Set `KAFKA_REST_URL` to the REST Proxy base URL in the deployment and call it again |
| Empty topic map | Connector config declares no matching tables | Read the declared `table.include.list` with `kafka_connect_get` and report it as undeclared |

## Prerequisites & environment
Connect via the `mcp-client` skill against the **`kafka-mcp`** MCP server.

| Variable | Required | Notes |
|----------|----------|-------|
| `KAFKA_CONNECT_URL` | ✅ | Kafka Connect worker REST base URL (default `http://localhost:8083`) |
| `KAFKA_CONNECT_TOKEN` / `KAFKA_CONNECT_USERNAME` + `KAFKA_CONNECT_PASSWORD` | optional | Connect REST has no documented built-in auth upstream; unauthenticated by default with a loud log line |
| `KAFKA_REST_URL` | for `kafka_cdc_lag` only | Confluent REST Proxy base URL — lag is read through the REST Proxy, not Connect REST |
| `KAFKA_CONNECTTOOL` / `KAFKA_CDCTOOL` | optional | Per-domain toggles; default `True` |


## Tools
| Condensed tool | Actions / params |
|----------------|-------------------|
| `kafka_connect_list`, `kafka_connect_get`, `kafka_connect_create`, `kafka_connect_update`, `kafka_connect_delete` | connector lifecycle |
| `kafka_connect_status`, `kafka_connect_restart`, `kafka_connect_pause`, `kafka_connect_resume`, `kafka_connect_offsets` | runtime control |
| `kafka_connect_plugins` | lists the worker plugins |
| `kafka_cdc_topic_map` | `{"connector": "<name>"}` |
| `kafka_cdc_lag` | `{"group": "<name>", "topics": [...]}` (`topics` optional) |
| `kafka_cdc_slot_health` | `{"connector": "<name>"}` |
| `kafka_ingest_cdc_topology` | `{"connectors": [...], "cluster_id": "..."}` (both optional) |

## Recipes (tool arguments)
List connectors with live status:
```json
{"expand": "status"}
```
Create a Debezium Postgres connector:
```json
{"name": "ca51pilot", "config": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "database.hostname": "ca51-pilot-postgres", "database.port": "5432", "database.dbname": "ca51pilot", "topic.prefix": "cdc.ca51pilot", "table.include.list": "public.orders", "slot.name": "ca_ca51pilot"}}
```
Check status, then restart if failed:
```json
{"name": "ca51pilot"}
```
Resolve a connector's CDC topics from its own declared config:
```json
{"connector": "ca51pilot"}
```
Lag for the group reading the CDC topics:
```json
{"group": "cdc-consumers", "topics": ["cdc.ca51pilot.public.orders"]}
```

## Gotchas
- `params_json` is a **string** of JSON, not an object (`kafka_cdc_topic_map`, `kafka_cdc_lag`, `kafka_cdc_slot_health`,
  `kafka_ingest_cdc_topology`); `kafka_connect_plugins` takes no params.
- `create` is idempotent by connector **name** — a second `create` against an
  existing name surfaces Connect's own `409 Conflict`, never silently upserts.
  Use `update` to change an existing connector's config.
- `delete` and `restart` are **not** idempotent — a repeat `delete` 404s.
- `kafka_cdc_topic_map` and `kafka_cdc_slot_health` only resolve what the
  connector's own config declares (`table.include.list`/`topic.prefix`,
  `slot.name`) — an undeclared table never gets a guessed mapping.
- `kafka_cdc_lag` needs `KAFKA_REST_URL` (the Confluent REST Proxy), not
  `KAFKA_CONNECT_URL` — Debezium source connectors don't read through a
  consumer group themselves; this reports lag for whatever *downstream*
  consumer group reads the CDC topics.
- No governed approval/conflict-policy gate is wired to `kafka_connect`'s
  mutating actions yet — every mutating tool in this package executes with the
  same authority it always has (DEC-CA-07's typed-Action gate is blocked on an
  `ActionSpec` schema extension outside this package; see `connector_manifest.yml`'s
  `review_todos`).

## Related
- `kafka-topic-administration` — create the target topic ahead of a connector if needed.
- `kafka-consumer-group-lag` — general (non-CDC-scoped) group inspection.
- `kafka-streaming-io` — produce/consume records directly.
- sql-mcp's `sql_query` — live `pg_replication_slots` health.
