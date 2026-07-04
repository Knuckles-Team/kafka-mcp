---
name: kafka-consumer-group-lag
description: >-
  Monitor Apache Kafka consumer groups and their lag via the kafka-mcp MCP server
  — list groups, describe a group, enumerate its member consumers, and read the
  lag summary or per-partition lags with the domain-typed kafka_groups tool over
  the Confluent REST Proxy. Use when the agent must find stalled/lagging groups,
  diagnose a rebalance, or report how far a group trails the log-end offset. Do
  NOT use for producing/consuming records (use kafka-streaming-io) or topic
  creation/config (use kafka-topic-administration).
license: MIT
tags: [kafka, consumer-groups, lag, monitoring, mcp]
metadata:
  author: Genius
  version: '0.1.0'
---
# Kafka Consumer-Group Lag

Domain-typed inspection of Apache Kafka **consumer groups** — membership, state,
and **lag** — through the Confluent REST Proxy v3. Prefer the `kafka_groups` tool
over raw HTTP; it returns group/consumer/lag-shaped records.

## When to use
- List consumer groups in the cluster.
- Describe a group (state, coordinator, partition assignor).
- List a group's member consumers (instances).
- Read a group's aggregate lag summary or its per-partition lags.

## When NOT to use
- Producing / consuming records or driving a consumer instance → `kafka-streaming-io`.
- Topic / partition provisioning or config → `kafka-topic-administration`.
- Broker / cluster / ACL metadata → the `kafka_cluster` tool directly.

## Prerequisites & environment
Connect via the `mcp-client` skill against the **`kafka-mcp`** MCP server.

| Variable | Required | Notes |
|----------|----------|-------|
| `KAFKA_REST_URL` | ✅ | Confluent REST Proxy base URL (default `http://localhost:8082`) |
| `KAFKA_CLUSTER_ID` | optional | Pin the cluster id; else auto-resolved |
| `KAFKA_TOKEN` / `KAFKA_USERNAME` / `KAFKA_PASSWORD` | optional | Bearer or basic auth |
| `KAFKA_SSL_VERIFY` | optional | TLS verification toggle |

`MCP_TOOL_MODE` selects condensed vs. verbose surfaces.

## Tools & actions
| Condensed tool | Actions |
|----------------|---------|
| `kafka_groups` | `list`, `describe`, `consumers`, `lag_summary`, `lags` |

## Recipes (`params_json`)
List all consumer groups:
```json
{}
```
Describe a group / list its members:
```json
{"group":"analytics"}
```
Aggregate lag summary for a group:
```json
{"group":"analytics"}
```
Per-partition lags (find the hottest partition):
```json
{"group":"analytics"}
```

## Gotchas
- `params_json` is a **string** of JSON, not an object.
- `lag_summary` gives the max/total lag across the group; `lags` breaks it down
  per topic-partition — use `lags` to localize a stuck partition.
- A group in `PreparingRebalance` or `Empty` state can report transient or zero
  lag; re-check once it returns to `Stable`.
- Lag is `log_end_offset - current_offset`; a group with no committed offsets
  yet may show lag equal to the full backlog.

## Related
- `kafka-streaming-io` — drive the consumers whose lag you are watching.
- `kafka-topic-administration` — inspect the partitions the lag is measured over.
- `kafka_ingest_catalog` — snapshot groups + topics into the KG as `:ConsumerGroup`
  and `:Topic` nodes for cross-session analysis.
