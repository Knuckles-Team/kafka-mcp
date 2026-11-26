---
name: kafka-consumer-group-lag
skill_type: skill
description: >-
  Monitor Apache Kafka consumer groups and their lag via the kafka-mcp MCP server — list groups, describe a group, enumerate its member consumers, and read the lag summary or per-partition lags with the domain-typed group tools over the Confluent REST Proxy. Use when the agent must find stalled/lagging groups, diagnose a rebalance, or report how far a group trails the log-end offset.
license: MIT
tags: [kafka, consumer-groups, lag, monitoring, mcp]
metadata:
  author: Knuckles-Team
  version: '0.1.0'
---
# Kafka Consumer-Group Lag

Scope: Monitor Apache Kafka consumer groups and their lag via the kafka-mcp MCP server — list groups, describe a group, enumerate its member consumers, and read the lag summary or per-partition lags with the domain-typed group tools over the Confluent REST Proxy. Use when the agent must find stalled/lagging groups, diagnose a rebalance, or report how far a group trails the log-end offset. Do NOT use for producing/consuming records (use kafka-streaming-io) or topic creation/config (use kafka-topic-administration).

Domain-typed inspection of Apache Kafka **consumer groups** — membership, state,
and **lag** — through the Confluent REST Proxy v3. Prefer the `kafka_list_consumer_groups` and related group tools
over raw HTTP; it returns group/consumer/lag-shaped records.

## When to use
- List consumer groups in the cluster.
- Describe a group (state, coordinator, partition assignor).
- List a group's member consumers (instances).
- Read a group's aggregate lag summary or its per-partition lags.

## When NOT to use
- Producing / consuming records or driving a consumer instance → `kafka-streaming-io`.
- Topic / partition provisioning or config → `kafka-topic-administration`.
- Broker / cluster / ACL metadata → the `kafka_list_brokers`, `kafka_list_acls` and related cluster tools.

## Prerequisites & environment
Connect via the `mcp-client` skill against the **`kafka-mcp`** MCP server.

| Variable | Required | Notes |
|----------|----------|-------|
| `KAFKA_REST_URL` | ✅ | Confluent REST Proxy base URL (default `http://localhost:8082`) |
| `KAFKA_CLUSTER_ID` | optional | Pin the cluster id; else auto-resolved |
| `KAFKA_TOKEN` / `KAFKA_USERNAME` / `KAFKA_PASSWORD` | optional | Bearer or basic auth |
| `KAFKA_SSL_VERIFY` | optional | TLS verification toggle |


## Steps

1. List groups with `kafka_list_consumer_groups`. Verify the group you need appears in the result.
2. Describe the group with `kafka_get_consumer_group` (`group`). Verify the state is `Stable` before trusting lag figures.
3. List its members with `kafka_list_consumers` (`group`). Verify at least one member is assigned partitions.
4. Read the aggregate with `kafka_get_group_lag_summary` (`group`). Verify the max and total lag are present.
5. Localise a stuck partition with `kafka_get_group_lags` (`group`). Verify the highest-lag topic-partition is named in your report.

## Failure recovery

| Failure | Cause | Recovery |
|---------|-------|----------|
| Connection error | `KAFKA_REST_URL` missing or wrong | Set `KAFKA_REST_URL` to the REST Proxy base URL and call `kafka_list_consumer_groups` again |
| Group state `Empty` or `PreparingRebalance` | Lag is transient | Wait for the group to reach `Stable` and call `kafka_get_group_lag_summary` again |
| Group not found | Wrong group name | Take the exact name from `kafka_list_consumer_groups` |
| 401 or 403 | Credentials missing | Set `KAFKA_TOKEN` or `KAFKA_USERNAME` and `KAFKA_PASSWORD` in the deployment |

## Tools
| Condensed tool | Actions |
|----------------|---------|
| `kafka_list_consumer_groups`, `kafka_get_consumer_group`, `kafka_list_consumers` | group and member inspection |
| `kafka_get_group_lag_summary`, `kafka_get_group_lags` | lag per group and per partition |

## Recipes (tool arguments)
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
