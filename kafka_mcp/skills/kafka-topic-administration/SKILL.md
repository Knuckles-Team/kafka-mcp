---
name: kafka-topic-administration
skill_type: skill
description: >-
  Administer Apache Kafka topics via the kafka-mcp MCP server — list, create,
  describe, and delete topics, inspect/alter topic configs, and enumerate
  partitions, using the domain-typed kafka_topics / kafka_partitions tools over
  the Confluent REST Proxy (or the native direct-to-broker client). Use when the
  agent must provision a topic with a partition/replication plan, tune retention
  or cleanup configs, or inspect a topic's partition layout. Do NOT use for
  producing/consuming records (use kafka-streaming-io) or consumer-group lag
  monitoring (use kafka-consumer-group-lag).
license: MIT
tags: [kafka, topics, partitions, admin, rest-api, mcp]
metadata:
  author: Genius
  version: '0.1.0'
---
# Kafka Topic Administration

Domain-typed lifecycle and configuration control for Apache Kafka **topics** and
their **partitions** through the Confluent REST Proxy v3 (with a native
direct-to-broker fallback). Prefer these tools over raw HTTP — they carry the v3
request shapes and return topic/partition-shaped records.

## When to use
- Provision a topic with an explicit partition count + replication factor.
- Describe a topic or list all topics in the cluster.
- Read or alter topic configs (retention.ms, cleanup.policy, …).
- Enumerate a topic's partitions or fetch a single partition.

## When NOT to use
- Producing or consuming records → `kafka-streaming-io`.
- Consumer-group membership or lag → `kafka-consumer-group-lag`.
- Cluster/broker/ACL governance → the `kafka_cluster` tool directly.

## Prerequisites & environment
Connect via the `mcp-client` skill against the **`kafka-mcp`** MCP server.

| Variable | Required | Notes |
|----------|----------|-------|
| `KAFKA_REST_URL` | ✅ | Confluent REST Proxy base URL (default `http://localhost:8082`) |
| `KAFKA_CLUSTER_ID` | optional | Pin the cluster id; else auto-resolved to the first cluster |
| `KAFKA_TOKEN` | optional | Bearer token |
| `KAFKA_USERNAME` / `KAFKA_PASSWORD` | optional | Basic auth |
| `KAFKA_SSL_VERIFY` | optional | TLS verification toggle |
| `KAFKA_BOOTSTRAP_SERVERS` | optional | For the native client only (default `localhost:9092`; needs `kafka-mcp[native]`) |

`MCP_TOOL_MODE` (`condensed`|`verbose`|`both`) selects the condensed surface
(used below) vs. the one-to-one verbose tools.

## Tools & actions
Prefer the **condensed** tools; each takes `action` + a `params_json` **JSON string**.

| Condensed tool | Actions |
|----------------|---------|
| `kafka_topics` | `list`, `create`, `describe`, `delete`, `configs`, `update_config` |
| `kafka_partitions` | `list`, `get` |
| `kafka_native` | `create_topic`, `delete_topic`, `list_topics` (direct-to-broker) |

## Recipes (`params_json`)
Create a 3-partition topic with retention override:
```json
{"topic":"events","partitions_count":3,"replication_factor":1,"configs":{"retention.ms":"604800000","cleanup.policy":"delete"}}
```
Describe a topic / list its partitions:
```json
{"topic":"events"}
```
Alter topic configs (batch):
```json
{"topic":"events","configs":{"retention.ms":"120000"}}
```
Get a single partition:
```json
{"topic":"events","partition_id":0}
```

## Gotchas
- `params_json` is a **string** of JSON, not an object — serialize it.
- `kafka_topics list` best-effort mirrors the topic catalog into the knowledge
  graph on every call; this never blocks or fails the read.
- `replication_factor` must be ≤ the number of brokers, or `create` fails.
- Topic `delete` is irreversible and drops all data; confirm the name first.
- The native `kafka_native` actions require the optional `kafka-mcp[native]`
  extra (librdkafka); without it they raise a clear RuntimeError.

## Related
- `kafka-streaming-io` — produce/consume records on the topics you create here.
- `kafka-consumer-group-lag` — monitor how far consumers trail these topics.
- `kafka_ingest_catalog` — pull the whole topic/group/broker topology into the KG
  as typed `:Topic` / `:Partition` / `:KafkaCluster` nodes.
