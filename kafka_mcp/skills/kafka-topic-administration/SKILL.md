---
name: kafka-topic-administration
skill_type: skill
description: >-
  Administer Apache Kafka topics via the kafka-mcp MCP server — list, create,
  describe, and delete topics, inspect/alter topic configs, and enumerate
  partitions, using the domain-typed kafka_list_topics / kafka_create_topic / kafka_list_partitions tools over
  the Confluent REST Proxy (or the native direct-to-broker client). Use when the
  agent must provision a topic with a partition/replication plan, tune retention
  or cleanup configs, or inspect a topic's partition layout. Do NOT use for
  producing/consuming records (use kafka-streaming-io) or consumer-group lag
  monitoring (use kafka-consumer-group-lag).
license: MIT
tags: [kafka, topics, partitions, admin, rest-api, mcp]
metadata:
  author: Knuckles-Team
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
- Cluster/broker/ACL governance → the `kafka_list_brokers`, `kafka_list_acls` and related cluster tools.

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

## Tools
One typed tool per operation; pass the arguments by name.

| Condensed tool | Actions |
|----------------|---------|
| `kafka_list_topics`, `kafka_create_topic`, `kafka_get_topic`, `kafka_delete_topic` | list, create, describe, delete |
| `kafka_list_topic_configs`, `kafka_update_topic_configs`, `kafka_update_topic_config` | read and alter configs |
| `kafka_list_partitions`, `kafka_get_partition` | partition layout |
| `kafka_native_create_topic`, `kafka_native_delete_topic`, `kafka_native_list_topics`, `kafka_native_list_groups` | direct-to-broker |

## Recipes (tool arguments)
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
- Arguments are typed; pass objects as objects.
- `kafka_list_topics` best-effort mirrors the topic catalog into the knowledge
  graph on every call; this never blocks or fails the read.
- `replication_factor` must be ≤ the number of brokers, or `create` fails.
- Topic `delete` is irreversible and drops all data; confirm the name first.
- The native `kafka_native_*` tools require the optional `kafka-mcp[native]`
  extra (librdkafka); without it they raise a clear RuntimeError.

## Related
- `kafka-streaming-io` — produce/consume records on the topics you create here.
- `kafka-consumer-group-lag` — monitor how far consumers trail these topics.
- `kafka_ingest_catalog` — pull the whole topic/group/broker topology into the KG
  as typed `:Topic` / `:Partition` / `:KafkaCluster` nodes.
