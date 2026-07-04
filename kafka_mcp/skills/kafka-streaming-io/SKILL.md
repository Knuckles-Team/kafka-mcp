---
name: kafka-streaming-io
description: >-
  Produce and consume Apache Kafka records via the kafka-mcp MCP server — publish
  a record to a topic (v3 /records endpoint) and drive the v2 consumer-instance
  lifecycle (create → subscribe → poll → commit → delete), or produce/consume
  directly against brokers with the native client. Use when the agent must emit
  an event, replay/poll a topic, or run a short consume loop. Do NOT use for
  creating/altering topics (use kafka-topic-administration) or reading
  consumer-group lag (use kafka-consumer-group-lag).
license: MIT
tags: [kafka, records, produce, consume, streaming, mcp]
metadata:
  author: Genius
  version: '0.1.0'
---
# Kafka Streaming I/O

Domain-typed **produce** and **consume** over Apache Kafka through the Confluent
REST Proxy (v3 produce + v2 consumer instances), with a native direct-to-broker
fallback. Prefer these tools over raw HTTP — they carry the correct content-type
/ accept headers and the record envelope shapes.

## When to use
- Produce a single record (JSON/STRING/BINARY/AVRO/…) to a topic.
- Poll a topic through a managed consumer instance (create/subscribe/consume/commit).
- Run a bounded native consume loop (max_messages + timeout).

## When NOT to use
- Topic/partition provisioning or config → `kafka-topic-administration`.
- Consumer-group lag / membership inspection → `kafka-consumer-group-lag`.
- ACLs / broker / cluster metadata → the `kafka_cluster` tool directly.

## Prerequisites & environment
Connect via the `mcp-client` skill against the **`kafka-mcp`** MCP server.

| Variable | Required | Notes |
|----------|----------|-------|
| `KAFKA_REST_URL` | ✅ | Confluent REST Proxy base URL (default `http://localhost:8082`) |
| `KAFKA_CLUSTER_ID` | optional | Pin the cluster id; else auto-resolved |
| `KAFKA_TOKEN` / `KAFKA_USERNAME` / `KAFKA_PASSWORD` | optional | Bearer or basic auth |
| `KAFKA_BOOTSTRAP_SERVERS` | optional | Native client only (default `localhost:9092`; needs `kafka-mcp[native]`) |

`MCP_TOOL_MODE` selects condensed vs. verbose surfaces.

## Tools & actions
| Condensed tool | Actions |
|----------------|---------|
| `kafka_records` | `produce`, `create_consumer`, `subscribe`, `consume`, `commit`, `delete_consumer` |
| `kafka_native` | `produce`, `consume` (direct-to-broker) |

## Recipes (`params_json`)
Produce a JSON record with a key:
```json
{"topic":"events","value":{"orderId":42,"status":"placed"},"key":"42","value_format":"JSON"}
```
Full consume cycle — create, subscribe, poll, commit, delete:
```json
{"group":"analytics","name":"c1","format":"json","auto_offset_reset":"earliest"}
```
```json
{"group":"analytics","instance":"c1","topics":["events"]}
```
```json
{"group":"analytics","instance":"c1","timeout":1000}
```
```json
{"group":"analytics","instance":"c1"}
```
Native bounded consume:
```json
{"topic":"events","group":"analytics","max_messages":10,"timeout":5}
```

## Gotchas
- `params_json` is a **string** of JSON, not an object.
- The v2 consumer path is **stateful**: you must `create_consumer` first, then
  `subscribe`, and the **first** `consume` after subscribe often returns empty
  while the assignment settles — poll again.
- Always `delete_consumer` when done, or the instance leaks on the proxy.
- `value_format`/`key_format` are one of `STRING`, `JSON`, `BINARY`, `AVRO`,
  `JSONSCHEMA`, `PROTOBUF`; `BINARY` values must be base64.
- Native `produce`/`consume` require the `kafka-mcp[native]` extra.

## Related
- `kafka-topic-administration` — create the topic before you produce to it.
- `kafka-consumer-group-lag` — see how far the consuming group trails the log.
