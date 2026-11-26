---
name: kafka-streaming-io
skill_type: skill
description: >-
  Produce and consume Apache Kafka records via the kafka-mcp MCP server — publish a record to a topic (v3 /records endpoint) and drive the v2 consumer-instance lifecycle (create → subscribe → poll → commit → delete), or produce/consume directly against brokers with the native client. Use when the agent must emit an event, replay/poll a topic, or run a short consume loop.
license: MIT
tags: [kafka, records, produce, consume, streaming, mcp]
metadata:
  author: Knuckles-Team
  version: '0.1.0'
---
# Kafka Streaming I/O

Scope: Produce and consume Apache Kafka records via the kafka-mcp MCP server — publish a record to a topic (v3 /records endpoint) and drive the v2 consumer-instance lifecycle (create → subscribe → poll → commit → delete), or produce/consume directly against brokers with the native client. Use when the agent must emit an event, replay/poll a topic, or run a short consume loop. Do NOT use for creating/altering topics (use kafka-topic-administration) or reading consumer-group lag (use kafka-consumer-group-lag).

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
- ACLs / broker / cluster metadata → the `kafka_list_brokers`, `kafka_list_acls` and related cluster tools.

## Prerequisites & environment
Connect via the `mcp-client` skill against the **`kafka-mcp`** MCP server.

| Variable | Required | Notes |
|----------|----------|-------|
| `KAFKA_REST_URL` | ✅ | Confluent REST Proxy base URL (default `http://localhost:8082`) |
| `KAFKA_CLUSTER_ID` | optional | Pin the cluster id; else auto-resolved |
| `KAFKA_TOKEN` / `KAFKA_USERNAME` / `KAFKA_PASSWORD` | optional | Bearer or basic auth |
| `KAFKA_BOOTSTRAP_SERVERS` | optional | Native client only (default `localhost:9092`; needs `kafka-mcp[native]`) |


## Steps

1. Confirm the topic exists with `kafka_list_topics`. Verify the topic name appears before producing.
2. Produce with `kafka_produce_record` (`topic`, `value`, `key`, `value_format`). Verify the response carries a partition and offset.
3. Open a consumer with `kafka_create_consumer` (`group`, `name`, `format`) and `kafka_subscribe_consumer` (`topics`). Verify both calls succeed without error.
4. Poll with `kafka_consume_records` (`group`, `instance`, `timeout`) and call it again if the first poll is empty. Verify records arrive or report that the topic is empty.
5. Save progress with `kafka_commit_offsets` (`group`, `instance`). Verify the call succeeds.
6. Close the consumer with `kafka_delete_consumer` (`group`, `instance`). Verify the instance no longer responds to `kafka_consume_records`.

## Failure recovery

| Failure | Cause | Recovery |
|---------|-------|----------|
| First poll returns empty | Assignment still settling after subscribe | Call `kafka_consume_records` again with a longer `timeout` |
| Produce rejected for BINARY | Value not base64 | Encode the value as base64 or use `value_format` `JSON` |
| Native tools raise RuntimeError | `kafka-mcp[native]` extra missing | Use the REST tools such as `kafka_produce_record` or install the extra in the deployment |
| Consumer instance leaks | `kafka_delete_consumer` skipped | Call `kafka_delete_consumer` with the same `group` and `instance` |

## Tools
| Condensed tool | Actions |
|----------------|---------|
| `kafka_produce_record` | produce through the REST Proxy |
| `kafka_create_consumer`, `kafka_subscribe_consumer`, `kafka_consume_records`, `kafka_commit_offsets`, `kafka_delete_consumer` | v2 consumer instance lifecycle |
| `kafka_native_produce`, `kafka_native_consume` | direct-to-broker |

## Recipes (tool arguments)
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
