# Overview

`kafka-mcp` is the **API + MCP server + A2A agent** for Apache Kafka in the
agent-utilities ecosystem. It wraps the Confluent REST Proxy (and, optionally, the
native broker protocol) in typed, deterministic tools that an agent or policy router
can call.

## The tool surface

Six action-dispatch MCP tools cover the full Kafka surface. Each accepts an `action`
plus a JSON `params_json` payload:

| Tool | Resource | Representative actions |
|---|---|---|
| `kafka_topics` | Topics & configs | `list`, `create`, `describe`, `delete`, `configs`, `update_config` |
| `kafka_partitions` | Partitions | `list`, `get` |
| `kafka_records` | Produce / consume | `produce`, `create_consumer`, `subscribe`, `consume`, `commit`, `delete_consumer` |
| `kafka_groups` | Consumer groups | `list`, `describe`, `consumers`, `lag_summary`, `lags` |
| `kafka_cluster` | Clusters, brokers, ACLs | `list_clusters`, `get_cluster`, `list_brokers`, `get_broker`, `broker_configs`, `list_acls`, `create_acl`, `delete_acls` |
| `kafka_native` | Direct-to-broker | `produce`, `consume`, `create_topic`, `delete_topic`, `list_topics`, `list_groups` |

## The REST contract

`KafkaApi` targets the **Confluent REST Proxy v3** for administration and v3 produce,
with **v2** helpers for the consumer-instance lifecycle:

- **Clusters / topics / partitions / brokers / ACLs** — `GET|POST|DELETE
  v3/clusters/{cluster}/…`.
- **Produce** — `POST v3/clusters/{cluster}/topics/{topic}/records` with a typed
  `value`/`key` (`STRING`, `JSON`, `BINARY`, `AVRO`, `JSONSCHEMA`, `PROTOBUF`).
- **Consume** — the v2 instance lifecycle: `create_consumer` → `subscribe` →
  `consume` (poll) → `commit` → `delete_consumer`.

The active cluster id is resolved lazily and cached — honor `KAFKA_CLUSTER_ID` to
pin it, otherwise the first cluster returned by `list_clusters` is used.

## Where to go next

- **[Installation](installation.md)** — pip, extras, Docker image.
- **[Deployment](deployment.md)** — transports, Compose, the agent server, Caddy, DNS.
- **[Usage](usage.md)** — the tools, the `KafkaApi` client, the CLI.
- **[Architecture](architecture.md)** — the layered client and tool surface.
