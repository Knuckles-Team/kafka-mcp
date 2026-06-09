# Concept Registry — kafka-mcp

> **Prefix**: `CONCEPT:KAFKA-*`

Stable concept IDs trace the connector's core ideas across the documentation, code
docstrings, and tests.

| Concept ID | Name | Description |
|---|---|---|
| `CONCEPT:KAFKA-001` | Kafka MCP Domain | The Kafka tool domain registered by `register_kafka_tools()` |
| `CONCEPT:KAFKA-002` | REST Proxy Facade | `KafkaApi` — a `requests`-based Confluent REST Proxy v3/v2 client |
| `CONCEPT:KAFKA-003` | Action Dispatch Tools | Six `kafka_*` MCP tools that route an `action` + `params_json` to the API |
| `CONCEPT:KAFKA-004` | Lazy Cluster Resolution | `cluster_id()` caches the first cluster unless `KAFKA_CLUSTER_ID` pins it |
| `CONCEPT:KAFKA-005` | Native Broker Client | Optional `NativeKafkaClient` (`kafka-mcp[native]`) for direct produce/consume/admin |
| `CONCEPT:KAFKA-006` | A2A Agent Server | The Pydantic-AI agent server (`kafka-agent`) wired to the MCP server via `MCP_URL` |
