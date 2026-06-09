# kafka-mcp

Apache Kafka **API + MCP Server + A2A Agent** for the agent-utilities ecosystem —
topics, records, consumer groups, brokers, and ACLs over the Confluent REST Proxy,
with an optional native (direct-to-broker) client.

!!! info "Official documentation"
    This site is the canonical reference for `kafka-mcp`, maintained alongside every
    release.

[![PyPI](https://img.shields.io/pypi/v/kafka-mcp)](https://pypi.org/project/kafka-mcp/)
![MCP Server](https://badge.mcpx.dev?type=server 'MCP Server')
[![License](https://img.shields.io/pypi/l/kafka-mcp)](https://github.com/Knuckles-Team/kafka-mcp/blob/main/LICENSE)
[![GitHub](https://img.shields.io/badge/source-GitHub-181717?logo=github)](https://github.com/Knuckles-Team/kafka-mcp)

## Overview

`kafka-mcp` wraps the Apache Kafka administration and data-plane surface with typed,
deterministic MCP tools, and ships an optional Pydantic-AI agent server. It provides:

- **`KafkaApi`** — a `requests`-based REST facade over the Confluent REST Proxy v3
  (with v2 consumer helpers), organized by Kafka resource: clusters, topics,
  partitions, records, consumer groups, brokers, and ACLs.
- **Six MCP tools** — action-dispatch wrappers (`kafka_topics`, `kafka_partitions`,
  `kafka_records`, `kafka_groups`, `kafka_cluster`, `kafka_native`) that expose the
  full surface to an agent or policy router.
- **An optional native client** (`kafka-mcp[native]`) that produces, consumes, and
  administers topics directly against brokers via `confluent-kafka`.

The active cluster id is resolved lazily — set `KAFKA_CLUSTER_ID` to pin it, or let
the client cache the first cluster the REST Proxy returns.

## Explore the documentation

<div class="grid cards" markdown>

- :material-rocket-launch: **[Installation](installation.md)** — pip, source, extras, and the prebuilt Docker image.
- :material-server-network: **[Deployment](deployment.md)** — run the MCP and agent servers, Docker Compose, Caddy + Technitium.
- :material-console: **[Usage](usage.md)** — the MCP tools, the `KafkaApi` client, and the CLI.
- :material-database-cog: **[Backing Platform](platform.md)** — deploy Apache Kafka with Docker.
- :material-sitemap: **[Architecture](architecture.md)** — the layered REST client and tool surface.
- :material-tag-multiple: **[Concepts](concepts.md)** — the `CONCEPT:KAFKA-*` registry.

</div>

## Quick start

```bash
pip install "kafka-mcp[mcp]"
kafka-mcp                        # stdio MCP server (default transport)
```

Connect it to a Confluent REST Proxy in front of your Kafka cluster:

```bash
export KAFKA_REST_URL=http://your-rest-proxy:8082
kafka-mcp --transport streamable-http --host 0.0.0.0 --port 8000
```

See **[Installation](installation.md)** and **[Deployment](deployment.md)** for the
full matrix (PyPI extras, Docker image, all transports, the agent server, reverse
proxy, DNS).
