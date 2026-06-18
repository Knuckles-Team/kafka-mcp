# Kafka Mcp
## API | MCP Server | A2A Agent

![PyPI - Version](https://img.shields.io/pypi/v/kafka-mcp)
![MCP Server](https://badge.mcpx.dev?type=server 'MCP Server')
![PyPI - Downloads](https://img.shields.io/pypi/dd/kafka-mcp)
![GitHub Repo stars](https://img.shields.io/github/stars/Knuckles-Team/kafka-mcp)
![GitHub forks](https://img.shields.io/github/forks/Knuckles-Team/kafka-mcp)
![GitHub contributors](https://img.shields.io/github/contributors/Knuckles-Team/kafka-mcp)
![PyPI - License](https://img.shields.io/pypi/l/kafka-mcp)
![GitHub](https://img.shields.io/github/license/Knuckles-Team/kafka-mcp)
![GitHub last commit (by committer)](https://img.shields.io/github/last-commit/Knuckles-Team/kafka-mcp)
![GitHub pull requests](https://img.shields.io/github/issues-pr/Knuckles-Team/kafka-mcp)
![GitHub closed pull requests](https://img.shields.io/github/issues-pr-closed/Knuckles-Team/kafka-mcp)
![GitHub issues](https://img.shields.io/github/issues/Knuckles-Team/kafka-mcp)
![GitHub top language](https://img.shields.io/github/languages/top/Knuckles-Team/kafka-mcp)
![GitHub language count](https://img.shields.io/github/languages/count/Knuckles-Team/kafka-mcp)
![GitHub repo size](https://img.shields.io/github/repo-size/Knuckles-Team/kafka-mcp)
![PyPI - Wheel](https://img.shields.io/pypi/wheel/kafka-mcp)
![PyPI - Implementation](https://img.shields.io/pypi/implementation/kafka-mcp)

Apache Kafka **API + MCP Server + A2A Agent** for the agent-utilities ecosystem.

*Version: 0.5.0*

> **Documentation** — Installation, deployment, usage across the API, CLI, and MCP
> interfaces, and guidance for provisioning the Apache Kafka platform are maintained
> in the [official documentation](https://knuckles-team.github.io/kafka-mcp/).

`kafka-mcp` wraps the Apache Kafka administration and data-plane surface with typed,
deterministic tools an agent or policy router calls. It speaks to the Confluent REST
Proxy by default and ships an optional native (direct-to-broker) client and a
Pydantic-AI agent server.

## What it provides

- **`KafkaApi`** (`kafka_mcp.api.api_client_kafka`) — a `requests`-based REST facade
  over the Confluent REST Proxy v3 (with v2 consumer helpers), organized by Kafka
  resource: clusters, topics, partitions, records, consumer groups, brokers, and
  ACLs. The active cluster id resolves lazily unless `KAFKA_CLUSTER_ID` pins it.
- **Six MCP tools** (`kafka-mcp` console script): `kafka_topics`,
  `kafka_partitions`, `kafka_records`, `kafka_groups`, `kafka_cluster`, and the
  optional native `kafka_native`. See
  [`docs/overview.md`](docs/overview.md) for the full action surface.
- **An A2A agent server** (`kafka-agent` console script) — a Pydantic-AI graph agent
  wired to the MCP server via `MCP_URL`.

## Configuration (environment)

| Var | Default | Meaning |
|---|---|---|
| `KAFKA_REST_URL` | `http://localhost:8082` | Confluent REST Proxy base URL |
| `KAFKA_CLUSTER_ID` | _(auto)_ | Pin the cluster id (else the first cluster is cached) |
| `KAFKA_TOKEN` | _(empty)_ | Bearer token for the REST Proxy |
| `KAFKA_USERNAME` | _(empty)_ | Basic-auth user (optional) |
| `KAFKA_PASSWORD` | _(empty)_ | Basic-auth password (optional) |
| `KAFKA_SSL_VERIFY` | `True` | Verify TLS (set `False` for self-signed homelab) |
| `KAFKATOOL` | `True` | Register the Kafka tool set |

The optional native client reads `KAFKA_BOOTSTRAP_SERVERS` (default
`localhost:9092`) and requires the `kafka-mcp[native]` extra. Copy
[`.env.example`](.env.example) to `.env` and populate only what you use; blank
connector credentials leave the corresponding surface inactive.

## Install & run

```bash
pip install -e .
kafka-mcp                        # stdio MCP server (default transport)
kafka-mcp --transport streamable-http --host 0.0.0.0 --port 8000
```

Run the agent server against a live MCP server:

```bash
kafka-agent --mcp-url http://localhost:8000/mcp --host 0.0.0.0 --port 8080
```

## MCP config

Register in your client's `mcp_config.json` (tools surface as `kafka_topics`,
`kafka_records`, `kafka_groups`, …). See `kafka_mcp/mcp_config.json`.

<!-- BEGIN GENERATED: additional-deployment-options -->
### Additional Deployment Options

`kafka-mcp` can also run as a **local container** (Docker / Podman / `uv`) or be
consumed from a **remote deployment**. The
[Deployment guide](https://knuckles-team.github.io/kafka-mcp/deployment/) has full, copy-paste
`mcp_config.json` for all four transports — **stdio**, **streamable-http**,
**local container / uv**, and **remote URL**:

- **Local container / uv** — launch the server from `mcp_config.json` via `uvx`,
  `docker run`, or `podman run`, or point at a local streamable-http container by `url`.
- **Remote URL** — connect to a server deployed behind Caddy at
  `http://kafka-mcp.arpa/mcp` using the `"url"` key.
<!-- END GENERATED: additional-deployment-options -->

## Documentation

The complete documentation is published as the
[official documentation site](https://knuckles-team.github.io/kafka-mcp/) and is the
recommended reference for installation, deployment, and day-to-day operation.

| Page | Contents |
|---|---|
| [Installation](https://knuckles-team.github.io/kafka-mcp/installation/) | pip, source, extras, prebuilt Docker image |
| [Deployment](https://knuckles-team.github.io/kafka-mcp/deployment/) | run the MCP and agent servers, Compose, Caddy + Technitium, env config |
| [Usage](https://knuckles-team.github.io/kafka-mcp/usage/) | the MCP tools, the `KafkaApi` client, the CLI |
| [Backing Platform](https://knuckles-team.github.io/kafka-mcp/platform/) | deploy Apache Kafka with Docker |
| [Overview](https://knuckles-team.github.io/kafka-mcp/overview/) | tools, REST contract, native client |
| [Architecture](https://knuckles-team.github.io/kafka-mcp/architecture/) | the layered REST client and tool surface |
| [Concepts](https://knuckles-team.github.io/kafka-mcp/concepts/) | concept registry (`CONCEPT:KAFKA-*`) |

`AGENTS.md` is the canonical contributor/agent guidance.


<!-- BEGIN agent-os-genesis-deploy (generated; do not edit between markers) -->

## Deploy with `agent-os-genesis`

This package can be provisioned for you — skill-guided — by the **`agent-os-genesis`**
universal skill (its *single-package deploy mode*): it picks your install method, seeds
secrets to OpenBao/Vault (or `.env`), trusts your enterprise CA, registers the MCP
server, and verifies it — the same machinery that stands up the whole Agent OS, narrowed
to just this package. Ask your agent to **"deploy `kafka-mcp` with agent-os-genesis"**.

| Install mode | Command |
|------|---------|
| Bare-metal, prod (PyPI) | `uvx kafka-mcp` · or `uv tool install kafka-mcp` |
| Bare-metal, dev (editable) | `uv pip install -e ".[all]"` · or `pip install -e ".[all]"` |
| Container, prod | deploy `knucklessg1/kafka-mcp:latest` via docker-compose / swarm / podman / podman-compose / kubernetes |
| Container, dev (editable) | deploy `docker/compose.dev.yml` (source-mounted at `/src`; edits live on restart) |

Secrets are read-existing + seeded via `vault_sync` — you are only prompted for what's missing.

<!-- END agent-os-genesis-deploy -->
