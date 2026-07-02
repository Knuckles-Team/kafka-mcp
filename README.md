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

## Environment Variables

<!-- ENV-VARS-TABLE:START -->

#### Package environment variables

| Variable | Example | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |  |
| `KAFKA_REST_URL` | `http://localhost:8082` | Confluent REST Proxy base URL |
| `KAFKA_CLUSTER_ID` | — | Pin the cluster id (else the first cluster is cached) |
| `KAFKA_TOKEN` | — | Bearer token for the REST Proxy |
| `KAFKA_USERNAME` | — | Basic-auth user (optional) |
| `KAFKA_PASSWORD` | — | Basic-auth password (optional) |
| `KAFKA_SSL_VERIFY` | `True` | Verify TLS (set False for self-signed homelab) |
| `KAFKATOOL` | `True` |  |

#### Inherited agent-utilities variables (apply to every connector)

| Variable | Example | Description |
|----------|---------|-------------|
| `TRANSPORT` | `stdio` | MCP transport: `stdio` | `streamable-http` | `sse` |
| `HOST` | `0.0.0.0` | Bind host (HTTP transports) |
| `PORT` | `8000` | Bind port (HTTP transports) |
| `MCP_TOOL_MODE` | `condensed` | Tool surface: `condensed` | `verbose` | `both` |
| `MCP_ENABLED_TOOLS` | — | Comma-separated tool allow-list |
| `MCP_DISABLED_TOOLS` | — | Comma-separated tool deny-list |
| `MCP_ENABLED_TAGS` | — | Comma-separated tag allow-list |
| `MCP_DISABLED_TAGS` | — | Comma-separated tag deny-list |
| `EUNOMIA_TYPE` | `none` | Authorization mode: `none` | `embedded` | `remote` |
| `EUNOMIA_POLICY_FILE` | `mcp_policies.json` | Embedded Eunomia policy file |
| `EUNOMIA_REMOTE_URL` | — | Remote Eunomia authorization server URL |
| `ENABLE_OTEL` | `False` | Enable OpenTelemetry export |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | OTLP collector endpoint |
| `MCP_CLIENT_AUTH` | — | Outbound MCP auth (`oidc-client-credentials` for fleet calls) |
| `OIDC_CLIENT_ID` | — | OIDC client id (service-account auth) |
| `OIDC_CLIENT_SECRET` | — | OIDC client secret (service-account auth) |
| `DEBUG` | `False` | Verbose logging |
| `PYTHONUNBUFFERED` | `1` | Unbuffered stdout (recommended in containers) |
| `MCP_URL` | `http://localhost:8000/mcp` | URL of the MCP server the agent connects to |
| `PROVIDER` | `openai` | LLM provider for the agent |
| `MODEL_ID` | `gpt-4o` | Model id for the agent |
| `ENABLE_WEB_UI` | `True` | Serve the AG-UI web interface |

_8 package + 22 inherited variable(s). Auto-generated from `.env.example` + the shared agent-utilities set — do not edit._
<!-- ENV-VARS-TABLE:END -->


### Connection & credentials (Confluent REST Proxy)
| Var | Default | Meaning |
|---|---|---|
| `KAFKA_REST_URL` | `http://localhost:8082` | Confluent REST Proxy base URL |
| `KAFKA_CLUSTER_ID` | _(auto)_ | Pin the cluster id (else the first cluster is cached) |
| `KAFKA_TOKEN` | _(empty)_ | Bearer token for the REST Proxy |
| `KAFKA_USERNAME` | _(empty)_ | Basic-auth user (optional) |
| `KAFKA_PASSWORD` | _(empty)_ | Basic-auth password (optional) |
| `KAFKA_SSL_VERIFY` | `True` | Verify TLS (set `False` for self-signed homelab) |

### Native client (optional `kafka-mcp[native]` extra)
| Var | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker bootstrap servers for the native (direct-to-broker) client |

### MCP server / transport
| Var | Default | Meaning |
|---|---|---|
| `TRANSPORT` | `stdio` | `stdio`, `streamable-http`, or `sse` |
| `HOST` | `0.0.0.0` | Bind host (HTTP transports) |
| `PORT` | `8000` | Bind port (HTTP transports) |
| `MCP_TOOL_MODE` | `condensed` | Tool surface: `condensed`, `verbose`, or `both` |

### Tool toggles
| Var | Default | Meaning |
|---|---|---|
| `KAFKATOOL` | `True` | Register the Kafka tool set (set `False` to disable) |

Copy [`.env.example`](.env.example) to `.env` and populate only what you use; blank
connector credentials leave the corresponding surface inactive.

## Installation

Pick the extra that matches what you want to run:

| Extra | Installs | Use when |
|-------|----------|----------|
| `kafka-mcp[mcp]` | Slim MCP server only (`agent-utilities[mcp]` — FastMCP/FastAPI) | You only run the **MCP server** (smallest install / image) |
| `kafka-mcp[agent]` | Full agent runtime (`agent-utilities[agent,logfire]` — Pydantic AI + the epistemic-graph engine) | You run the **integrated agent** |
| `kafka-mcp[all]` | Everything (`mcp` + `agent` + `logfire`) | Development / both surfaces |

```bash
# MCP server only (recommended for tool hosting — slim deps)
uv pip install "kafka-mcp[mcp]"

# Full agent runtime (Pydantic AI + epistemic-graph engine)
uv pip install "kafka-mcp[agent]"

# Everything (development)
uv pip install "kafka-mcp[all]"      # or: python -m pip install "kafka-mcp[all]"
```

```bash
kafka-mcp                        # stdio MCP server (default transport)
kafka-mcp --transport streamable-http --host 0.0.0.0 --port 8000
```

Run the agent server against a live MCP server:

```bash
kafka-agent --mcp-url http://localhost:8000/mcp --host 0.0.0.0 --port 8080
```

### Container images (`:mcp` vs `:agent`)

One multi-stage `docker/Dockerfile` builds two right-sized images, selected by `--target`:

| Image tag | Build target | Contents | Entrypoint |
|-----------|--------------|----------|------------|
| `knucklessg1/kafka-mcp:mcp` | `--target mcp` | `kafka-mcp[mcp]` — **slim**, no engine/`pydantic-ai`/`dspy`/`llama-index`/`tree-sitter` | `kafka-mcp` |
| `knucklessg1/kafka-mcp:latest` | `--target agent` (default) | `kafka-mcp[agent]` — **full** agent runtime + epistemic-graph engine | `kafka-agent` |

```bash
docker build --target mcp   -t knucklessg1/kafka-mcp:mcp    docker/   # slim MCP server
docker build --target agent -t knucklessg1/kafka-mcp:latest docker/   # full agent
```

### Knowledge-graph database (`epistemic-graph`)

The **full agent** (`[agent]` / `:latest`) embeds the **epistemic-graph** engine (pulled in
transitively via `agent-utilities[agent]`). For production — or to share one knowledge graph
across multiple agents — run **epistemic-graph as its own database container** and point the
agent at it instead of embedding it. Deployment recipes (single-node + Raft HA), connection
config, and the full database architecture (with diagrams) are documented in the
[epistemic-graph deployment guide](https://knuckles-team.github.io/epistemic-graph/deployment/).
The slim `[mcp]` server does **not** require the database.

### MCP Configuration Examples

<!-- MCP-CONFIG-EXAMPLES:START -->

> **Install the slim `[mcp]` extra.** All examples install `kafka-mcp[mcp]` — the
> MCP-server extra that pulls only the FastMCP / FastAPI tooling (`agent-utilities[mcp]`).
> It deliberately **excludes** the heavy agent runtime (`pydantic-ai`, the epistemic-graph
> engine, `dspy`, `llama-index`), so `uvx` / container installs are far smaller. Use the
> full `[agent]` extra only when you need the integrated Pydantic AI agent.

#### stdio Transport (local IDEs — Cursor, Claude Desktop, VS Code)

```json
{
  "mcpServers": {
    "kafka-mcp": {
      "command": "uvx",
      "args": [
        "--from",
        "kafka-mcp[mcp]",
        "kafka-mcp"
      ],
      "env": {
        "MCP_TOOL_MODE": "condensed",
        "KAFKATOOL": "True",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "KAFKA_CLUSTER_ID": "",
        "KAFKA_PASSWORD": "",
        "KAFKA_REST_URL": "http://localhost:8082",
        "KAFKA_TOKEN": "",
        "KAFKA_USERNAME": ""
      }
    }
  }
}
```

#### Streamable-HTTP Transport (networked / production)

```json
{
  "mcpServers": {
    "kafka-mcp": {
      "command": "uvx",
      "args": [
        "--from",
        "kafka-mcp[mcp]",
        "kafka-mcp",
        "--transport",
        "streamable-http",
        "--port",
        "8000"
      ],
      "env": {
        "TRANSPORT": "streamable-http",
        "HOST": "0.0.0.0",
        "PORT": "8000",
        "MCP_TOOL_MODE": "condensed",
        "KAFKATOOL": "True",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
        "KAFKA_CLUSTER_ID": "",
        "KAFKA_PASSWORD": "",
        "KAFKA_REST_URL": "http://localhost:8082",
        "KAFKA_TOKEN": "",
        "KAFKA_USERNAME": ""
      }
    }
  }
}
```

Alternatively, connect to a pre-deployed Streamable-HTTP instance by `url`:

```json
{
  "mcpServers": {
    "kafka-mcp": {
      "url": "http://localhost:8000/kafka-mcp/mcp"
    }
  }
}
```

Deploying the Streamable-HTTP server via Docker:

```bash
docker run -d \
  --name kafka-mcp-mcp \
  -p 8000:8000 \
  -e TRANSPORT=streamable-http \
  -e HOST=0.0.0.0 \
  -e PORT=8000 \
  -e MCP_TOOL_MODE=condensed \
  -e KAFKATOOL=True \
  -e KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  -e KAFKA_CLUSTER_ID="" \
  -e KAFKA_PASSWORD="" \
  -e KAFKA_REST_URL=http://localhost:8082 \
  -e KAFKA_TOKEN="" \
  -e KAFKA_USERNAME="" \
  knucklessg1/kafka-mcp:mcp
```

_Auto-generated from the code-read env surface (`MCP_TOOL_MODE` + package vars) — do not edit._
<!-- MCP-CONFIG-EXAMPLES:END -->

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

## Available MCP Tools

Auto-generated — do not edit between the markers below.

<!-- MCP-TOOLS-TABLE:START -->

#### Condensed action-routed tools (default — `MCP_TOOL_MODE=condensed`)

| MCP Tool | Toggle Env Var | Description |
|----------|----------------|-------------|
| `kafka_cluster` | `KAFKATOOL` | Inspect clusters/brokers and manage ACLs via the REST Proxy. |
| `kafka_groups` | `KAFKATOOL` | Inspect Kafka consumer groups and their lag. |
| `kafka_native` | `KAFKATOOL` | Produce/consume/admin directly against brokers (native client). |
| `kafka_partitions` | `KAFKATOOL` | List partitions for a topic or get a single partition. |
| `kafka_records` | `KAFKATOOL` | Produce or consume records via the Confluent REST Proxy. |
| `kafka_topics` | `KAFKATOOL` | Manage Kafka topics via the Confluent REST Proxy. |

#### Verbose 1:1 API-mapped tools (`MCP_TOOL_MODE=verbose` or `both`)

<details>
<summary>29 per-operation tools — one per public API method (click to expand)</summary>

| MCP Tool | Toggle Env Var | Description |
|----------|----------------|-------------|
| `kafka_cluster_id` | `KAFKA_APITOOL` | Return the active Kafka cluster id, resolving + caching the first. |
| `kafka_commit_offsets` | `KAFKA_APITOOL` | Commit current offsets for a v2 consumer instance. |
| `kafka_consume_records` | `KAFKA_APITOOL` | Poll records from a v2 consumer instance. |
| `kafka_create_acl` | `KAFKA_APITOOL` | Create an ACL binding. |
| `kafka_create_consumer` | `KAFKA_APITOOL` | Create a v2 consumer instance inside a consumer group. |
| `kafka_create_topic` | `KAFKA_APITOOL` | Create a topic with partitions, replication, and optional configs. |
| `kafka_delete_acls` | `KAFKA_APITOOL` | Delete ACLs matching the given filter. |
| `kafka_delete_consumer` | `KAFKA_APITOOL` | Delete a v2 consumer instance. |
| `kafka_delete_topic` | `KAFKA_APITOOL` | Delete a topic. |
| `kafka_get_broker` | `KAFKA_APITOOL` | Get a single broker's metadata. |
| `kafka_get_cluster` | `KAFKA_APITOOL` | Get a single cluster's metadata. |
| `kafka_get_consumer_group` | `KAFKA_APITOOL` | Describe a single consumer group. |
| `kafka_get_group_lag_summary` | `KAFKA_APITOOL` | Get the lag summary for a consumer group. |
| `kafka_get_group_lags` | `KAFKA_APITOOL` | List per-partition lags for a consumer group. |
| `kafka_get_partition` | `KAFKA_APITOOL` | Get a single partition of a topic. |
| `kafka_get_topic` | `KAFKA_APITOOL` | Describe a single topic. |
| `kafka_list_acls` | `KAFKA_APITOOL` | Search ACLs (all filters optional). |
| `kafka_list_broker_configs` | `KAFKA_APITOOL` | List configuration entries for a broker. |
| `kafka_list_brokers` | `KAFKA_APITOOL` | List brokers in a cluster. |
| `kafka_list_clusters` | `KAFKA_APITOOL` | List Kafka clusters known to the REST Proxy. |
| `kafka_list_consumer_groups` | `KAFKA_APITOOL` | List consumer groups in a cluster. |
| `kafka_list_consumers` | `KAFKA_APITOOL` | List the consumers (members) of a consumer group. |
| `kafka_list_partitions` | `KAFKA_APITOOL` | List partitions for a topic. |
| `kafka_list_topic_configs` | `KAFKA_APITOOL` | List configuration entries for a topic. |
| `kafka_list_topics` | `KAFKA_APITOOL` | List topics in a cluster. |
| `kafka_produce_record` | `KAFKA_APITOOL` | Produce a single record to a topic via the v3 ``/records`` endpoint. |
| `kafka_subscribe_consumer` | `KAFKA_APITOOL` | Subscribe a v2 consumer instance to a list of topics. |
| `kafka_update_topic_config` | `KAFKA_APITOOL` | Update (alter) a single topic configuration entry. |
| `kafka_update_topic_configs` | `KAFKA_APITOOL` | Batch-alter multiple topic configuration entries. |

</details>

_6 action-routed tool(s) (default) · 29 verbose 1:1 tool(s). Each is enabled unless its `<DOMAIN>TOOL` toggle is set false; `MCP_TOOL_MODE` selects the surface (`condensed` default · `verbose` 1:1 · `both`). Auto-generated — do not edit._
<!-- MCP-TOOLS-TABLE:END -->

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
