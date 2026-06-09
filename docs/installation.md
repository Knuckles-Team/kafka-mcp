# Installation

`kafka-mcp` is a standard Python package and a prebuilt container image. Pick the
path that matches how you want to run it.

## Requirements

- **Python 3.11 – 3.14**.
- A reachable **Confluent REST Proxy** (default surface) in front of Apache Kafka —
  see [Backing Platform](platform.md) to deploy Kafka locally. The optional native
  client connects directly to brokers instead.

## From PyPI (recommended)

```bash
pip install kafka-mcp
```

### Optional extras

The base install is intentionally minimal. Install the extra for what you need:

| Extra | Install | Pulls in |
|---|---|---|
| `mcp` | `pip install "kafka-mcp[mcp]"` | FastMCP MCP-server runtime (`agent-utilities[mcp]`) |
| `agent` | `pip install "kafka-mcp[agent]"` | Pydantic-AI agent + Logfire tracing |
| `native` | `pip install "kafka-mcp[native]"` | `confluent-kafka` direct-to-broker client |
| `all` | `pip install "kafka-mcp[all]"` | MCP + agent + Logfire |
| `test` | `pip install "kafka-mcp[test]"` | `pytest`, `pytest-asyncio`, `pytest-cov`, `pytest-xdist` |

```bash
# Typical: run the MCP server plus the native client
pip install "kafka-mcp[mcp,native]"
```

## From source

```bash
git clone https://github.com/Knuckles-Team/kafka-mcp.git
cd kafka-mcp
pip install -e ".[all]"          # editable install with the MCP + agent extras
```

With [`uv`](https://docs.astral.sh/uv/):

```bash
uv pip install -e ".[all]"
uv run kafka-mcp
```

## Prebuilt Docker image

A multi-stage, slim image is published on every release (entrypoint `kafka-mcp`):

```bash
docker pull knucklessg1/kafka-mcp:latest

docker run --rm -i \
  -e KAFKA_REST_URL=http://your-rest-proxy:8082 \
  knucklessg1/kafka-mcp:latest         # stdio transport (default)
```

For an HTTP server with a published port, see [Deployment](deployment.md).

## Verify the install

```bash
kafka-mcp --help
python -c "import kafka_mcp; print(kafka_mcp.__version__)"
```

## Next steps

- **[Deployment](deployment.md)** — run it as a long-lived MCP server (and agent) behind Caddy + DNS.
- **[Usage](usage.md)** — call the tools, the `KafkaApi` client, and the CLI.
- **[Configuration](deployment.md#configuration-environment)** — every environment variable.
