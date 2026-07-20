# Deployment

<!-- BEGIN GENERATED: deployment-options -->
## Deployment Options

`kafka-mcp` supports local stdio, a loopback-only development listener, a
least-privilege stdio container, and a remote authenticated HTTPS boundary.
Provider endpoint, credential, selector, identity, and trust material are supplied
at runtime through `AgentConfig`; none is stored in this repository.

### Installed stdio process

```json
{
  "mcpServers": {
    "kafka": {
      "command": "kafka-mcp",
      "args": [],
      "env": {"MCP_TOOL_MODE": "intent"}
    }
  }
}
```

### Loopback development listener

```bash
kafka-mcp --transport streamable-http --host 127.0.0.1 --port 8000
```

Do not expose this listener beyond loopback. Network deployments require direct TLS
or an explicitly trusted TLS-terminating ingress, configured authentication, exact
`MCP_ALLOWED_HOSTS`, and an exact trusted-proxy CIDR policy.

### Least-privilege local container

```bash
docker run -i --rm \
  --read-only \
  --cap-drop=ALL \
  --security-opt=no-new-privileges \
  --pids-limit=256 \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=64m \
  -e TRANSPORT=stdio \
  registry.example.invalid/kafka-mcp@sha256:<digest> kafka-mcp
```

The operator projects the selected AgentConfig profile into the process at runtime;
the image remains immutable and contains no environment connection profile.

### Remote authenticated HTTPS endpoint

```json
{
  "mcpServers": {
    "kafka": {"url": "https://service.example.invalid/mcp"}
  }
}
```

Store the real remote URL, outbound identity reference, and TLS-profile reference in
`AgentConfig`, not in MCP client JSON or documentation.
<!-- END GENERATED: deployment-options -->

This page covers running `kafka-mcp` as a long-lived server: the transports, a
Docker Compose stack, the optional A2A agent server, putting it behind a Caddy
reverse proxy, and giving it a DNS name with Technitium. To provision the **Apache
Kafka** cluster it connects to, see [Backing Platform](platform.md).

> `kafka-mcp` ships both an **MCP server** (console script `kafka-mcp`) and an
> **A2A agent server** (console script `kafka-agent`). The MCP server is the typed,
> deterministic tool surface a policy router calls; the agent server wraps it with a
> Pydantic-AI graph for conversational workflows.

## Run the MCP server

The transport is selected with `--transport` (or the `TRANSPORT` env var):

=== "stdio (default)"

    ```bash
    kafka-mcp
    ```
    For IDE / desktop MCP clients that launch the server as a subprocess.

=== "streamable-http"

    ```bash
    kafka-mcp --transport streamable-http --host 0.0.0.0 --port 8000
    ```
    A network server with a `/health` endpoint and `/mcp` route.

=== "sse"

    ```bash
    kafka-mcp --transport sse --host 0.0.0.0 --port 8000
    ```

Health check (HTTP transports):

```bash
curl -s http://localhost:8000/health        # {"status":"OK"}
```

## Configuration (environment)

`kafka-mcp` is configured entirely from the environment. The **required** set for
the Confluent REST Proxy surface:

| Var | Default | Meaning |
|---|---|---|
| `KAFKA_REST_URL` | `http://localhost:8082` | Confluent REST Proxy base URL |
| `KAFKA_CLUSTER_ID` | _(auto)_ | Pin the cluster id (else the first cluster is cached) |
| `KAFKA_TOKEN` | _(empty)_ | Bearer token for the REST Proxy |
| `KAFKA_USERNAME` | _(empty)_ | Basic-auth user (optional) |
| `KAFKA_PASSWORD` | _(empty)_ | Basic-auth password (optional) |
| `KAFKA_REST_TLS_PROFILE` | _(system trust)_ | Optional named profile from the agent-utilities TLS catalog |
| `KAFKA_REST_TLS_PROFILE_REF` | _(empty)_ | Runtime secret ref containing a TLS profile |
| `KAFKATOOL` | `True` | Register the Kafka tool set |

The optional native (direct-to-broker) client reads `KAFKA_BOOTSTRAP_SERVERS`
(default `localhost:9092`) and requires the `kafka-mcp[native]` extra. Plus
`HOST` / `PORT` / `TRANSPORT` for HTTP transports. Copy
[`.env.example`](https://github.com/Knuckles-Team/kafka-mcp/blob/main/.env.example)
to `.env` and fill in only what you use.

## Docker Compose

The repo ships [`docker/mcp.compose.yml`](https://github.com/Knuckles-Team/kafka-mcp/blob/main/docker/mcp.compose.yml).
A production-style stack reads a sibling `.env` and publishes the HTTP server on
`:8000`:

```yaml
services:
  kafka-mcp:
    image: example/kafka-mcp@sha256:<digest>
    container_name: kafka-mcp
    hostname: kafka-mcp
    restart: always
    env_file:
      - .env
    environment:
      - PYTHONUNBUFFERED=1
      - HOST=0.0.0.0
      - PORT=8000
      - TRANSPORT=streamable-http
    ports:
      - "8000:8000"
    healthcheck:
      test: ["CMD", "python3", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"]
      interval: 30s
      timeout: 10s
      retries: 3
```

```bash
cp .env.example .env          # then edit KAFKA_* values
docker compose -f docker/mcp.compose.yml up -d
docker compose -f docker/mcp.compose.yml logs -f
```

## Agent server (A2A)

`kafka-mcp` also ships a graph-based Pydantic-AI agent server under the console
script `kafka-agent` (declared in [`a2a.json`](https://github.com/Knuckles-Team/kafka-mcp/blob/main/a2a.json)).
It connects to a running MCP server via `MCP_URL` and exposes an agent HTTP endpoint
for conversational, multi-step Kafka workflows.

```bash
# Point the agent at an already-running MCP server
kafka-agent --mcp-url http://kafka-mcp:8000/mcp --host 0.0.0.0 --port 8080
```

A companion `docker/agent.compose.yml` runs the agent alongside the MCP server:

```yaml
services:
  kafka-agent:
    image: example/kafka-mcp@sha256:<digest>
    container_name: kafka-agent
    hostname: kafka-agent
    restart: always
    command: ["kafka-agent", "--host", "0.0.0.0", "--port", "8080"]
    env_file:
      - .env
    environment:
      - MCP_URL=http://kafka-mcp:8000/mcp
    ports:
      - "8080:8080"
    depends_on:
      - kafka-mcp
```

```bash
docker compose -f docker/agent.compose.yml up -d
```

## Behind a Caddy reverse proxy

Expose the HTTP server on a hostname with automatic TLS. Add to your `Caddyfile`:

```caddy
# Internal (self-signed) — homelab .example.invalid zone
kafka-mcp.example.invalid {
    tls internal
    reverse_proxy kafka-mcp:8000
}
```

```caddy
# Public — automatic Let's Encrypt
kafka-mcp.example.com {
    reverse_proxy kafka-mcp:8000
}
```

Reload Caddy:

```bash
docker compose -f services/caddy/compose.yml exec caddy caddy reload --config /etc/caddy/Caddyfile
```

## DNS with Technitium

Point the hostname at the host running Caddy. Via the Technitium API:

```bash
curl -s "http://technitium.example.invalid:5380/api/zones/records/add" \
  --data-urlencode "token=$TECHNITIUM_DNS_TOKEN" \
  --data-urlencode "domain=kafka-mcp.example.invalid" \
  --data-urlencode "zone=arpa" \
  --data-urlencode "type=A" \
  --data-urlencode "ipAddress=192.0.2.10" \
  --data-urlencode "ttl=3600"
```

…or add an **A record** `kafka-mcp.example.invalid → <caddy-host-ip>` in the Technitium web
console (`http://technitium.example.invalid:5380`). The ecosystem
[`technitium-dns-mcp`](https://knuckles-team.github.io/technitium-dns-mcp/) automates
this as a tool.

## Register with an MCP client

Add to your client's `mcp_config.json`:

```json
{
  "mcpServers": {
    "kafka-mcp": {
      "command": "uv",
      "args": ["run", "kafka-mcp"],
      "env": {
        "KAFKA_REST_URL": "http://your-rest-proxy:8082",
        "KAFKA_CLUSTER_ID": "",
        "KAFKA_TOKEN": "",
        "KAFKATOOL": "True"
      }
    }
  }
}
```

For a remote HTTP server, point the client at `http://kafka-mcp.example.invalid/mcp` instead.
