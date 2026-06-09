# Usage — API / CLI / MCP

`kafka-mcp` exposes the same capability three ways: as **MCP tools** an agent calls,
as a **Python API** (`KafkaApi`) you import, and as **command-line servers**. The
complete tool surface is summarized in [Overview](overview.md).

## As an MCP server

Once [deployed](deployment.md), the server registers six action-dispatch tools. Each
takes an `action` and a JSON `params_json` payload, so a single tool covers a whole
Kafka resource family:

| Tool | Actions |
|---|---|
| `kafka_topics` | `list`, `create`, `describe`, `delete`, `configs`, `update_config` |
| `kafka_partitions` | `list`, `get` |
| `kafka_records` | `produce`, `create_consumer`, `subscribe`, `consume`, `commit`, `delete_consumer` |
| `kafka_groups` | `list`, `describe`, `consumers`, `lag_summary`, `lags` |
| `kafka_cluster` | `list_clusters`, `get_cluster`, `list_brokers`, `get_broker`, `broker_configs`, `list_acls`, `create_acl`, `delete_acls` |
| `kafka_native` | `produce`, `consume`, `create_topic`, `delete_topic`, `list_topics`, `list_groups` (requires `kafka-mcp[native]`) |

Example agent prompts that map onto these tools:

- *"List the topics in the cluster"* → `kafka_topics` (`list`)
- *"Create a topic `events` with 3 partitions"* → `kafka_topics` (`create`)
- *"What is the consumer lag for group `billing`?"* → `kafka_groups` (`lag_summary`)
- *"Produce a JSON record to `events`"* → `kafka_records` (`produce`)

## As a Python API

`KafkaApi` (exported as `Api`) is a `requests`-based facade over the Confluent REST
Proxy. The active cluster id is resolved lazily, so most calls need no cluster
argument.

```python
from kafka_mcp.api_client import Api

api = Api(
    base_url="http://your-rest-proxy:8082",
    token=None,            # or a bearer token
    verify=True,
)

# Reads
clusters = api.list_clusters()
topics = api.list_topics()                     # uses the resolved cluster id
detail = api.get_topic("events")
groups = api.list_consumer_groups()
lag = api.get_group_lag_summary("billing")
brokers = api.list_brokers()
```

Build a client straight from the environment:

```python
from kafka_mcp.auth import get_client
api = get_client()         # reads KAFKA_REST_URL / KAFKA_TOKEN / ... from the environment
```

### Writes

Produce records and administer topics, configs, and ACLs:

```python
api.create_topic("events", partitions_count=3, replication_factor=1)
api.produce_record("events", value={"id": 1}, key="k", value_format="JSON")
api.update_topic_config("events", "retention.ms", "120000")
api.create_acl(
    resource_type="TOPIC",
    resource_name="events",
    pattern_type="LITERAL",
    principal="User:alice",
    host="*",
    operation="READ",
    permission="ALLOW",
)
```

### Native (direct-to-broker) client

With the `kafka-mcp[native]` extra, connect straight to brokers:

```python
from kafka_mcp.auth import get_native_client

native = get_native_client()                   # reads KAFKA_BOOTSTRAP_SERVERS
native.produce("events", "hello", key="k")
records = native.consume("events", "my-group", max_messages=10, timeout=5.0)
```

## As a CLI

Two console scripts ship with the package:

```bash
# MCP server (stdio by default; see Deployment for HTTP transports)
kafka-mcp --transport streamable-http --host 0.0.0.0 --port 8000

# A2A agent server (connects to a running MCP server via --mcp-url)
kafka-agent --mcp-url http://localhost:8000/mcp --host 0.0.0.0 --port 8080
```

Each connector reads its own configuration from the environment (see
[`.env.example`](https://github.com/Knuckles-Team/kafka-mcp/blob/main/.env.example))
and remains inactive when the required credentials are absent.
