# Architecture

How `kafka-mcp` fits together: a layered REST client, thin MCP tool wrappers, an
optional native broker client, and an optional Pydantic-AI agent server.

## The request path

```mermaid
flowchart LR
    Agent["Agent / policy router"] -->|MCP call| Tools["kafka_* MCP tools<br/>(mcp/mcp_kafka.py)"]
    CLI["kafka-agent<br/>(Pydantic-AI graph)"] -->|MCP_URL| Tools
    Tools -->|"REST Proxy v3/v2"| Api["KafkaApi<br/>(api/api_client_kafka.py)"]
    Tools -.->|"native extra"| Native["NativeKafkaClient<br/>(api/api_client_native.py)"]
    Api -->|"requests"| REST[("Confluent REST Proxy")]
    Native -->|"confluent-kafka"| Brokers[("Kafka brokers")]
    REST --> Brokers

    classDef store fill:#dae8fe,stroke:#6c8ebf;
    classDef proc fill:#d5e8d4,stroke:#82b366;
    class REST,Brokers store;
    class Tools,Api,Native proc;
```

The MCP tools add no business logic — they parse the `action` / `params_json` payload
and dispatch to a `KafkaApi` (or `NativeKafkaClient`) method. All API surface lives in
`kafka_mcp.api`.

## Layered client

```mermaid
flowchart TB
    Auth["auth.get_client() / get_native_client()<br/>builds clients from the environment"]
    Auth --> Api2["Api (api_client.py)<br/>= KafkaApi subclass"]
    Api2 --> Base["ApiClientBase<br/>(api/api_client_base.py)<br/>auth, TLS, request()"]
    Auth --> Native2["NativeKafkaClient<br/>(api/api_client_native.py)"]

    classDef proc fill:#d5e8d4,stroke:#82b366;
    class Auth,Api2,Base,Native2 proc;
```

- **`ApiClientBase`** centralizes bearer-token / basic-auth, TLS verification, and a
  single `request()` helper.
- **`KafkaApi`** organizes the REST surface by resource (clusters, topics,
  partitions, records, consumer groups, brokers, ACLs) and resolves the cluster id
  lazily.
- **`Api`** is the public facade re-exported for import.
- **`NativeKafkaClient`** is the optional `confluent-kafka` path for direct broker
  produce / consume / admin.

## Entry points

- `kafka_mcp.mcp_server:mcp_server` — the FastMCP server (`kafka-mcp` console
  script), with a `/health` endpoint on HTTP transports.
- `kafka_mcp.agent_server:agent_server` — the Pydantic-AI agent server
  (`kafka-agent` console script), wired to the MCP server through `MCP_URL`.
