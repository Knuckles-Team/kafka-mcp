# Backing Platform — Apache Kafka

`kafka-mcp` is a **client** of an Apache Kafka cluster, reached either through the
Confluent REST Proxy (the default surface) or directly via the native client. This
page provides a Docker recipe for deploying Kafka locally to serve as the target of
`KAFKA_REST_URL` / `KAFKA_BOOTSTRAP_SERVERS`. For production topologies, follow the
upstream [Apache Kafka documentation](https://kafka.apache.org/documentation/).

!!! note "Backing-system recipe"
    Each connector in the ecosystem follows the same convention — a
    `docs/platform.md` recipe for the system it integrates with, accompanied by a
    sample Compose stack that mirrors [`services/`](https://github.com/Knuckles-Team).
    Systems offered only as a managed service have no local recipe.

## Single-node Kafka (Compose)

Apache publishes the `apache/kafka` image. The following stack runs one KRaft-mode
broker on `:9092` (mirrors `services/kafka/compose.yml`):

```yaml
# docker/kafka.compose.yml
services:
  kafka:
    image: apache/kafka:latest
    container_name: kafka
    hostname: kafka
    restart: unless-stopped
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: "1"
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0"
    volumes:
      - kafka_data:/var/lib/kafka/data

volumes:
  kafka_data:
```

```bash
docker compose -f docker/kafka.compose.yml up -d
```

## Add the Confluent REST Proxy

The default `kafka-mcp` surface speaks to the Confluent REST Proxy. Add it to the
stack so the connector reaches Kafka over HTTP on `:8082`:

```yaml
# docker/kafka.compose.yml (continued)
  rest-proxy:
    image: confluentinc/cp-kafka-rest:latest
    container_name: kafka-rest
    hostname: kafka-rest
    restart: unless-stopped
    depends_on:
      - kafka
    ports:
      - "8082:8082"
    environment:
      KAFKA_REST_HOST_NAME: kafka-rest
      KAFKA_REST_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_REST_LISTENERS: http://0.0.0.0:8082
```

## Connect kafka-mcp

Point the connector at the REST Proxy (default surface):

```bash
export KAFKA_REST_URL=http://localhost:8082
export KAFKA_SSL_VERIFY=False          # plaintext / self-signed homelab

kafka-mcp --transport streamable-http --host 0.0.0.0 --port 8000
```

Or, with the `kafka-mcp[native]` extra, connect directly to brokers:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

## Combined deployment

A combined stack places Kafka, the REST Proxy, and the MCP server on one Docker
network, so the server reaches the proxy by container name:

```yaml
# docker/stack.compose.yml
services:
  kafka:
    image: apache/kafka:latest
    hostname: kafka
    environment:
      KAFKA_NODE_ID: "1"
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
    volumes: ["kafka_data:/var/lib/kafka/data"]

  rest-proxy:
    image: confluentinc/cp-kafka-rest:latest
    hostname: kafka-rest
    depends_on: [kafka]
    environment:
      KAFKA_REST_HOST_NAME: kafka-rest
      KAFKA_REST_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_REST_LISTENERS: http://0.0.0.0:8082

  kafka-mcp:
    image: knucklessg1/kafka-mcp:latest
    depends_on: [rest-proxy]
    environment:
      - KAFKA_REST_URL=http://kafka-rest:8082
      - TRANSPORT=streamable-http
      - HOST=0.0.0.0
      - PORT=8000
    ports: ["8000:8000"]

volumes:
  kafka_data:
```

```bash
docker compose -f docker/stack.compose.yml up -d
```
