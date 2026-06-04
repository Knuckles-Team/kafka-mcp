# kafka-mcp

A Model Context Protocol (MCP) server for Kafka integration.

## Table of Contents
- [Overview](#overview)
- [Installation](#installation)
- [Usage](#usage)
- [Architecture](#architecture)
- [Deployment](#deployment)
- [Environment Variables](#environment-variables)
- [MCP Tools](#mcp-tools)

## Overview
kafka-mcp exposes a standardized interface to interact with Kafka using the Model Context Protocol.

## Installation
```bash
pip install -e .
```

## Usage
Run the MCP server directly:
```bash
python -m kafka_mcp
```

## Architecture
See `/docs` for architectural diagrams and further documentation.

## Deployment
### Bare-metal
```bash
python -m kafka_mcp.agent_server
```

### Docker
```bash
docker compose -f docker/agent.compose.yml up -d
```

## Environment Variables
| Variable | Description |
|----------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka brokers |
| `KAFKA_SCHEMA_REGISTRY_URL` | Schema registry |

## MCP Tools
| Tool | Description |
|------|-------------|
| `get_kafka_info` | Retrieve basic information from Kafka |
| `query_kafka` | Run a query against the Kafka instance |
