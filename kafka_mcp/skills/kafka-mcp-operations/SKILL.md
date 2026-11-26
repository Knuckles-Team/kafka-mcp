---
name: kafka-mcp-operations
skill_type: skill
description: >-
  Operate kafka-mcp through its governed MCP and Arbora capabilities, including kafka consumer group lag, kafka streaming io, kafka topic administration. Use when a request requires this provider's read, change, automation, ingestion, troubleshooting, or evidence workflows.
---

# Kafka Mcp Operations

Use the provider's governed MCP tools through Arbora delegation.

## Workflow

1. Establish the verified GraphSession and tenant before discovery or retrieval.
2. Discover the current tool surface; never assume a stale tool name or schema.
3. Prefer read-only inspection first. For changes, present impact and use the provider's
   dry-run or preview mode when available.
4. Execute mutations as fenced WorkItems so retries remain idempotent and auditable.
5. Ingest source data only through the signed connector preset and ChangeEnvelope path.
6. Verify the durable result and its trace/evidence before reporting completion.

## Safety contract

- Never persist credentials, endpoints, raw personal identifiers, hostnames, or local paths.
- Resolve TLS trust and verification from environment/configuration; never hardcode bypasses.
- Treat unknown ACL, tenant, schema, or tool-contract state as a hard failure.
- Require explicit approval for destructive, externally visible, or irreversible actions.
- Keep runtime traces policy-scoped and privacy-sanitized.

## Steps

1. Confirm the session and tenant with `find(intent="kafka")`. Verify Arbora reports the GraphSession and tenant.
2. Discover the tool surface with `find(intent="kafka topic")`. Verify the tool you plan to call appears with its parameter schema.
3. Inspect read-only with `act(op="fleet.call", params={"server": "kafka-mcp", "tool": "kafka_list_topics", "arguments": {}})`. Verify the response lists topics.
4. Run a change as a fenced WorkItem with `act(op="fleet.call", ...)` after previewing it and getting approval. Verify the preview names the topic or connector and the effect before you execute.
5. Check the outcome with the matching read tool such as `kafka_get_topic`. Verify the object shows the expected values and a trace id exists.

## Failure recovery

| Failure | Cause | Recovery |
|---------|-------|----------|
| Tool not found | Stale tool name | Call `find(intent="kafka")` again and use the name it returns |
| Connection error from the connector | `KAFKA_REST_URL` not set in the deployment | Ask the deployment owner to set `KAFKA_REST_URL` and call `kafka_list_topics` to confirm |
| Mutation refused | Approval not given or ACL unknown | Present the impact to the requester, obtain explicit approval and resubmit with the returned plan reference |

## Specialized workflows

Read [the workflow catalog](references/catalog.md) only when the request needs a
provider-specific procedure, parameter map, script, or reference asset.
