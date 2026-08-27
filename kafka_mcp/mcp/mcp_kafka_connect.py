"""Thin MCP wrappers around the Kafka Connect worker REST API.

Each tool is a thin shim: it parses params, calls the corresponding
``ConnectApi`` method, and returns the result. All API surface lives in
``kafka_mcp.api.api_client_connect`` — these tools add no business logic.
"""

import json
from typing import Any

from fastmcp import FastMCP
from pydantic import Field

from kafka_mcp.auth import get_connect_client


def register_kafka_connect_tools(mcp: FastMCP) -> None:
    """Register Kafka Connect connector-lifecycle and plugin-discovery tools."""

    @mcp.tool(tags={"connect"})
    async def kafka_connect(
        action: str = Field(
            description=(
                "Kafka Connect connector action. One of: 'list', 'get', 'create', "
                "'update', 'delete', 'status', 'restart', 'pause', 'resume', "
                "'offsets'."
            )
        ),
        params_json: str = Field(
            default="{}",
            description=(
                "JSON of arguments. list: {} or {\"expand\": \"status\"}. get/"
                "delete/status/pause/resume/offsets: {\"name\": \"ca51pilot\"}. "
                "create: {\"name\": \"ca51pilot\", \"config\": {\"connector.class\": "
                '"io.debezium.connector.postgresql.PostgresConnector", ...}}. '
                'update: {"name": "ca51pilot", "config": {...}} (create-or-update '
                "per the Connect REST contract — use 'create' when the caller "
                "intends to fail loud on an existing name instead). restart: "
                '{"name": "ca51pilot", "include_tasks": false, '
                '"only_failed": false}.'
            ),
        ),
    ) -> Any:
        """Manage Kafka Connect connectors via the worker's REST API (:8083).

        ``create`` is idempotent by connector name: a second ``create`` against
        an existing name surfaces Connect's own ``409 Conflict`` rather than
        being swallowed or silently turned into an update — use ``update`` to
        change an existing connector's config. ``delete`` and ``restart`` are
        **not** idempotent: a repeat ``delete`` 404s, and each ``restart`` is a
        distinct operation, not a no-op if already running.
        """
        client = get_connect_client()
        p = json.loads(params_json) if params_json else {}
        if action == "list":
            return client.list_connectors(expand=p.get("expand"))
        if action == "get":
            return client.get_connector(p["name"])
        if action == "create":
            return client.create_connector(p["name"], p["config"])
        if action == "update":
            return client.update_connector_config(p["name"], p["config"])
        if action == "delete":
            return client.delete_connector(p["name"])
        if action == "status":
            return client.get_connector_status(p["name"])
        if action == "restart":
            return client.restart_connector(
                p["name"],
                include_tasks=p.get("include_tasks", False),
                only_failed=p.get("only_failed", False),
            )
        if action == "pause":
            return client.pause_connector(p["name"])
        if action == "resume":
            return client.resume_connector(p["name"])
        if action == "offsets":
            return client.get_connector_offsets(p["name"])
        raise ValueError(f"Unknown connect action: {action!r}.")

    @mcp.tool(tags={"connect"})
    async def kafka_connect_plugins() -> Any:
        """List connector plugins available on the Kafka Connect worker's classpath."""
        client = get_connect_client()
        return client.list_connector_plugins()
