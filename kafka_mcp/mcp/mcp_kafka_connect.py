"""Thin MCP wrappers around the Kafka Connect worker REST API.

Each tool is a thin shim: it parses params, calls the corresponding
``ConnectApi`` method, and returns the result. All API surface lives in
``kafka_mcp.api.api_client_connect`` — these tools add no business logic.
"""

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
        name: str | None = Field(
            default=None,
            description="Connector name (every action except 'list').",
        ),
        config: dict[str, Any] | None = Field(
            default=None,
            description="Connector config map (create, update).",
        ),
        expand: str | None = Field(
            default=None,
            description="list only: 'status' or 'info' to expand each connector.",
        ),
        include_tasks: bool | None = Field(
            default=None, description="restart only: also restart tasks."
        ),
        only_failed: bool | None = Field(
            default=None, description="restart only: restart only failed instances."
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
        if action == "list":
            return client.list_connectors(expand=expand)
        if action == "get":
            return client.get_connector(name)
        if action == "create":
            return client.create_connector(name, config)
        if action == "update":
            return client.update_connector_config(name, config)
        if action == "delete":
            return client.delete_connector(name)
        if action == "status":
            return client.get_connector_status(name)
        if action == "restart":
            return client.restart_connector(
                name,
                include_tasks=bool(include_tasks),
                only_failed=bool(only_failed),
            )
        if action == "pause":
            return client.pause_connector(name)
        if action == "resume":
            return client.resume_connector(name)
        if action == "offsets":
            return client.get_connector_offsets(name)
        raise ValueError(f"Unknown connect action: {action!r}.")

    @mcp.tool(tags={"connect"})
    async def kafka_connect_plugins() -> Any:
        """List connector plugins available on the Kafka Connect worker's classpath."""
        client = get_connect_client()
        return client.list_connector_plugins()
