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
    async def kafka_connect_list(
        expand: str | None = Field(default=None, description="'status' or 'info' to expand each connector."),
    ) -> Any:
        """List the connectors on the Kafka Connect worker."""
        client = get_connect_client()
        return client.list_connectors(expand=expand)

    @mcp.tool(tags={"connect"})
    async def kafka_connect_get(
        name: str = Field(description="Connector name."),
    ) -> Any:
        """Get one connector's info (config and tasks)."""
        client = get_connect_client()
        return client.get_connector(name)

    @mcp.tool(tags={"connect"})
    async def kafka_connect_create(
        name: str = Field(description="Connector name."),
        config: dict[str, Any] = Field(description="Connector config map."),
    ) -> Any:
        """Create a connector. A second create on an existing name returns Connect's own 409; use kafka_connect_update to change config."""
        client = get_connect_client()
        return client.create_connector(name, config)

    @mcp.tool(tags={"connect"})
    async def kafka_connect_update(
        name: str = Field(description="Connector name."),
        config: dict[str, Any] = Field(description="Connector config map."),
    ) -> Any:
        """Replace an existing connector's config."""
        client = get_connect_client()
        return client.update_connector_config(name, config)

    @mcp.tool(tags={"connect"})
    async def kafka_connect_delete(
        name: str = Field(description="Connector name."),
    ) -> Any:
        """Delete a connector. Not idempotent: a repeat delete returns 404. Destructive."""
        client = get_connect_client()
        return client.delete_connector(name)

    @mcp.tool(tags={"connect"})
    async def kafka_connect_status(
        name: str = Field(description="Connector name."),
    ) -> Any:
        """Read a connector's and its tasks' runtime status."""
        client = get_connect_client()
        return client.get_connector_status(name)

    @mcp.tool(tags={"connect"})
    async def kafka_connect_restart(
        name: str = Field(description="Connector name."),
        include_tasks: bool = Field(default=False, description="Also restart tasks."),
        only_failed: bool = Field(default=False, description="Restart only failed instances."),
    ) -> Any:
        """Restart a connector. Each restart is a distinct operation, not a no-op when running."""
        client = get_connect_client()
        return client.restart_connector(name, include_tasks=include_tasks, only_failed=only_failed)

    @mcp.tool(tags={"connect"})
    async def kafka_connect_pause(
        name: str = Field(description="Connector name."),
    ) -> Any:
        """Pause a connector and its tasks."""
        client = get_connect_client()
        return client.pause_connector(name)

    @mcp.tool(tags={"connect"})
    async def kafka_connect_resume(
        name: str = Field(description="Connector name."),
    ) -> Any:
        """Resume a paused connector."""
        client = get_connect_client()
        return client.resume_connector(name)

    @mcp.tool(tags={"connect"})
    async def kafka_connect_offsets(
        name: str = Field(description="Connector name."),
    ) -> Any:
        """Read a connector's committed offsets."""
        client = get_connect_client()
        return client.get_connector_offsets(name)

    @mcp.tool(tags={"connect"})
    async def kafka_connect_plugins() -> Any:
        """List connector plugins available on the Kafka Connect worker's classpath."""
        client = get_connect_client()
        return client.list_connector_plugins()
