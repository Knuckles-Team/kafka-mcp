"""Kafka Connect REST API wrapper (worker REST surface, default ``:8083``).

Covers the Kafka Connect worker's connector-lifecycle surface used to drive
Debezium/CDC connectors end to end:

* **Connectors** — list, create, get, get config, update config (create-or-update
  semantics per the Connect REST contract), delete.
* **Status/control** — status, restart (connector and/or its tasks), pause, resume.
* **Offsets** — read a connector's committed source/sink offsets (Connect's
  "Connector Offset Management" surface, available on Connect >= 3.5/Kafka 3.6).
* **Plugins** — list the connector plugins the worker has on its classpath.

Base URL comes from ``KAFKA_CONNECT_URL`` (default ``http://localhost:8083``). Unlike
the Confluent REST Proxy client, Kafka Connect's own REST API has **no documented
built-in auth** as of this writing — callers may still layer a bearer token or basic
auth (e.g. behind a reverse-proxy/ingress that adds one), so this client accepts the
same optional credential shape as :class:`~kafka_mcp.api.api_client_base.ApiClientBase`
and simply sends nothing when none is configured.
"""

from typing import Any

from kafka_mcp.api.api_client_base import ApiClientBase


class ConnectApi(ApiClientBase):
    """Client for a Kafka Connect worker's REST API."""

    # ------------------------------------------------------------------ #
    # Connectors
    # ------------------------------------------------------------------ #
    def list_connectors(self, *, expand: str | None = None) -> Any:
        """List connector names, or expanded ``{name: {status, info}}`` detail.

        ``expand`` is a comma-separated subset of ``status``/``info`` per the
        Connect REST contract (e.g. ``"status"``); omit for a bare name list.
        """
        params = {"expand": expand} if expand else None
        return self.request("GET", "connectors", params=params, accept="application/json")

    def get_connector(self, name: str) -> Any:
        """Get one connector's top-level descriptor (name/config/tasks/type)."""
        return self.request("GET", f"connectors/{name}", accept="application/json")

    def get_connector_config(self, name: str) -> Any:
        """Get one connector's raw config map."""
        return self.request(
            "GET", f"connectors/{name}/config", accept="application/json"
        )

    def create_connector(self, name: str, config: dict[str, Any]) -> Any:
        """Create a new connector via ``POST /connectors``.

        Idempotent by name: the Connect REST API itself returns ``409 Conflict``
        (not silently ignored/overwritten here) if ``name`` already exists — the
        caller must go through :meth:`update_connector_config` to change an
        existing connector's config.
        """
        return self.request(
            "POST",
            "connectors",
            json={"name": name, "config": config},
            content_type="application/json",
            accept="application/json",
        )

    def update_connector_config(self, name: str, config: dict[str, Any]) -> Any:
        """Create-or-update a connector's config via ``PUT /connectors/{name}/config``."""
        return self.request(
            "PUT",
            f"connectors/{name}/config",
            json=config,
            content_type="application/json",
            accept="application/json",
        )

    def delete_connector(self, name: str) -> Any:
        """Delete a connector and its tasks. Not idempotent — a repeat call 404s."""
        return self.request("DELETE", f"connectors/{name}")

    # ------------------------------------------------------------------ #
    # Status / control
    # ------------------------------------------------------------------ #
    def get_connector_status(self, name: str) -> Any:
        """Get a connector's and its tasks' current state."""
        return self.request(
            "GET", f"connectors/{name}/status", accept="application/json"
        )

    def restart_connector(
        self,
        name: str,
        *,
        include_tasks: bool = False,
        only_failed: bool = False,
    ) -> Any:
        """Restart a connector (optionally its tasks too). Not idempotent."""
        params: dict[str, Any] = {}
        if include_tasks:
            params["includeTasks"] = "true"
        if only_failed:
            params["onlyFailed"] = "true"
        return self.request(
            "POST",
            f"connectors/{name}/restart",
            params=params or None,
            accept="application/json",
        )

    def pause_connector(self, name: str) -> Any:
        """Pause a connector and its tasks."""
        return self.request("PUT", f"connectors/{name}/pause")

    def resume_connector(self, name: str) -> Any:
        """Resume a paused connector and its tasks."""
        return self.request("PUT", f"connectors/{name}/resume")

    # ------------------------------------------------------------------ #
    # Offsets
    # ------------------------------------------------------------------ #
    def get_connector_offsets(self, name: str) -> Any:
        """Read a connector's committed offsets (Connect's offset-management API)."""
        return self.request(
            "GET", f"connectors/{name}/offsets", accept="application/json"
        )

    # ------------------------------------------------------------------ #
    # Plugins
    # ------------------------------------------------------------------ #
    def list_connector_plugins(self) -> Any:
        """List connector plugins available on the worker's classpath."""
        return self.request(
            "GET", "connector-plugins", accept="application/json"
        )
