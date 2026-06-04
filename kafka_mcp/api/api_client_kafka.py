"""Apache Kafka REST API wrapper (Confluent REST Proxy v3, with v2 helpers).

Covers the HTTP surfaces the Confluent REST Proxy exposes:

* **Clusters** — list clusters, get a cluster.
* **Topics** — list, create, describe, delete, list/update topic configs.
* **Partitions** — list partitions for a topic, get a partition.
* **Records** — produce records (v3 ``/records`` streaming endpoint) and the
  v2 consumer-instance helpers (create/subscribe/poll/commit/delete).
* **Consumer groups** — list, describe, lag summary, list consumers, lag.
* **Brokers** — list, get, broker configs.
* **ACLs** — search/list, create, delete.

Base URL comes from ``KAFKA_REST_URL`` (default ``http://localhost:8082``).
The active cluster id is resolved lazily via :meth:`cluster_id` — it caches the
first cluster returned by ``list_clusters`` unless ``KAFKA_CLUSTER_ID`` is set.
"""

import os
from typing import Any

from kafka_mcp.api.api_client_base import ApiClientBase

V2_JSON = "application/vnd.kafka.json.v2+json"
V2_ACCEPT = "application/vnd.kafka.v2+json"


class KafkaApi(ApiClientBase):
    """Full client for a Confluent REST Proxy in front of Apache Kafka."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._cluster_id: str | None = os.getenv("KAFKA_CLUSTER_ID") or None

    # ------------------------------------------------------------------ #
    # Cluster resolution helper
    # ------------------------------------------------------------------ #
    def cluster_id(self) -> str:
        """Return the active Kafka cluster id, resolving + caching the first.

        Honors the ``KAFKA_CLUSTER_ID`` env override; otherwise calls
        ``list_clusters`` and caches the first cluster's id.
        """
        if self._cluster_id:
            return self._cluster_id
        data = self.list_clusters()
        items = data.get("data") if isinstance(data, dict) else None
        if not items:
            raise RuntimeError("No Kafka clusters returned by the REST Proxy.")
        self._cluster_id = items[0]["cluster_id"]
        return self._cluster_id

    def _cid(self, cluster_id: str | None) -> str:
        return cluster_id or self.cluster_id()

    # ------------------------------------------------------------------ #
    # Clusters
    # ------------------------------------------------------------------ #
    def list_clusters(self) -> Any:
        """List Kafka clusters known to the REST Proxy."""
        return self.request("GET", "v3/clusters", accept="application/json")

    def get_cluster(self, cluster_id: str | None = None) -> Any:
        """Get a single cluster's metadata."""
        return self.request(
            "GET", f"v3/clusters/{self._cid(cluster_id)}", accept="application/json"
        )

    # ------------------------------------------------------------------ #
    # Topics
    # ------------------------------------------------------------------ #
    def list_topics(self, cluster_id: str | None = None) -> Any:
        """List topics in a cluster."""
        return self.request(
            "GET",
            f"v3/clusters/{self._cid(cluster_id)}/topics",
            accept="application/json",
        )

    def get_topic(self, topic: str, cluster_id: str | None = None) -> Any:
        """Describe a single topic."""
        return self.request(
            "GET",
            f"v3/clusters/{self._cid(cluster_id)}/topics/{topic}",
            accept="application/json",
        )

    def create_topic(
        self,
        topic: str,
        partitions_count: int = 1,
        replication_factor: int = 1,
        configs: dict[str, Any] | None = None,
        cluster_id: str | None = None,
    ) -> Any:
        """Create a topic with partitions, replication, and optional configs."""
        body: dict[str, Any] = {
            "topic_name": topic,
            "partitions_count": partitions_count,
            "replication_factor": replication_factor,
        }
        if configs:
            body["configs"] = [
                {"name": k, "value": v} for k, v in configs.items()
            ]
        return self.request(
            "POST",
            f"v3/clusters/{self._cid(cluster_id)}/topics",
            json=body,
            content_type="application/json",
            accept="application/json",
        )

    def delete_topic(self, topic: str, cluster_id: str | None = None) -> Any:
        """Delete a topic."""
        return self.request(
            "DELETE",
            f"v3/clusters/{self._cid(cluster_id)}/topics/{topic}",
        )

    def list_topic_configs(self, topic: str, cluster_id: str | None = None) -> Any:
        """List configuration entries for a topic."""
        return self.request(
            "GET",
            f"v3/clusters/{self._cid(cluster_id)}/topics/{topic}/configs",
            accept="application/json",
        )

    def update_topic_config(
        self,
        topic: str,
        name: str,
        value: Any,
        cluster_id: str | None = None,
    ) -> Any:
        """Update (alter) a single topic configuration entry."""
        return self.request(
            "PUT",
            f"v3/clusters/{self._cid(cluster_id)}/topics/{topic}/configs/{name}",
            json={"value": value},
            content_type="application/json",
        )

    def update_topic_configs(
        self,
        topic: str,
        configs: dict[str, Any],
        cluster_id: str | None = None,
    ) -> Any:
        """Batch-alter multiple topic configuration entries."""
        body = {
            "data": [{"name": k, "value": v} for k, v in configs.items()]
        }
        return self.request(
            "POST",
            f"v3/clusters/{self._cid(cluster_id)}/topics/{topic}/configs:alter",
            json=body,
            content_type="application/json",
        )

    # ------------------------------------------------------------------ #
    # Partitions
    # ------------------------------------------------------------------ #
    def list_partitions(self, topic: str, cluster_id: str | None = None) -> Any:
        """List partitions for a topic."""
        return self.request(
            "GET",
            f"v3/clusters/{self._cid(cluster_id)}/topics/{topic}/partitions",
            accept="application/json",
        )

    def get_partition(
        self, topic: str, partition_id: int, cluster_id: str | None = None
    ) -> Any:
        """Get a single partition of a topic."""
        return self.request(
            "GET",
            f"v3/clusters/{self._cid(cluster_id)}/topics/{topic}"
            f"/partitions/{partition_id}",
            accept="application/json",
        )

    # ------------------------------------------------------------------ #
    # Records (v3 produce)
    # ------------------------------------------------------------------ #
    def produce_record(
        self,
        topic: str,
        value: Any,
        key: Any = None,
        value_format: str = "JSON",
        key_format: str | None = None,
        partition_id: int | None = None,
        headers: list[dict[str, Any]] | None = None,
        cluster_id: str | None = None,
    ) -> Any:
        """Produce a single record to a topic via the v3 ``/records`` endpoint.

        ``value_format``/``key_format`` are one of ``STRING``, ``JSON``,
        ``BINARY``, ``AVRO``, ``JSONSCHEMA``, ``PROTOBUF``.
        """
        body: dict[str, Any] = {
            "value": {"type": value_format, "data": value},
        }
        if key is not None:
            body["key"] = {"type": key_format or value_format, "data": key}
        if partition_id is not None:
            body["partition_id"] = partition_id
        if headers:
            body["headers"] = headers
        return self.request(
            "POST",
            f"v3/clusters/{self._cid(cluster_id)}/topics/{topic}/records",
            json=body,
            content_type="application/json",
            accept="application/json",
        )

    # ------------------------------------------------------------------ #
    # Records (v2 consumer helpers)
    # ------------------------------------------------------------------ #
    def create_consumer(
        self,
        group: str,
        name: str | None = None,
        fmt: str = "json",
        auto_offset_reset: str = "earliest",
    ) -> Any:
        """Create a v2 consumer instance inside a consumer group."""
        body: dict[str, Any] = {
            "format": fmt,
            "auto.offset.reset": auto_offset_reset,
        }
        if name:
            body["name"] = name
        return self.request(
            "POST",
            f"consumers/{group}",
            json=body,
            content_type=V2_JSON,
            accept=V2_ACCEPT,
        )

    def subscribe_consumer(
        self, group: str, instance: str, topics: list[str]
    ) -> Any:
        """Subscribe a v2 consumer instance to a list of topics."""
        return self.request(
            "POST",
            f"consumers/{group}/instances/{instance}/subscription",
            json={"topics": topics},
            content_type=V2_JSON,
        )

    def consume_records(
        self,
        group: str,
        instance: str,
        timeout: int = 1000,
        max_bytes: int | None = None,
        fmt: str = "json",
    ) -> Any:
        """Poll records from a v2 consumer instance."""
        params: dict[str, Any] = {"timeout": timeout}
        if max_bytes is not None:
            params["max_bytes"] = max_bytes
        accept = f"application/vnd.kafka.{fmt}.v2+json"
        return self.request(
            "GET",
            f"consumers/{group}/instances/{instance}/records",
            params=params,
            accept=accept,
        )

    def commit_offsets(self, group: str, instance: str) -> Any:
        """Commit current offsets for a v2 consumer instance."""
        return self.request(
            "POST",
            f"consumers/{group}/instances/{instance}/offsets",
            content_type=V2_JSON,
        )

    def delete_consumer(self, group: str, instance: str) -> Any:
        """Delete a v2 consumer instance."""
        return self.request(
            "DELETE",
            f"consumers/{group}/instances/{instance}",
            content_type=V2_JSON,
        )

    # ------------------------------------------------------------------ #
    # Consumer groups
    # ------------------------------------------------------------------ #
    def list_consumer_groups(self, cluster_id: str | None = None) -> Any:
        """List consumer groups in a cluster."""
        return self.request(
            "GET",
            f"v3/clusters/{self._cid(cluster_id)}/consumer-groups",
            accept="application/json",
        )

    def get_consumer_group(
        self, group: str, cluster_id: str | None = None
    ) -> Any:
        """Describe a single consumer group."""
        return self.request(
            "GET",
            f"v3/clusters/{self._cid(cluster_id)}/consumer-groups/{group}",
            accept="application/json",
        )

    def list_consumers(self, group: str, cluster_id: str | None = None) -> Any:
        """List the consumers (members) of a consumer group."""
        return self.request(
            "GET",
            f"v3/clusters/{self._cid(cluster_id)}/consumer-groups/{group}/consumers",
            accept="application/json",
        )

    def get_group_lag_summary(
        self, group: str, cluster_id: str | None = None
    ) -> Any:
        """Get the lag summary for a consumer group."""
        return self.request(
            "GET",
            f"v3/clusters/{self._cid(cluster_id)}/consumer-groups/{group}"
            "/lag-summary",
            accept="application/json",
        )

    def get_group_lags(self, group: str, cluster_id: str | None = None) -> Any:
        """List per-partition lags for a consumer group."""
        return self.request(
            "GET",
            f"v3/clusters/{self._cid(cluster_id)}/consumer-groups/{group}/lags",
            accept="application/json",
        )

    # ------------------------------------------------------------------ #
    # Brokers
    # ------------------------------------------------------------------ #
    def list_brokers(self, cluster_id: str | None = None) -> Any:
        """List brokers in a cluster."""
        return self.request(
            "GET",
            f"v3/clusters/{self._cid(cluster_id)}/brokers",
            accept="application/json",
        )

    def get_broker(self, broker_id: int, cluster_id: str | None = None) -> Any:
        """Get a single broker's metadata."""
        return self.request(
            "GET",
            f"v3/clusters/{self._cid(cluster_id)}/brokers/{broker_id}",
            accept="application/json",
        )

    def list_broker_configs(
        self, broker_id: int, cluster_id: str | None = None
    ) -> Any:
        """List configuration entries for a broker."""
        return self.request(
            "GET",
            f"v3/clusters/{self._cid(cluster_id)}/brokers/{broker_id}/configs",
            accept="application/json",
        )

    # ------------------------------------------------------------------ #
    # ACLs
    # ------------------------------------------------------------------ #
    def list_acls(
        self,
        resource_type: str | None = None,
        resource_name: str | None = None,
        pattern_type: str | None = None,
        principal: str | None = None,
        operation: str | None = None,
        permission: str | None = None,
        cluster_id: str | None = None,
    ) -> Any:
        """Search ACLs (all filters optional)."""
        params: dict[str, Any] = {}
        if resource_type:
            params["resource_type"] = resource_type
        if resource_name:
            params["resource_name"] = resource_name
        if pattern_type:
            params["pattern_type"] = pattern_type
        if principal:
            params["principal"] = principal
        if operation:
            params["operation"] = operation
        if permission:
            params["permission"] = permission
        return self.request(
            "GET",
            f"v3/clusters/{self._cid(cluster_id)}/acls",
            params=params or None,
            accept="application/json",
        )

    def create_acl(
        self,
        resource_type: str,
        resource_name: str,
        pattern_type: str,
        principal: str,
        host: str,
        operation: str,
        permission: str,
        cluster_id: str | None = None,
    ) -> Any:
        """Create an ACL binding."""
        body = {
            "resource_type": resource_type,
            "resource_name": resource_name,
            "pattern_type": pattern_type,
            "principal": principal,
            "host": host,
            "operation": operation,
            "permission": permission,
        }
        return self.request(
            "POST",
            f"v3/clusters/{self._cid(cluster_id)}/acls",
            json=body,
            content_type="application/json",
        )

    def delete_acls(
        self,
        resource_type: str | None = None,
        resource_name: str | None = None,
        pattern_type: str | None = None,
        principal: str | None = None,
        host: str | None = None,
        operation: str | None = None,
        permission: str | None = None,
        cluster_id: str | None = None,
    ) -> Any:
        """Delete ACLs matching the given filter."""
        params: dict[str, Any] = {}
        if resource_type:
            params["resource_type"] = resource_type
        if resource_name:
            params["resource_name"] = resource_name
        if pattern_type:
            params["pattern_type"] = pattern_type
        if principal:
            params["principal"] = principal
        if host:
            params["host"] = host
        if operation:
            params["operation"] = operation
        if permission:
            params["permission"] = permission
        return self.request(
            "DELETE",
            f"v3/clusters/{self._cid(cluster_id)}/acls",
            params=params or None,
            accept="application/json",
        )
