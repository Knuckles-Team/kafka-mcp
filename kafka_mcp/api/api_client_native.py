"""Native Apache Kafka client fallback built on ``confluent_kafka``.

This is a thin convenience wrapper for environments that talk to brokers
directly (no REST Proxy). ``confluent-kafka`` carries a C/librdkafka build
requirement that breaks portable wheel builds, so it is an OPTIONAL extra
(``kafka-mcp[native]``) and is imported lazily inside each method. If it is
not installed, the method raises a clear ``RuntimeError``.
"""

from typing import Any

_NATIVE_HINT = "Install kafka-mcp[native] for native client support"


class NativeKafkaClient:
    """Direct-to-broker admin/produce/consume helper (lazy confluent_kafka)."""

    def __init__(self, bootstrap_servers: str = "localhost:9092") -> None:
        self.bootstrap_servers = bootstrap_servers

    def _conf(self, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        conf = {"bootstrap.servers": self.bootstrap_servers}
        if extra:
            conf.update(extra)
        return conf

    # ------------------------------------------------------------------ #
    # Admin
    # ------------------------------------------------------------------ #
    def create_topics(
        self,
        topics: list[str],
        partitions_count: int = 1,
        replication_factor: int = 1,
    ) -> Any:
        try:
            from confluent_kafka.admin import AdminClient, NewTopic
        except ImportError as exc:
            raise RuntimeError(_NATIVE_HINT) from exc
        admin = AdminClient(self._conf())
        new_topics = [
            NewTopic(t, num_partitions=partitions_count,
                     replication_factor=replication_factor)
            for t in topics
        ]
        futures = admin.create_topics(new_topics)
        results: dict[str, str] = {}
        for topic, future in futures.items():
            try:
                future.result()
                results[topic] = "created"
            except Exception as exc:  # noqa: BLE001
                results[topic] = f"error: {type(exc).__name__}"
        return {"topics": results}

    def delete_topics(self, topics: list[str]) -> Any:
        try:
            from confluent_kafka.admin import AdminClient
        except ImportError as exc:
            raise RuntimeError(_NATIVE_HINT) from exc
        admin = AdminClient(self._conf())
        futures = admin.delete_topics(topics)
        results: dict[str, str] = {}
        for topic, future in futures.items():
            try:
                future.result()
                results[topic] = "deleted"
            except Exception as exc:  # noqa: BLE001
                results[topic] = f"error: {type(exc).__name__}"
        return {"topics": results}

    def list_topics(self, timeout: float = 10.0) -> Any:
        try:
            from confluent_kafka.admin import AdminClient
        except ImportError as exc:
            raise RuntimeError(_NATIVE_HINT) from exc
        admin = AdminClient(self._conf())
        md = admin.list_topics(timeout=timeout)
        return {
            "topics": [
                {"name": name, "partitions": len(t.partitions)}
                for name, t in md.topics.items()
            ]
        }

    def list_consumer_groups(self, timeout: float = 10.0) -> Any:
        try:
            from confluent_kafka.admin import AdminClient
        except ImportError as exc:
            raise RuntimeError(_NATIVE_HINT) from exc
        admin = AdminClient(self._conf())
        future = admin.list_consumer_groups(request_timeout=timeout)
        result = future.result()
        return {
            "groups": [
                {"group_id": g.group_id, "state": str(getattr(g, "state", ""))}
                for g in result.valid
            ]
        }

    # ------------------------------------------------------------------ #
    # Produce / Consume
    # ------------------------------------------------------------------ #
    def produce(
        self, topic: str, value: str, key: str | None = None
    ) -> Any:
        try:
            from confluent_kafka import Producer
        except ImportError as exc:
            raise RuntimeError(_NATIVE_HINT) from exc
        producer = Producer(self._conf())
        producer.produce(
            topic,
            value=value.encode() if isinstance(value, str) else value,
            key=key.encode() if isinstance(key, str) else key,
        )
        producer.flush()
        return {"status": "success", "topic": topic}

    def consume(
        self,
        topic: str,
        group: str,
        max_messages: int = 10,
        timeout: float = 5.0,
    ) -> Any:
        try:
            from confluent_kafka import Consumer
        except ImportError as exc:
            raise RuntimeError(_NATIVE_HINT) from exc
        consumer = Consumer(
            self._conf(
                {
                    "group.id": group,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": True,
                }
            )
        )
        consumer.subscribe([topic])
        messages: list[dict[str, Any]] = []
        try:
            while len(messages) < max_messages:
                msg = consumer.poll(timeout)
                if msg is None:
                    break
                if msg.error():
                    messages.append({"error": str(msg.error())})
                    continue
                key = msg.key()
                value = msg.value()
                messages.append(
                    {
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "key": key.decode(errors="replace") if key else None,
                        "value": value.decode(errors="replace") if value else None,
                    }
                )
        finally:
            consumer.close()
        return {"messages": messages, "count": len(messages)}
