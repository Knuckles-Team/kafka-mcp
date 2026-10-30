"""Public client facade for kafka_mcp."""

from scion_sdk.connector_identity import (
    connector_version as _connector_version,
)

from kafka_mcp.api.api_client_kafka import KafkaApi

__version__ = _connector_version("kafka_mcp")


class Api(KafkaApi):
    """Authenticated Apache Kafka (Confluent REST Proxy) client."""

    pass
