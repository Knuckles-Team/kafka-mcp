"""Public client facade for kafka_mcp."""

from kafka_mcp.api.api_client_kafka import KafkaApi

__version__ = "0.2.0"


class Api(KafkaApi):
    """Authenticated Apache Kafka (Confluent REST Proxy) client."""

    pass
