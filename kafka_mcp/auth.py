"""Identity credentials loader for the Apache Kafka clients."""

import os

from agent_utilities.base_utilities import get_logger, to_boolean

from kafka_mcp.api.api_client_native import NativeKafkaClient
from kafka_mcp.api_client import Api

logger = get_logger(__name__)


def get_client() -> Api:
    """Build an authenticated Kafka REST Proxy client from the environment.

    Honors ``KAFKA_REST_URL`` (default ``http://localhost:8082``); bearer token
    via ``KAFKA_TOKEN``; optional basic auth via
    ``KAFKA_USERNAME``/``KAFKA_PASSWORD``; TLS verification via
    ``KAFKA_SSL_VERIFY``.
    """
    base_url = os.getenv("KAFKA_REST_URL") or "http://localhost:8082"
    token = os.getenv("KAFKA_TOKEN", "")
    username = os.getenv("KAFKA_USERNAME", "")
    password = os.getenv("KAFKA_PASSWORD", "")
    verify = to_boolean(os.getenv("KAFKA_SSL_VERIFY", "True"))

    return Api(
        base_url=base_url,
        token=token or None,
        username=username or None,
        password=password or None,
        verify=verify,
    )


def get_native_client() -> NativeKafkaClient:
    """Build a native (direct-to-broker) Kafka client from the environment.

    Honors ``KAFKA_BOOTSTRAP_SERVERS`` (default ``localhost:9092``). Requires
    the optional ``kafka-mcp[native]`` extra at call time.
    """
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or "localhost:9092"
    return NativeKafkaClient(bootstrap_servers=bootstrap)
