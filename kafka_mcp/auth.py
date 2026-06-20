"""Identity credentials loader for the Apache Kafka clients."""

from agent_utilities.base_utilities import get_logger
from agent_utilities.core.config import setting

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
    base_url = setting("KAFKA_REST_URL", "http://localhost:8082")
    token = setting("KAFKA_TOKEN", "")
    username = setting("KAFKA_USERNAME", "")
    password = setting("KAFKA_PASSWORD", "")
    verify = setting("KAFKA_SSL_VERIFY", True)

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
    bootstrap = setting("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return NativeKafkaClient(bootstrap_servers=bootstrap)
