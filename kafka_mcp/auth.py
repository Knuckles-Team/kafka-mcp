"""Identity credentials loader for the Apache Kafka clients."""

from agent_utilities.base_utilities import get_logger
from agent_utilities.core.config import setting

from kafka_mcp.api.api_client_connect import ConnectApi
from kafka_mcp.api.api_client_native import NativeKafkaClient
from kafka_mcp.api_client import Api

logger = get_logger(__name__)


def get_client() -> Api:
    """Build an authenticated Kafka REST Proxy client from the environment.

    Honors ``KAFKA_REST_URL`` (default ``http://localhost:8082``); bearer token
    via ``KAFKA_TOKEN``; optional basic auth via
    ``KAFKA_USERNAME``/``KAFKA_PASSWORD``; TLS trust through the shared
    mandatory-verification profile contract.
    """
    base_url = setting("KAFKA_REST_URL", "http://localhost:8082")
    token = setting("KAFKA_TOKEN", "")
    username = setting("KAFKA_USERNAME", "")
    password = setting("KAFKA_PASSWORD", "")
    tls_profile = setting("KAFKA_REST_TLS_PROFILE", "")
    tls_profile_ref = setting("KAFKA_REST_TLS_PROFILE_REF", "")

    return Api(
        base_url=base_url,
        token=token or None,
        username=username or None,
        password=password or None,
        tls_profile=tls_profile or None,
        tls_profile_ref=tls_profile_ref or None,
    )


def get_native_client() -> NativeKafkaClient:
    """Build a native (direct-to-broker) Kafka client from the environment.

    Honors ``KAFKA_BOOTSTRAP_SERVERS`` (default ``localhost:9092``). Requires
    the optional ``kafka-mcp[native]`` extra at call time.
    """
    bootstrap = setting("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return NativeKafkaClient(bootstrap_servers=bootstrap)


def get_connect_client() -> ConnectApi:
    """Build a Kafka Connect REST client from the environment.

    Honors ``KAFKA_CONNECT_URL`` (default ``http://localhost:8083``); bearer
    token via ``KAFKA_CONNECT_TOKEN``; optional basic auth via
    ``KAFKA_CONNECT_USERNAME``/``KAFKA_CONNECT_PASSWORD``; TLS trust through the
    shared mandatory-verification profile contract
    (``KAFKA_CONNECT_TLS_PROFILE``/``KAFKA_CONNECT_TLS_PROFILE_REF``).

    Kafka Connect's own REST API has no documented built-in auth as of this
    writing (CA-51 deployed it unauthenticated, in-cluster only). When none of
    the credential vars are set, this client is deliberately unauthenticated —
    logged loudly rather than silently, so an operator notices before assuming
    a credential was honored.
    """
    base_url = setting("KAFKA_CONNECT_URL", "http://localhost:8083")
    token = setting("KAFKA_CONNECT_TOKEN", "")
    username = setting("KAFKA_CONNECT_USERNAME", "")
    password = setting("KAFKA_CONNECT_PASSWORD", "")
    tls_profile = setting("KAFKA_CONNECT_TLS_PROFILE", "")
    tls_profile_ref = setting("KAFKA_CONNECT_TLS_PROFILE_REF", "")

    if not (token or (username and password)):
        logger.warning(
            "kafka-mcp: KAFKA_CONNECT_URL=%s has no credential configured "
            "(KAFKA_CONNECT_TOKEN / KAFKA_CONNECT_USERNAME+PASSWORD) — "
            "connecting UNAUTHENTICATED. Kafka Connect's REST API has no "
            "documented built-in auth; this is expected only behind a "
            "trusted network boundary (e.g. in-cluster).",
            base_url,
        )

    return ConnectApi(
        base_url=base_url,
        token=token or None,
        username=username or None,
        password=password or None,
        tls_profile=tls_profile or None,
        tls_profile_ref=tls_profile_ref or None,
    )
