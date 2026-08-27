"""Main FastMCP server and tool registration for kafka-mcp."""

import sys
from typing import Any

from agent_utilities.core.config import load_config
from agent_utilities.mcp.server_factory import create_mcp_server
from agent_utilities.mcp.verbose_tools import register_tool_surface
from fastmcp.utilities.logging import get_logger
from starlette.requests import Request
from starlette.responses import JSONResponse

from kafka_mcp.api_client import Api
from kafka_mcp.auth import get_client, get_native_client
from kafka_mcp.mcp.mcp_kafka import register_kafka_tools
from kafka_mcp.mcp.mcp_kafka_cdc import register_kafka_cdc_tools
from kafka_mcp.mcp.mcp_kafka_connect import register_kafka_connect_tools

__version__ = "0.2.0"
logger = get_logger(name="kafka_mcp")

# Re-exported so ``register_tool_surface``'s module auto-discovery (which scans
# this namespace for ``register_<tag>_tools``) finds each domain registrar.
__all__ = [
    "get_mcp_instance",
    "mcp_server",
    "register_kafka_tools",
    "register_kafka_connect_tools",
    "register_kafka_cdc_tools",
]


def get_mcp_instance() -> tuple[Any, ...]:
    load_config()
    args, mcp, middlewares = create_mcp_server(
        name="Kafka MCP",
        version=__version__,
        instructions=(
            "Apache Kafka MCP Server - topics, records (produce/consume), "
            "consumer groups, brokers, ACLs via the Confluent REST Proxy, plus "
            "an optional native (direct-to-broker) client."
        ),
    )

    @mcp.custom_route("/health", methods=["GET"])
    async def health_check(request: Request) -> JSONResponse:
        """Liveness/readiness probe.

        Proves the process is up AND that it can actually reach a real
        Kafka broker — a bare ``{"status": "OK"}`` here previously reported
        healthy for weeks while ``KAFKA_URL`` (a dead key from an earlier
        refactor) meant the REST-proxy client never had a valid endpoint.
        Uses the native (direct-to-broker) client rather than the REST
        Proxy client, since no Confluent REST Proxy is deployed in this
        environment — only broker ports (a known, accepted gap, not what
        this probe is meant to catch). The ``kafka-mcp[native]`` extra is
        optional, so its absence degrades to "unverified", not "down".
        """
        try:
            import confluent_kafka.admin  # noqa: F401
        except ImportError as exc:
            return JSONResponse(
                {
                    "status": "OK",
                    "backend": "unverified",
                    "reason": f"native client extra not installed: {exc}",
                }
            )
        try:
            result = get_native_client().list_topics(timeout=2.0)
        except Exception as exc:  # noqa: BLE001 - report any backend failure as degraded
            return JSONResponse(
                {"status": "degraded", "backend": "unreachable", "error": str(exc)},
                status_code=503,
            )
        return JSONResponse(
            {
                "status": "OK",
                "backend": "reachable",
                "topics_visible": len(result.get("topics", [])),
            }
        )

    registered_tags = register_tool_surface(
        mcp,
        client_cls=Api,
        get_client=get_client,
        service="kafka-mcp",
        tools_module=sys.modules[__name__],
    )
    logger.info("Registered condensed tool surfaces: count=%d", len(registered_tags))

    for mw in middlewares:
        mcp.add_middleware(mw)
    return mcp, args, middlewares


def mcp_server() -> None:
    mcp, args, middlewares = get_mcp_instance()
    print(f"Kafka MCP v{__version__}", file=sys.stderr)
    if args.transport == "stdio":
        mcp.run(transport="stdio")
    elif args.transport == "streamable-http":
        mcp.run(transport="streamable-http", host=args.host, port=args.port)
    elif args.transport == "sse":
        mcp.run(transport="sse", host=args.host, port=args.port)
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    mcp_server()
