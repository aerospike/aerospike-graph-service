import os
from fastmcp import FastMCP
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.client import Client as GremlinClient
from loguru import logger
import sys


def check_aerospike_connection(g, url):
    try:
        g.inject(1).next()
        logger.log('INFO', "Successfully connected to AGS!")
    except:
        logger.exception("Could not connect to AGS at \"" + url + "\", check your container and make sure its running")
        raise RuntimeError("Could not connect to AGS; check that the service is running and GREMLIN_URL is reachable")

def start_mcp_server(run_server: bool = True) -> FastMCP:
    from graph_service_mcp.core_components import register_all

    mcp = FastMCP(
        "Aerospike Graph Service MCP Server",
        instructions='This server provides the ability to check connectivity, status and schema for working with Aerospike Graph Service.',
        mask_error_details=False)

    logger.remove()
    logger.add(
        sys.stderr,
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        filter=lambda record: "password" not in record["message"].lower() and "token" not in record["message"].lower()  # Don't log sensitive data
    )

    GREMLIN_URL = os.environ.get("GREMLIN_URL", "ws://localhost:8182/gremlin")
    GREMLIN_ALIAS = os.environ.get("GREMLIN_ALIAS", "g")
    if not GREMLIN_URL:
        raise ValueError("GREMLIN_URL environment variable is required")
    if not GREMLIN_URL.startswith(("ws://", "wss://")):
        raise ValueError("GREMLIN_URL must use ws:// or wss:// protocol")

    _ags_connection = DriverRemoteConnection(GREMLIN_URL, GREMLIN_ALIAS)
    g = traversal().with_remote(_ags_connection)
    mcp.app = {"g": g}
    client = GremlinClient(GREMLIN_URL, GREMLIN_ALIAS)

    check_aerospike_connection(g, GREMLIN_URL)

    register_all(mcp, g, client)


    if run_server:
        host = os.environ.get("HOST", "127.0.0.1")
        port = int(os.environ.get("PORT", "8000"))

        logger.info(f"Starting secure MCP server on {host}:{port}")

        mcp.run(
            transport="streamable-http",
            host=host,
            port=port,
            log_level=os.environ.get("LOG_LEVEL", "INFO")
        )
    return mcp


if __name__ == "__main__":
    mcp = start_mcp_server()