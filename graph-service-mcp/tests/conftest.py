"""
Pytest fixtures for MCP server testing.

Requires Aerospike Graph Service running at ws://localhost:8182/gremlin
Start with: docker compose up -d
"""
import os
import pytest
from fastmcp import FastMCP

# Set environment variables before importing server
os.environ.setdefault("GREMLIN_URL", "ws://localhost:8182/gremlin")
os.environ.setdefault("GREMLIN_ALIAS", "g")


@pytest.fixture(scope="session")
def mcp() -> FastMCP:
    """Create MCP server instance without starting the HTTP server."""
    from graph_service_mcp.server import start_mcp_server
    server = start_mcp_server(run_server=False)
    server.enable(names={"write_gremlin_test"})
    return server


@pytest.fixture(scope="session")
def init_data(mcp: FastMCP):
    """
    Ensure test data exists in the graph.
    Creates a simple test vertex if the graph is empty.
    """
    import json
    import asyncio
    
    async def setup():
        tool = await mcp.get_tool("write_gremlin_test")
        
        # Check if graph has data
        read_tool = await mcp.get_tool("read_gremlin")
        response = await read_tool.run(dict(query="g.V().limit(1).count()"))
        result = json.loads(response.content[0].text)
        
        if result.get("results", [0])[0] == 0:
            # Add test vertex if graph is empty
            await tool.run(dict(
                query='g.addV("TestVertex").property("name", "test").iterate()'
            ))
        
        return True
    
    return asyncio.run(setup())
