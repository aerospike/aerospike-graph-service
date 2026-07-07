from __future__ import annotations
import anyio
from fastmcp import FastMCP
from gremlin_python.process.graph_traversal import GraphTraversalSource
from .prompts import get_prompt_map


def register(mcp: FastMCP, g: GraphTraversalSource, *_deps) -> None:
    def _run(call: str):
        return g.call(call).next()

    @mcp.resource(
        "ags://metadata",
        name="AGS Metadata",
        description="Returns graph summary with counts for vertices, edges, and supernodes.",
        mime_type="application/json",
    )
    async def get_metadata() -> str:
        return await anyio.to_thread.run_sync(_run, "aerospike.graph.admin.metadata.summary")

    @mcp.resource(
        "ags://config_data",
        name="AGS Config Data",
        description="Returns a snapshot of server and graph configuration.",
        mime_type="application/json",
    )
    async def get_config_data() -> str:
        return await anyio.to_thread.run_sync(_run, "aerospike.graph.admin.metadata.config")

    @mcp.resource(
        "ags://version_data",
        name="AGS Version Data",
        description="Returns Aerospike Database, Aerospike Graph, and Gremlin versions.",
        mime_type="application/json",
    )
    async def get_version_data() -> str:
        return await anyio.to_thread.run_sync(_run, "aerospike.graph.admin.metadata.version")

    @mcp.resource(
        "ags://index_cardinality",
        name="AGS Index Cardinality",
        description="Returns distinct value counts for all existing secondary indexes.",
        mime_type="application/json",
    )
    async def get_index_cardinality() -> str:
        return await anyio.to_thread.run_sync(_run, "aerospike.graph.admin.index.cardinality")
