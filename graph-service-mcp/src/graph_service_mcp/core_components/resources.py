# Copyright 2022-2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
