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

import json
from typing import Any
import re
import pytest
from fastmcp import FastMCP


@pytest.mark.asyncio(loop_scope="function")
async def test_op_write_gremlin(mcp: FastMCP):
    query = "g.addV(\"Person\").property(\"name\", \"Greg\").next()"
    tool = await mcp.get_tool("write_gremlin_test")
    response = await tool.run(dict(query=query))

    result = json.loads(response.content[0].text)

    # Check that we get a result indicating mutation
    assert "mutating" in result
    assert result["mutating"] == True
    assert "results" in result
    assert "count" in result


@pytest.mark.asyncio(loop_scope="function")
async def test_get_schema(mcp: FastMCP, init_data: Any):
    tool = await mcp.get_tool("get_schema")
    response = await tool.run(dict(include_counts=True))
    schema = json.loads(response.content[0].text)

    # Verify the schema result structure (using AGS summary API)
    assert "vertexLabels" in schema
    assert "edgeLabels" in schema
    assert isinstance(schema["vertexLabels"], list)
    assert isinstance(schema["edgeLabels"], list)
    
    # New summary API provides properties by label and counts
    assert "vertexPropertiesByLabel" in schema
    assert "edgePropertiesByLabel" in schema
    assert "vertexCountByLabel" in schema
    assert "edgeCountByLabel" in schema


@pytest.mark.asyncio(loop_scope="function")
async def test_get_schema_without_counts(mcp: FastMCP, init_data: Any):
    tool = await mcp.get_tool("get_schema")
    response = await tool.run(dict(include_counts=False))
    schema = json.loads(response.content[0].text)

    # Without counts, should still have labels and properties
    assert "vertexLabels" in schema
    assert "edgeLabels" in schema
    assert "vertexPropertiesByLabel" in schema
    assert "edgePropertiesByLabel" in schema
    
    # Counts should NOT be present
    assert "vertexCountByLabel" not in schema
    assert "edgeCountByLabel" not in schema
    assert "totalVertexCount" not in schema


@pytest.mark.asyncio(loop_scope="function")
async def test_no_op_write_gremlin(mcp: FastMCP):
    query = "g.V().limit(1).toList()"
    tool = await mcp.get_tool("write_gremlin_test")
    response = await tool.run(dict(query=query))
    result = json.loads(response.content[0].text)

    # Check that we get a result for a non-mutating query
    assert "mutating" in result
    assert result["mutating"] == False
    assert "results" in result
    assert "count" in result


@pytest.mark.asyncio(loop_scope="function")
async def test_read_gremlin(mcp: FastMCP, init_data: Any):
    query = "g.V().limit(1).has(\"name\")"

    tool = await mcp.get_tool("read_gremlin")
    response = await tool.run(dict(query=query))

    result = json.loads(response.content[0].text)

    # Check that we get a result structure
    assert "results" in result
    assert "count" in result
    assert isinstance(result["results"], list)


@pytest.mark.asyncio(loop_scope="function")
async def test_profile_gremlin(mcp: FastMCP, init_data: Any):
    query = "g.V().limit(1)"

    tool = await mcp.get_tool("profile_gremlin")
    response = await tool.run(dict(query=query))

    result = json.loads(response.content[0].text)

    # Check that we get a simplified profile result
    assert "query" in result
    assert "metrics" in result
    assert "total_steps" in result
    assert "total_duration_ms" in result
    assert isinstance(result["metrics"], list)