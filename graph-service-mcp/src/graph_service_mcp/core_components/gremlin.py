from __future__ import annotations

from loguru import logger
from typing import Any, Dict

import anyio
from gremlin_python.process.graph_traversal import GraphTraversalSource
from gremlin_python.driver.client import Client as GremlinClient
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

from graph_service_mcp.util import is_mutating_query, _submit
from graph_service_mcp.util.gremlin import _convert_results_to_list, append_profile_if_missing, _flatten_metrics

_SUMMARY_KEY_MAP = {
    "Total vertex count": "totalVertexCount",
    "Vertex count by label": "vertexCountByLabel",
    "Vertex properties by label": "vertexPropertiesByLabel",
    "Total edge count": "totalEdgeCount",
    "Edge count by label": "edgeCountByLabel",
    "Edge properties by label": "edgePropertiesByLabel",
    "Total supernode count": "totalSupernodeCount",
    "Supernode count by label": "supernodeCountByLabel",
}


def _normalize_summary_dict(summary: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize AGS summary dict keys to the camelCase schema shape."""
    result: Dict[str, Any] = {}
    for key, value in summary.items():
        normalized_key = _SUMMARY_KEY_MAP.get(key, key)
        if normalized_key.endswith("PropertiesByLabel") and isinstance(value, dict):
            value = {
                label: sorted(props) if isinstance(props, set) else props
                for label, props in value.items()
            }
        result[normalized_key] = value
    return result


def register(mcp: FastMCP, g: GraphTraversalSource, client: GremlinClient) -> None:
    @mcp.tool(name="get_schema",
              description="Get graph schema and summary using the AGS metadata API. Returns vertex/edge labels, properties by label, counts, and supernode information.")
    async def get_schema(include_counts: bool = False) -> Dict[str, Any]:
        """
        Uses the aerospike.graph.admin.metadata.summary API for efficient schema retrieval.
        See: https://aerospike.com/docs/graph/manage/summary/
        """
        def _run() -> Dict[str, Any]:
            # Use the AGS summary API - much more efficient than scanning the graph
            summary = g.call("aerospike.graph.admin.metadata.summary").next()
            
            # Parse the summary string output into structured data
            result: Dict[str, Any] = {"raw_summary": summary}
            
            # If summary is already a dict, normalize AGS key names
            if isinstance(summary, dict):
                result = _normalize_summary_dict(summary)
            elif isinstance(summary, str):
                # Parse the string format returned by AGS
                lines = summary.strip().split('\n')
                for line in lines:
                    line = line.strip()
                    if line.startswith('Total vertex count='):
                        result['totalVertexCount'] = int(line.split('=')[1])
                    elif line.startswith('Vertex count by label='):
                        result['vertexCountByLabel'] = _parse_dict(line.split('=', 1)[1])
                    elif line.startswith('Vertex properties by label='):
                        result['vertexPropertiesByLabel'] = _parse_dict(line.split('=', 1)[1])
                    elif line.startswith('Total edge count='):
                        result['totalEdgeCount'] = int(line.split('=')[1])
                    elif line.startswith('Edge count by label='):
                        result['edgeCountByLabel'] = _parse_dict(line.split('=', 1)[1])
                    elif line.startswith('Edge properties by label='):
                        result['edgePropertiesByLabel'] = _parse_dict(line.split('=', 1)[1])
                    elif line.startswith('Total supernode count='):
                        result['totalSupernodeCount'] = int(line.split('=')[1])
                    elif line.startswith('Supernode count by label='):
                        result['supernodeCountByLabel'] = _parse_dict(line.split('=', 1)[1])
            
            # Extract just labels and properties for simplified view
            result['vertexLabels'] = list(result.get('vertexCountByLabel', {}).keys())
            result['edgeLabels'] = list(result.get('edgeCountByLabel', {}).keys())
            
            # If counts not requested, remove count fields for cleaner output
            if not include_counts:
                result.pop('totalVertexCount', None)
                result.pop('vertexCountByLabel', None)
                result.pop('totalEdgeCount', None)
                result.pop('edgeCountByLabel', None)
                result.pop('totalSupernodeCount', None)
                result.pop('supernodeCountByLabel', None)
                result.pop('raw_summary', None)
            
            return result

        def _parse_dict(s: str) -> Dict[str, Any]:
            """Parse AGS dict format like {Label1=100, Label2=200} or {Label1=[prop1, prop2]}"""
            s = s.strip()
            if s.startswith('{') and s.endswith('}'):
                s = s[1:-1]
            result = {}
            if not s:
                return result
            # Handle nested brackets for property lists
            depth = 0
            current_key = ""
            current_value = ""
            in_value = False
            for char in s:
                if char == '[':
                    depth += 1
                    current_value += char
                elif char == ']':
                    depth -= 1
                    current_value += char
                elif char == '=' and depth == 0 and not in_value:
                    in_value = True
                elif char == ',' and depth == 0:
                    if current_key:
                        result[current_key.strip()] = _parse_value(current_value.strip())
                    current_key = ""
                    current_value = ""
                    in_value = False
                elif in_value:
                    current_value += char
                else:
                    current_key += char
            if current_key:
                result[current_key.strip()] = _parse_value(current_value.strip())
            return result

        def _parse_value(s: str) -> Any:
            """Parse a value - could be int, list, or string"""
            s = s.strip()
            if s.startswith('[') and s.endswith(']'):
                # Parse as list
                inner = s[1:-1].strip()
                if not inner:
                    return []
                return [item.strip() for item in inner.split(',')]
            try:
                return int(s)
            except ValueError:
                return s

        return await anyio.to_thread.run_sync(_run)

    @mcp.tool(name="read_gremlin",
              description="Execute a read-only Gremlin query string; blocks mutations.")
    async def read_gremlin(query: str, bindings: Dict[str, Any] | None = None, timeout_ms: int = 15000) -> Dict[
        str, Any]:
        q = query.strip()
        if is_mutating_query(q):
            raise ToolError(
                "Error, the client tried a Mutating query using the read only tool. Please use write_gremlin")
        data = await _submit(client, q, bindings, timeout_ms)

        # Simplify the output structure
        results_list = _convert_results_to_list(data.get("results", []))

        return {
            "query": q,
            "results": results_list,
            "count": len(results_list)
        }

    @mcp.tool(name="write_gremlin",
              description="""Execute a (possibly mutating) Gremlin query.

IMPORTANT USAGE NOTES:
- For mutations that don't need to return results, end your query with .iterate() to avoid serialization issues
- Examples:
  ✅ Good: g.addV('person').property('name', 'Alice').iterate()
  ✅ Good: g.V().has('name', 'Alice').drop().iterate()
  ❌ Avoid: g.addV('person').property('name', 'Alice') (returns vertex object)
  ❌ Avoid: g.V().has('name', 'Alice').as('a').V().has('name', 'Bob').addE('knows').from('a') (returns edge object)
- If you need the created object details, use separate read queries after the mutation
- The tool automatically serializes returned vertices/edges, but .iterate() is more efficient for pure mutations""")
    async def write_gremlin(query: str, bindings: Dict[str, Any] | None = None,
                            timeout_ms: int = 15000) -> Dict[str, Any]:
        q = query.strip()
        mutation = is_mutating_query(q)

        try:
            data = await _submit(client, q, bindings, timeout_ms)

            # Simplify the output structure
            results_list = _convert_results_to_list(data.get("results", []))

            return {
                "query": q,
                "mutating": mutation,
                "results": results_list,
                "count": len(results_list)
            }
        except TimeoutError as te:
            return {"error": "timeout", "message": str(te)}
        except Exception as e:
            return {"error": "execution_error", "message": str(e)}

    # Test version without Context dependency
    @mcp.tool(name="write_gremlin_test",
              description="Execute a (possibly mutating) Gremlin query for testing. No elicitation.")
    async def write_gremlin_test(query: str, bindings: Dict[str, Any] | None = None, timeout_ms: int = 15000) -> Dict[
        str, Any]:
        q = query.strip()
        mutation = is_mutating_query(q)

        try:
            data = await _submit(client, q, bindings, timeout_ms)

            # Simplify the output structure
            results_list = _convert_results_to_list(data.get("results", []))

            return {
                "query": q,
                "mutating": mutation,
                "results": results_list,
                "count": len(results_list)
            }
        except TimeoutError as te:
            return {"error": "timeout", "message": str(te)}
        except Exception as e:
            return {"error": "execution_error", "message": str(e)}

    @mcp.tool(name="profile_gremlin",
              description="Profile a read-only Gremlin query: appends .profile(), blocks mutations, returns sorted step timings.")
    async def profile_gremlin(query: str, bindings: Dict[str, Any] | None = None, timeout_ms: int = 30000) -> Dict[
        str, Any]:
        base = query.strip()
        if is_mutating_query(base):
            raise ToolError("Tried running a mutating query on profile_gremlin")
        profiled = append_profile_if_missing(base)
        try:
            res = await _submit(client, profiled, bindings, timeout_ms)
            logger.log("INFO", f"Profile result: {res}")

            # Extract and flatten the profile metrics
            flattened_metrics = []
            if "results" in res and res["results"]:
                # Handle the nested structure: results -> list -> list -> dict with metrics
                for result_set in res["results"]:
                    if isinstance(result_set, list):
                        for result in result_set:
                            if isinstance(result, dict) and "metrics" in result:
                                _flatten_metrics(result, flattened_metrics)

            # Sort by duration (descending) to show slowest operations first
            flattened_metrics.sort(key=lambda x: x["dur_ms"], reverse=True)

            return {
                "query": profiled,
                "metrics": flattened_metrics,
                "total_steps": len(flattened_metrics),
                "total_duration_ms": sum(m["dur_ms"] for m in flattened_metrics)
            }
        except TimeoutError as te:
            return {"error": "timeout", "message": str(te)}
        except Exception as e:
            logger.log("ERROR", f"Profile error: {e}")
            return {"error": "execution_error", "message": str(e)}

    mcp.disable(names={"write_gremlin_test"})
