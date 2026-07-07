import re
from loguru import logger
from typing import Any, Dict, List
import anyio
from gremlin_python.driver.client import Client as GremlinClient
from gremlin_python.structure.graph import Vertex, Edge, Property, Path

_MUTATING_PATTERNS = [
    r"\baddV\s*\(",
    r"\baddE\s*\(",
    r"\bmergeV\s*\(",
    r"\bmergeE\s*\(",
    r"\bproperty\s*\(",
    r"\bdrop\s*\(",
    r"\baddProperty\s*\(",
    r"\bremove\w*\s*\(",
    r"\bclear\s*\(",
    r"\btx\s*\(",
]

_MUTATING_RE = re.compile("|".join(_MUTATING_PATTERNS), flags=re.IGNORECASE)


def is_mutating_query(query: str) -> bool:
    return bool(_MUTATING_RE.search(query))


def serialize_gremlin_objects(obj):
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj

    # Handle Gremlin-specific objects that FastMCP can't serialize
    if isinstance(obj, Vertex):
        vertex_data = {
            "type": "vertex",
            "id": getattr(obj, "id", None),
            "label": getattr(obj, "label", None)
        }
        # Add properties if they exist
        try:
            properties = {}
            # vertex.properties is a list of VertexProperty objects
            for prop in obj.properties:
                key = getattr(prop, "key", None)
                value = getattr(prop, "value", None)
                if key is not None:
                    properties[key] = value
            if properties:
                vertex_data["properties"] = properties
        except Exception as e:
            # If properties access fails, log and continue
            pass
        return vertex_data
    elif isinstance(obj, Edge):
        edge_data = {
            "type": "edge",
            "id": getattr(obj, "id", None),
            "label": getattr(obj, "label", None),
            "outV": getattr(obj, "outV", None),
            "inV": getattr(obj, "inV", None),
        }
        # Add properties if they exist
        try:
            properties = {}
            # edge.properties is a list of Property objects
            for prop in obj.properties:
                key = getattr(prop, "key", None)
                value = getattr(prop, "value", None)
                if key is not None:
                    properties[key] = value
            if properties:
                edge_data["properties"] = properties
        except Exception as e:
            # If properties access fails, log and continue
            pass
        return edge_data
    elif isinstance(obj, Property):
        return {
            "type": "property",
            "key": getattr(obj, "key", None),
            "value": getattr(obj, "value", None)
        }
    elif isinstance(obj, Path):
        return {
            "type": "path",
            "labels": [list(s) for s in getattr(obj, "labels", [])],
            "objects": [serialize_gremlin_objects(x) for x in getattr(obj, "objects", [])]
        }
    elif isinstance(obj, list):
        return [serialize_gremlin_objects(x) for x in obj]
    elif isinstance(obj, dict):
        return {str(k): serialize_gremlin_objects(v) for k, v in obj.items()}

    # For everything else, let FastMCP handle it
    return obj


async def _submit(client: GremlinClient, query: str, bindings: Dict[str, Any] | None, timeout_ms: int) -> Dict:
    """Run a Gremlin string query using gremlin_python Client in a worker thread.
    Returns native Python objects and lets FastMCP handle serialization.
    g.V().limit(1).toList()
    """

    def _run():
        logger.log("INFO", query)
        try:
            rs = client.submit(query, bindings or {})
            return serialize_gremlin_objects(rs)
        except Exception as e:
            # Provide helpful error messages for common serialization issues
            error_msg = str(e)
            if "Unable to serialize unknown type" in error_msg and "gremlin_python.structure" in error_msg:
                raise Exception(f"Serialization error: Query returned Gremlin objects that couldn't be serialized. "
                              f"Try ending your mutation query with .iterate() to avoid returning objects. "
                              f"Original error: {error_msg}")
            raise

    with anyio.move_on_after(timeout_ms / 1000.0) as scope:
        res = await anyio.to_thread.run_sync(_run)
        if scope.cancel_called:
            raise TimeoutError(f"gremlin query timed out after {timeout_ms}ms")
        return {"results": res}
    return {}


def append_profile_if_missing(query: str) -> str:
    q = query.strip()
    if re.search(r"\.profile\s*\(\s*\)\s*$", q):
        return q
    return f"{q}.profile()"


def _guess_ms(duration_value: float | int) -> float:
    d = float(duration_value)
    if d <= 0:
        return 0.0
    return d / 1e6


def _flatten_metrics(metric: Dict[str, Any], bucket: List[Dict[str, Any]]) -> None:
    dur = metric.get("dur", metric.get("duration", metric.get("time", 0)))
    name = metric.get("name") or metric.get("annotation") or metric.get("step", "Overall Query")
    count = metric.get("count", metric.get("calls", metric.get("n", 0)))
    bucket.append({
        "name": str(name),
        "dur_ms": _guess_ms(dur),
        "raw_duration": dur,
        "count": int(count) if isinstance(count, (int, float)) else 0,
    })
    for child in metric.get("metrics", []) or metric.get("children", []) or []:
        if isinstance(child, dict):
            _flatten_metrics(child, bucket)


def _convert_results_to_list(results) -> list:
    """Convert various result types to a list for consistent handling."""
    if hasattr(results, '__iter__') and not isinstance(results, (str, dict)):
        # Convert ResultSet or other iterable to list and serialize each item
        return [serialize_gremlin_objects(item) for item in results]
    elif isinstance(results, list):
        return [serialize_gremlin_objects(item) for item in results]
    else:
        return [serialize_gremlin_objects(results)] if results is not None else []
