# Graph MCP

An MCP (Model Context Protocol) server for [Aerospike Graph Service](https://aerospike.com/products/graph-database/), enabling AI assistants like Cursor, Claude Desktop, and other MCP-compatible clients to interact with graph databases using natural language.

## Overview

This project provides a bridge between LLM-powered tools and Aerospike Graph Service, allowing you to:

- **Query graphs conversationally** — Ask questions about your data in plain English
- **Execute Gremlin traversals** — Run read and write queries with safety guardrails
- **Inspect schema and metadata** — Understand your graph's structure automatically
- **Profile and optimize queries** — Get performance insights for slow traversals

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Cursor / AI   │────▶│   MCP Server    │────▶│ Aerospike Graph │
│     Client      │ MCP │  (FastMCP)      │ WS  │    Service      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                              │                        │
                              │                        ▼
                              │                 ┌─────────────────┐
                              │                 │  Aerospike DB   │
                              │                 └─────────────────┘
                              ▼
                        HTTP :8000/mcp
```

## Features

### Tools

| Tool | Description |
|------|-------------|
| `get_schema` | Get graph schema via AGS summary API — vertex/edge labels, properties by label, counts, and supernode info |
| `read_gremlin` | Execute read-only Gremlin queries with mutation blocking |
| `write_gremlin` | Execute mutating Gremlin queries (addV, addE, drop, etc.) |
| `profile_gremlin` | Profile query performance with step-by-step timing breakdown |

### Resources

| Resource | Description |
|----------|-------------|
| `ags://metadata` | Graph summary with vertex, edge, and supernode counts |
| `ags://config_data` | Server and graph configuration snapshot |
| `ags://version_data` | Aerospike DB, Graph Service, and Gremlin versions |
| `ags://index_cardinality` | Distinct value counts for secondary indexes |

### Prompts

| Prompt | Description |
|--------|-------------|
| `optimize_gremlin` | Iterative query performance tuning with profiling |

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+ (for local development)
- An MCP-compatible client (Cursor, Claude Desktop, etc.)

### 1. Start the Stack

```bash
cd graph-service-mcp
docker compose up -d
```

This launches:
- **Aerospike Database** on ports `3000-3002`
- **Aerospike Graph Service** on port `8182` (Gremlin WebSocket)
- **MCP Server** on port `8000` (HTTP)

### 2. Load Sample Data (Optional)

Load the included air-routes sample dataset (run locally):

```bash
cd graph-service-mcp
pip install gremlinpython
python bulkload.py
```

You'll see a summary when complete:
```
============================================================
GRAPH SUMMARY
============================================================
  Total vertex count: 3749
  Vertex count by label: {continent=7, country=237, version=1, airport=3504}
  Total edge count: 57645
  Edge count by label: {contains=7008, route=50637}
============================================================
```

Or use your own data by placing CSV files in:
- `data/vertices/` — Vertex data
- `data/edges/` — Edge data

### 3. Configure Your MCP Client

#### Cursor

Create `.cursor/mcp.json` in your project:

```json
{
  "mcpServers": {
    "aerospike-graph": {
      "url": "http://localhost:8000/mcp"
    }
  }
}
```

Then enable the server in **Cursor Settings → MCP**. A green indicator means you're connected!

#### Claude Desktop

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "aerospike-graph": {
      "url": "http://localhost:8000/mcp"
    }
  }
}
```

### 4. Start Querying

Once connected, try prompts like:

- *"What's the schema of my graph?"*
- *"Show me all airports in California"*
- *"What's the longest flight route by distance?"*
- *"Which airports have the most connections?"*

## Example Gremlin Queries

Here are some queries that work with the air-routes sample data:

```gremlin
// Get the 10 busiest airports by number of routes
g.V().hasLabel('airport').order().by(out('route').count(), desc).limit(10)
  .project('code', 'city', 'routes')
  .by('code').by('city').by(out('route').count())

// Find all direct flights from SFO
g.V().has('airport', 'code', 'SFO').out('route').values('code', 'city')

// Longest routes by distance
g.E().hasLabel('route').order().by('dist', desc).limit(10)
  .project('from', 'to', 'distance')
  .by(outV().values('code'))
  .by(inV().values('code'))
  .by('dist')

// Airports by country (top 10)
g.V().hasLabel('airport').groupCount().by('country')
  .order(local).by(values, desc).limit(local, 10)

// Airports above 10,000 feet elevation
g.V().hasLabel('airport').has('elev', gt(10000))
  .project('code', 'city', 'elevation')
  .by('code').by('city').by('elev')

// Path between two airports (up to 2 hops)
g.V().has('code', 'SFO')
  .repeat(out('route').simplePath()).until(has('code', 'JFK').or().loops().is(2))
  .has('code', 'JFK')
  .path().by('code').limit(5)
```


### Docker Compose Customization

Override defaults in `docker-compose.yaml` or use a `.env` file:

```bash
# .env
GREMLIN_URL=ws://custom-host:8182/gremlin
LOG_LEVEL=DEBUG
```

## Development

### Local Setup

```bash
cd graph-service-mcp
python -m venv .venv
source .venv/bin/activate
pip install -e ".[test]"
```

### Running Tests

```bash
pytest tests/ -v
```

### Building the Docker Image

```bash
docker build -t aerospike-graph-mcp:latest .
```

## Project Structure

```
graph-service-mcp/
├── README.md                    # This file
├── docker-compose.yaml          # Full stack orchestration
├── Dockerfile                   # MCP server container
├── pyproject.toml               # Python dependencies
├── bulkload.py                  # Sample data loader
├── data/                        # Sample CSV data
│   ├── vertices/
│   └── edges/
├── src/graph_service_mcp/
│   ├── server.py                # MCP server entrypoint
│   ├── core_components/
│   │   ├── gremlin.py           # Gremlin tools
│   │   ├── resources.py         # AGS resources
│   │   └── prompts.py           # Optimization prompts
│   ├── util/                    # Helpers
│   └── assets/prompts/          # Prompt templates
└── tests/
```

## Sample Data

The included sample data is based on the [air-routes dataset](https://github.com/krlawrence/graph/tree/main/sample-data) — a graph of worldwide airports and flight routes, perfect for exploring graph queries.

## Testing the MCP Endpoint

### Quick Health Check

```bash
# Verify the server is responding
curl http://localhost:8000/mcp
# Should return: {"name":"Aerospike Graph Service MCP Server",...}
```

### Testing with Cursor (Recommended)

The easiest way to test is directly in Cursor:

1. Ensure `.cursor/mcp.json` is configured (see above)
2. Open **Cursor Settings → MCP** — verify green status for `aerospike-graph`
3. In Agent mode, ask: *"Use the get_schema tool to show the graph schema"*

### Testing with Python

```python
import httpx

# Initialize session
with httpx.Client() as client:
    # The MCP endpoint handles session management automatically
    # when accessed through proper MCP clients like Cursor
    response = client.get("http://localhost:8000/mcp")
    print(response.json())
```

## Troubleshooting

### MCP Server Won't Connect

1. Check that all containers are running: `docker compose ps`
2. Verify the Gremlin connection: `docker compose logs aerospike-graph-mcp`
3. Ensure port 8000 isn't blocked by a firewall

### Queries Timeout

- Increase `timeout_ms` parameter in your queries
- Use `.profile()` to identify slow traversal steps
- Check `ags://index_cardinality` to verify indexes exist

### Container Health Issues

```bash
# Restart the stack
docker compose down && docker compose up -d

# Check logs
docker compose logs -f
```

## License

See individual component licenses:
- [Aerospike Graph Service](https://aerospike.com/products/graph-database/)
- [FastMCP](https://github.com/jlowin/fastmcp)

## Resources

- [Aerospike Graph Documentation](https://docs.aerospike.com/graph)
- [Apache TinkerPop / Gremlin](https://tinkerpop.apache.org/)
- [Model Context Protocol](https://modelcontextprotocol.io/)
- [Air Routes Sample Data](https://github.com/krlawrence/graph/tree/main/sample-data)
