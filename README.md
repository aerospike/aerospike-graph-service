# Aerospike Graph Service

> An [Apache TinkerPop][tinkerpop]-compatible graph database engine backed
> by [Aerospike][aerospike]. Designed for low-latency graph traversals at
> scale — terabytes of vertices and edges, thousands of concurrent
> queries, single-digit-millisecond p99 — while keeping a plain,
> Gremlin-standard interface on the wire.

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![TinkerPop](https://img.shields.io/badge/TinkerPop-3.7.x-orange.svg)](https://tinkerpop.apache.org)
[![Java](https://img.shields.io/badge/Java-11%2B-green.svg)](https://adoptium.net)

---

## What is Aerospike Graph Service

Aerospike Graph Service is a JVM process that speaks the standard
TinkerPop Gremlin wire protocol over WebSocket and stores its data in
an Aerospike cluster. Use any Gremlin driver variant
(`gremlin-python`, `gremlin-javascript`, `tinkerpop-client` in Java,
etc.), issue normal Gremlin traversals and the
service translates them into efficient Aerospike operations, applies
query-planning optimizations specific to Aerospike's data model, and
streams results back to the driver.

**Why another graph database?**

- **Aerospike as the storage tier**, inherit Aerospike's strong
  consistency, predictable sub-millisecond reads, horizontal scale,
  hybrid-memory architecture, and cross-datacenter replication. Graph
  state is just another flavor of workload on top of a high performance, battle-tested
  KV store.
- **Standard Gremlin on the wire.** Anything that speaks TinkerPop 3.7.x works. No bespoke query language, no
  custom drivers. 3.8 is not supported yet due to breaking changes in that line.
- **Aerospike-native data layout.** Vertices and edges are stored in
  a `packed` on-disk layout tuned for Aerospike's record structure,
  keeping adjacency information co-resident with the vertex so that
  most traversal hops resolve in a single Aerospike read. See
  [`docs/DATA_MODEL_DESIGN.md`](docs/DATA_MODEL_DESIGN.md).
- **Scale-out OLAP.** The Spark-backed OLAP module lets you run
  `GraphComputer` jobs (e.g. PageRank, connected components, custom
  VertexPrograms) over the full graph without holding it in a single
  JVM.
- **Bulk loading.** A Spark-backed bulk loader ingests CSV / GraphML /
  GraphSON vertex and edge files directly into the Aerospike data
  model, bypassing the query path for order-of-magnitude faster
  initial loads.

## Quickstart

```
┌──────────────┐    Gremlin (ws://host:8182)    ┌─────────────────────┐    Aerospike    ┌─────────────────┐
│  Your app    │  ───────────────────────────>  │  Graph Service      │  ───────────>   │  Aerospike      │
│  (any lang)  │  <───────────────────────────  │  (this container)   │  <───────────   │  cluster        │
└──────────────┘                                └─────────────────────┘                 └─────────────────┘
```

### Run the server in Docker

You'll need a running Aerospike cluster.
Then start the Graph Service:

```bash
docker run -d --name graph \
  -p 8182:8182 \
  -e aerospike.client.host="aerospike:3000" \
  -e aerospike.client.namespace="test" \
  aerospike/aerospike-graph-service:latest
```

`aerospike.client.host` accepts one or more `host:port` pairs (comma-separated)
pointing at your Aerospike cluster. `aerospike.client.namespace` must already
exist on that cluster.

Verify it's up:

```bash
docker logs graph | grep 'Channel started'
# => Channel started at port 8182.
```

### Connect with a Gremlin driver

Python, using `gremlin-python`:

```python
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

g = traversal().with_remote(
    DriverRemoteConnection("ws://localhost:8182/gremlin", "g")
)

# Create a couple of vertices and an edge
alice = g.addV("person").property("name", "alice").next()
bob   = g.addV("person").property("name", "bob").next()
g.add_e("knows").from_(alice).to(bob).property("since", 2020).iterate()

# Traverse
friends = g.V().has("person", "name", "alice").out("knows").values("name").to_list()
print(friends)  # ['bob']
```

Java, using the TinkerPop driver:

```java
Cluster cluster = Cluster.build("localhost").port(8182).create();
GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));

List<Object> friends = g.V().has("person", "name", "alice")
                          .out("knows").values("name").toList();
```

All TinkerPop-compatible driver work the same way. A more
complete walk-through, including loading sample datasets (the air-routes
graph and a synthetic schema generator), is in
[`docs/SETUP.md`](docs/SETUP.md).

For more runnable end-to-end examples, notebooks, sample datasets, bulk-load
recipes, and application patterns see the companion repository
[`aerospike/aerospike-graph`](https://github.com/aerospike/aerospike-graph).


## Configuration

Configuration is supplied as a standard Java `.properties` file mounted
into the container, or as environment
variables. A minimal `aerospike-graph.properties`:

```properties
aerospike.client.host=aerospike-node:3000
aerospike.client.namespace=test
```

Pass it to the container:

```bash
docker run -d --name graph \
  -p 8182:8182 \
  -v $PWD/aerospike-graph.properties:/opt/aerospike-graph/conf/aerospike-graph.properties \
  aerospike/aerospike-graph-service:latest
```

For the full option reference — including auth, TLS, caching, indexing,
Spark executor tuning, metrics, and JVM heap sizing — see
[`docs/CONFIG_OPTIONS.md`](docs/CONFIG_OPTIONS.md).

## Documentation

Core docs, organized roughly by audience:

**User guides**
- [`docs/SETUP.md`](docs/SETUP.md) — first-run walkthrough
- [`docs/DOCKER_USER_DOCUMENTATION.md`](docs/DOCKER_USER_DOCUMENTATION.md) — Docker-specific deployment patterns
- [`docs/CONFIG_OPTIONS.md`](docs/CONFIG_OPTIONS.md) — complete configuration reference
- [`docs/INDEX_USAGE.md`](docs/INDEX_USAGE.md) — when and how to use indexes
- [`docs/METRICS.md`](docs/METRICS.md) — operational metrics and what to alert on
- [`aerospike/aerospike-graph`](https://github.com/aerospike/aerospike-graph) — runnable example applications, notebooks, and sample datasets

**Design docs**
- [`docs/DATA_MODEL_DESIGN.md`](docs/DATA_MODEL_DESIGN.md) — the details about the `packed` data model
- [`docs/INDEX_DESIGN.md`](docs/INDEX_DESIGN.md) — how graph leverages aerospike's secondary indexes
- [`docs/ID_MANAGEMENT.md`](docs/ID_MANAGEMENT.md) — vertex/edge ID generation and allocation
- [`docs/BULK_LOADER_DESIGN.md`](docs/BULK_LOADER_DESIGN.md) — architecture of the Spark bulk loader
- [`docs/LOCAL_GRAPH_COMPUTER.md`](docs/LOCAL_GRAPH_COMPUTER.md) — single-JVM OLAP for smaller graphs
- [`docs/CACHE_MANAGEMENT.md`](docs/CACHE_MANAGEMENT.md) — multi-level cache hierarchy
- [`docs/TRAVERSAL_CACHE.md`](docs/TRAVERSAL_CACHE.md) — query-plan caching
- [`docs/GENERATION_CHECK_BASED_WRITES_DESIGN.md`](docs/GENERATION_CHECK_BASED_WRITES_DESIGN.md) — optimistic concurrency control

**Operations**
- [`docs/GRAPH_MAX_MEMORY.md`](docs/GRAPH_MAX_MEMORY.md) — heap sizing guidance
- [`docs/TTL.md`](docs/TTL.md) — time-to-live semantics
- [`docs/QUERY_TRACING.md`](docs/QUERY_TRACING.md) — per-query tracing
- [`docs/SUPERNODE_FLAG.md`](docs/SUPERNODE_FLAG.md) — handling extreme-degree vertices


## Building from source

> **Codename `firefly`.** The project was developed internally under the
> codename *Firefly*, and the codename is preserved throughout the code
> and repository: Java packages (`com.aerospike.firefly.*`), the root
> Maven `<artifactId>`, the internal GHCR dev image
> (`ghcr.io/aerospike/firefly`), and config-prefix/env-var names
> (`firefly.*` / `FIREFLY_*`). The **product name** — the thing you
> deploy, document, and talk to other humans about — is
> **Aerospike Graph Service**, and that's the name used for the
> user-facing Docker Hub image (`aerospike/aerospike-graph-service`).
> Mapping the codename onto every internal identifier was deliberately
> skipped to keep the OSS diff small and avoid churn for anyone on
> internal branches; think of `firefly` the same way `linux` users
> think of `tux` or `gcc` users think of `gnu`: it's the animal, not
> the product.

The repository is laid out as a multi-module Maven build:

| Module | What's in it |
|---|---|
| [`aerospike-graph-gremlin/`](aerospike-graph-gremlin)           | Core OLTP engine. `FireflyGraph`, the `packed` storage layout, indexes, traversal strategies, and the embedded gremlin-server. |
| [`aerospike-graph-bulk-loader/`](aerospike-graph-bulk-loader)   | Spark-based bulk loader for CSV / GraphML / GraphSON inputs. |
| [`aerospike-graph-olap/`](aerospike-graph-olap)                 | Spark-backed `GraphComputer` (OLAP) implementation for cluster-wide analytics. |
| [`aerospike-graph-api/`](aerospike-graph-api)                   | Stable API surface shared between the engine and extensions. |

Deeper design docs live under [`docs/`](docs); see the
[documentation index](#documentation) below.

### Prerequisites

- JDK 11 or newer. CI covers JDK 11 and 17; JDK 21 is on the roadmap
  but not yet in the test matrix (see
  [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md)).
- Maven 3.9+ (matches CI; see
  [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md))
- Docker (for the integration tests and for building the runtime image)
- An Aerospike cluster, or `docker run -p 3000:3000 aerospike/aerospike-server` for local development

### Build the JARs

```bash
mvn -ntp -DskipTests clean package
```

Outputs land in each module's `target/` directory as `*-jar-with-dependencies.jar`.

### Build the Docker image

```bash
docker build --tag firefly:dev .
docker run -d -p 8182:8182 \
  -e AEROSPIKE_HOST=host.docker.internal:3000 \
  -e AEROSPIKE_NAMESPACE=test \
  firefly:dev
```

Released user-facing images are published on Docker Hub as
`aerospike/aerospike-graph-service:<version>` (with a moving `:latest`
tag). Release-candidate builds are pushed to GitHub Container Registry
as `ghcr.io/aerospike/firefly:<version>` under the codename.

### License-clean dependency graph

```bash
mvn -ntp -Plicense-check verify
```

This profile is opt-in (CI runs it on every PR) and will fail the build
if any compile/runtime dependency's license is not on the Apache-2.0
-compatible allowlist in the root `pom.xml`. The generated inventory
is written to `THIRD_PARTY.txt`.

## Testing

The default test suite is entirely self-contained:

```bash
mvn -ntp test
```

Integration tests that require a live Aerospike node are invoked via
module-specific profiles; see the individual module `pom.xml` files for
the exact profile names. CI runs the full matrix; see
[`.github/workflows/`](.github/workflows).

Benchmark tests use [JMH](https://openjdk.org/projects/code-tools/jmh/).
They live under `src/test/java/.../benchmark/` in each module and are
skipped by default. Run them with:

```bash
mvn -ntp -pl aerospike-graph-gremlin test -Dtest='BenchmarkTest*'
```


## Contributing

We welcome pull requests. Before you start, please:

1. Read [`CONTRIBUTING.md`](CONTRIBUTING.md) for the development
   workflow and DCO sign-off requirement.
2. Check the [issue tracker][issues] for open discussions on the area
   you want to work in; for anything non-trivial, open an issue first
   so we can agree on the approach.
3. Follow the [Code of Conduct](CODE_OF_CONDUCT.md) in all project
   spaces.

For security-sensitive reports, follow the private disclosure process
in [`SECURITY.md`](SECURITY.md) instead of opening a public issue.

## License

Aerospike Graph Service is licensed under the [Apache License,
Version 2.0](LICENSE). Copyright notices and third-party attributions
are in [`NOTICE`](NOTICE); the machine-generated dependency inventory
is in `THIRD_PARTY.txt` (produced by `mvn -Plicense-check verify`).

## Trademarks

"Aerospike" is a registered trademark of Aerospike, Inc. Use of the
mark is governed by the
[Aerospike trademark policy](https://aerospike.com/legal/trademarks).
The Apache-2.0 license on the source and binaries in this repository
does **not** grant any trademark license.

"Apache", "Apache TinkerPop", "Gremlin", and "Apache Spark" are
trademarks of the Apache Software Foundation.

## Acknowledgements

This project stands on the shoulders of:

- [Apache TinkerPop](https://tinkerpop.apache.org) — the Gremlin
  language, server, and provider framework.
- [Apache Spark](https://spark.apache.org) — the OLAP and bulk-loader
  runtimes.
- [Netty](https://netty.io) — the async I/O foundation.
- [The Aerospike Java client](https://github.com/aerospike/aerospike-client-java)
  — the storage layer's transport.
- Every contributor to the many transitive dependencies listed in
  `NOTICE` and `THIRD_PARTY.txt`.

[tinkerpop]: https://tinkerpop.apache.org
[aerospike]: https://aerospike.com
[issues]: https://github.com/aerospike/aerospike-graph-service/issues
