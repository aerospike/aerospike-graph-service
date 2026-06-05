# Aerospike Graph Service (AGS)

> An [Apache TinkerPop][tinkerpop]-compatible graph database engine backed
> by [Aerospike][aerospike]. Designed for low-latency graph traversals at
> scale: terabytes of vertices and edges, thousands of concurrent
> queries, single-digit-millisecond p99, while keeping a plain,
> Gremlin-standard interface on the wire.

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![TinkerPop](https://img.shields.io/badge/TinkerPop-3.7.x-orange.svg)](https://tinkerpop.apache.org)
[![Java](https://img.shields.io/badge/Java-11%2B-green.svg)](https://adoptium.net)

> Documentation: User guides and deployment procedures at [aerospike.com/docs/graph](https://aerospike.com/docs/graph).

---

## What is Aerospike Graph Service

Aerospike Graph Service (AGS) is a JVM process that speaks the standard
TinkerPop Gremlin wire protocol over WebSocket and stores its data in
an Aerospike cluster. Use any Gremlin driver variant
(`gremlin-python`, `gremlin-javascript`, `tinkerpop-client` in Java,
and other TinkerPop-compatible drivers), issue normal Gremlin traversals and the
service translates them into efficient Aerospike operations, applies
query-planning optimizations specific to Aerospike's data model, and
streams results back to the driver.

### Why another graph database?

- Aerospike as the storage tier. Inherit Aerospike's strong
  consistency, predictable sub-millisecond reads, horizontal scale,
  hybrid-memory architecture, and cross-datacenter replication. Graph
  state is another flavor of workload on top of a high-performance, battle-tested
  key-value store.
- Standard Gremlin on the wire. Anything that speaks TinkerPop 3.7.x works. No bespoke query language, no
  custom drivers. TinkerPop 3.8 is not supported because of breaking changes in that line.
- Aerospike-native data layout. Vertices and edges are stored in
  a `packed` on-disk layout tuned for Aerospike's record structure,
  keeping adjacency information co-resident with the vertex so that
  most traversal hops resolve in a single Aerospike read. See
  [`docs/DATA_MODEL_DESIGN.md`](docs/DATA_MODEL_DESIGN.md).
- Scale-out OLAP. The Spark-backed OLAP module lets you run
  `GraphComputer` jobs (for example PageRank, connected components, custom
  VertexPrograms) over the full graph without holding it in a single
  JVM.
- Bulk loading. A Spark-backed bulk loader ingests CSV / GraphML /
  GraphSON vertex and edge files directly into the Aerospike data
  model, bypassing the query path for order-of-magnitude faster
  initial loads.

## Quickstart

```text
┌──────────────┐    Gremlin (ws://host:8182)    ┌─────────────────────┐    Aerospike    ┌─────────────────┐
│  Your app    │  ───────────────────────────>  │  Graph Service      │  ───────────>   │  Aerospike      │
│  (any lang)  │  <───────────────────────────  │  (this container)   │  <───────────   │  cluster        │
└──────────────┘                                └─────────────────────┘                 └─────────────────┘
```

### Fastest path from zero

If you do not already have an Aerospike cluster, the companion repository
[`aerospike/aerospike-graph`](https://github.com/aerospike/aerospike-graph) ships a
`docker-compose.yml` that starts AGS, Aerospike Database, and Zipkin in one command:

```bash
git clone https://github.com/aerospike/aerospike-graph.git
cd aerospike-graph
docker compose up -d
```

The repository also includes the Air Routes sample dataset and a bulk-load guide. The
official step-by-step walkthrough is at
[aerospike.com/docs/graph/quick-start](https://aerospike.com/docs/graph/quick-start).

The rest of this section shows how to connect AGS to an existing Aerospike cluster.

### Prerequisites

Before you run AGS in Docker, you need:

- An Aerospike feature-key file with the `graph-service` key enabled. See [feature-key file](https://aerospike.com/docs/database/manage/planning/feature-key) in the Aerospike Database documentation and [Deploy Aerospike Graph Service with Docker](https://aerospike.com/docs/graph/deploy/docker) for the full prerequisite list.
- A running Aerospike Database instance, version 7.0 or later. See [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md) and [Deploy Aerospike Graph Service with Docker](https://aerospike.com/docs/graph/deploy/docker). To start Aerospike with Docker, see [Install with Docker](https://aerospike.com/docs/database/install/docker/).
- A namespace that already exists on the cluster. The namespace must have the [`default-ttl`](https://aerospike.com/docs/database/reference/config#namespace__default-ttl) configuration option set to `0`. See [TTL on the Aerospike namespace](https://aerospike.com/docs/graph/deploy/docker#ttl-on-the-aerospike-namespace).

### Connection values

`HOSTNAME:PORT` and `NAMESPACE` come from your Aerospike cluster, not from AGS.

- **Seed address (`HOSTNAME:PORT`)**: hostname or IP of an Aerospike seed node, plus the Aerospike service port. The default service port is `3000`.
- **Namespace (`NAMESPACE`)**: name of an existing namespace on that cluster. Read it from the `namespace` block in `aerospike.conf`, or list namespaces with [`asadm`](https://aerospike.com/docs/database/tools/asadm) (`show namespaces`). The Aerospike [Install with Docker](https://aerospike.com/docs/database/install/docker/) quick start uses the default namespace `test`.

The namespace must exist before AGS starts.

### Run the server in Docker

**Option A — Aerospike already running outside Docker** (use `localhost` or the node IP):

```bash
docker run -d --name graph \
  -p 8182:8182 \
  -e aerospike.client.host="HOSTNAME:PORT" \
  -e aerospike.client.namespace="NAMESPACE" \
  aerospike/aerospike-graph-service:latest
```

**Option B — Aerospike running in Docker on the same machine** (put both containers on a shared network so the hostname resolves):

```bash
docker network create ags-net
docker run -d --name aerospike --network ags-net \
  aerospike/aerospike-server:latest          # or your Enterprise image

docker run -d --name graph --network ags-net \
  -p 8182:8182 \
  -e aerospike.client.host="aerospike:3000" \
  -e aerospike.client.namespace="test" \
  aerospike/aerospike-graph-service:latest
```

Using `localhost` inside a Docker container refers to the container itself, not the host. Always use a shared network with a named host, or get the Aerospike container IP with `docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' aerospike`.

`aerospike.client.host` accepts one or more `HOSTNAME:PORT` seed addresses (comma-separated).

Verify it's up:

```bash
docker logs graph | grep 'Channel started'
```

Expected output includes:

```text
Channel started at port 8182.
```

If AGS exits instead of starting, check `docker logs graph` for a `default-ttl` error. If present, set the namespace TTL to 0 with `asadm` and restart:

```bash
docker run --rm --network ags-net aerospike/aerospike-tools \
  asadm -h aerospike -e "enable; manage config namespace test param default-ttl to 0"
```

### Connect with a Gremlin driver

AGS speaks TinkerPop **3.7.x**. Use a 3.7.x driver. A 3.8.x driver will fail with
a `Could not locate method` error.

Python — install `gremlin-python` at the matching version, then run:

```bash
pip install 'gremlinpython>=3.7,<3.8'
```

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

Java, using the TinkerPop driver (imports omitted for brevity):

```java
Cluster cluster = Cluster.build("localhost").port(8182).create();
GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));

List<Object> friends = g.V().has("person", "name", "alice")
                          .out("knows").values("name").toList();
```

All TinkerPop-compatible drivers work the same way. A more
complete walk-through, including loading sample datasets (the air-routes
graph and a synthetic schema generator), is in
[`docs/SETUP.md`](docs/SETUP.md).

For more runnable end-to-end examples, notebooks, sample datasets, bulk-load
recipes, and application patterns see the companion repository
[`aerospike/aerospike-graph`](https://github.com/aerospike/aerospike-graph).


## Configuration

Configuration is supplied as a standard Java `.properties` file mounted
into the container, or as environment
variables. A minimal `aerospike-graph.properties` (see [Connection values](#connection-values) for where `HOSTNAME`, `PORT`, and `NAMESPACE` come from):

```properties
aerospike.client.host=HOSTNAME
aerospike.client.port=PORT
aerospike.client.namespace=NAMESPACE
```

The namespace must have the [`default-ttl`](https://aerospike.com/docs/database/reference/config#namespace__default-ttl) configuration option set to `0`. See [TTL on the Aerospike namespace](https://aerospike.com/docs/graph/deploy/docker#ttl-on-the-aerospike-namespace).

Pass it to the container:

```bash
docker run -d --name graph \
  -p 8182:8182 \
  -v $PWD/aerospike-graph.properties:/opt/aerospike-graph/aerospike-graph.properties \
  aerospike/aerospike-graph-service:latest
```

### Environment variables and properties

The container reads environment variables whose names start with `aerospike`, using the same keys as in a properties file (for example `aerospike.client.host=HOSTNAME:PORT` and `aerospike.client.namespace=NAMESPACE`).

For the full option reference (auth, TLS, caching, indexing,
Spark executor tuning, metrics, and JVM heap sizing), see
[`docs/CONFIG_OPTIONS.md`](docs/CONFIG_OPTIONS.md) and the [Configuration reference](https://aerospike.com/docs/graph/reference/config) on aerospike.com.

## Documentation

Core docs, organized roughly by audience:

### User guides

- [`docs/SETUP.md`](docs/SETUP.md): minimal properties and connection (runtime-focused, not a contributor build guide)
- [`docs/DOCKER_USER_DOCUMENTATION.md`](docs/DOCKER_USER_DOCUMENTATION.md): Docker-specific deployment patterns
- [`docs/CONFIG_OPTIONS.md`](docs/CONFIG_OPTIONS.md): complete configuration reference for this repository
- [`docs/INDEX_USAGE.md`](docs/INDEX_USAGE.md): when and how to use indexes
- [`docs/METRICS.md`](docs/METRICS.md): operational metrics and what to alert on
- [`aerospike/aerospike-graph`](https://github.com/aerospike/aerospike-graph): runnable example applications, notebooks, and sample datasets

### Design docs

- [`docs/DATA_MODEL_DESIGN.md`](docs/DATA_MODEL_DESIGN.md): the details about the `packed` data model
- [`docs/INDEX_DESIGN.md`](docs/INDEX_DESIGN.md): how graph leverages Aerospike secondary indexes
- [`docs/ID_MANAGEMENT.md`](docs/ID_MANAGEMENT.md): vertex/edge ID generation and allocation
- [`docs/BULK_LOADER_DESIGN.md`](docs/BULK_LOADER_DESIGN.md): architecture of the Spark bulk loader
- [`docs/LOCAL_GRAPH_COMPUTER.md`](docs/LOCAL_GRAPH_COMPUTER.md): single-JVM OLAP for smaller graphs
- [`docs/CACHE_MANAGEMENT.md`](docs/CACHE_MANAGEMENT.md): multi-level cache hierarchy
- [`docs/TRAVERSAL_CACHE.md`](docs/TRAVERSAL_CACHE.md): query-plan caching
- [`docs/GENERATION_CHECK_BASED_WRITES_DESIGN.md`](docs/GENERATION_CHECK_BASED_WRITES_DESIGN.md): optimistic concurrency control

### Operations

- [`docs/GRAPH_MAX_MEMORY.md`](docs/GRAPH_MAX_MEMORY.md): heap sizing guidance
- [`docs/TTL.md`](docs/TTL.md): time to live (TTL) semantics
- [`docs/QUERY_TRACING.md`](docs/QUERY_TRACING.md): per-query tracing
- [`docs/SUPERNODE_FLAG.md`](docs/SUPERNODE_FLAG.md): handling extreme-degree vertices

For install, deploy, query, and manage guides, see [aerospike.com/docs/graph](https://aerospike.com/docs/graph).


## Building from source

> Codename `firefly`. The internal codename remains in Java packages (`com.aerospike.firefly.*`), the root Maven `artifactId`, dev images on GHCR (`ghcr.io/aerospike/firefly`), and `firefly.*` / `FIREFLY_*` configuration. The shipped product is Aerospike Graph Service. For more background, see [Why `firefly` persists in identifiers](CONTRIBUTING.md#why-firefly-persists-in-identifiers) in CONTRIBUTING.md.

The repository is laid out as a multi-module Maven build:

| Module | What's in it |
|---|---|
| [`aerospike-graph-gremlin/`](aerospike-graph-gremlin)           | Core OLTP engine. `FireflyGraph`, the `packed` storage layout, indexes, traversal strategies, and the embedded gremlin-server. |
| [`aerospike-graph-bulk-loader/`](aerospike-graph-bulk-loader)   | Spark-based bulk loader for CSV / GraphML / GraphSON inputs. |
| [`aerospike-graph-olap/`](aerospike-graph-olap)                 | Spark-backed `GraphComputer` (OLAP) implementation for cluster-wide analytics. |
| [`aerospike-graph-api/`](aerospike-graph-api)                   | Stable API surface shared between the engine and extensions. |

Deeper design docs live under [`docs/`](docs). See the [Documentation](#documentation) section.

### Prerequisites

- JDK 11 or newer. CI covers JDK 11 and 17. Supported combinations are listed in [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md).
- Maven 3.9+ (matches CI). See [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md).
- Docker with [Buildx](https://docs.docker.com/build/buildx/) for building container images.
- Python 3 and [`python_on_whales`](https://github.com/gabrieldemarmiesse/python-on-whales) for [`scripts/build-docker.py`](scripts/build-docker.py) (`pip install python_on_whales`).
- An Aerospike Database 7.0+ cluster for integration-style testing. Supported versions are listed in [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md).
- CI-style cluster: [`scripts/start_aerospike.sh`](scripts/start_aerospike.sh) starts a multi-node Enterprise cluster and expects a feature-key file (see `.github/aerospike/`). That path is oriented toward matching CI, not general laptop setup. On macOS, [`scripts/macos-start-aerospike.sh`](scripts/macos-start-aerospike.sh) runs the same flow inside a container with the Docker socket mounted. It assumes a local image tag such as `firefly:dev` already exists.

Maven compiles with Java 11. The Dockerfiles in [`docker/`](docker/) package a JDK 17 runtime.

### Build the JARs

```bash
mvn -ntp -DskipTests clean package
```

Shaded runnable JARs are written under each module's `target/` directory. `VERSION` matches the `<version>` in the root `pom.xml`, for example:

- `aerospike-graph-gremlin/target/aerospike-graph-gremlin-VERSION.jar`
- `aerospike-graph-bulk-loader/target/aerospike-graph-bulk-loader-VERSION.jar`
- `aerospike-graph-olap/target/aerospike-graph-olap-VERSION.jar` (when built)

### Run from the JVM

From the repository root, after building the gremlin module:

```bash
mvn -ntp -DskipTests -pl aerospike-graph-gremlin -am package
java -jar aerospike-graph-gremlin/target/aerospike-graph-gremlin-*.jar conf/local.yaml
```

[`conf/local.yaml`](conf/local.yaml) points Gremlin Server at the sample properties under [`conf/`](conf/). Edit `conf/aerospike-graph.properties` (or the YAML) so `aerospike.client.host` reaches your Aerospike seeds.

Verify the process is listening: in the server log output, look for
`Channel started at port 8182.` (the same line as in the Docker quickstart).

### Build the Docker image

Do not run `docker build` from the repository root without the build arguments the [`docker/Dockerfile`](docker/Dockerfile) expects. Use the helper script from the repository root:

```bash
pip install python_on_whales
python3 scripts/build-docker.py --tags firefly:dev --platforms linux/amd64
```

- `--slim`: builds only the graph JAR and uses [`docker/Dockerfile-slim`](docker/Dockerfile-slim). The image has no bulk loader (smaller footprint, same idea as `aerospike/aerospike-graph-service:latest-slim` on Docker Hub).
- `--use_local`: skip Maven and reuse JARs already present under `*/target/`.

Run the image you built. Use the same `HOSTNAME:PORT` and `NAMESPACE` values as in [Connection values](#connection-values):

```bash
docker run -d -p 8182:8182 \
  -e aerospike.client.host=HOSTNAME:PORT \
  -e aerospike.client.namespace=NAMESPACE \
  firefly:dev
```

Released user-facing images are published on Docker Hub as
`aerospike/aerospike-graph-service:VERSION` (with a moving `:latest`
tag). The standard image includes the standalone bulk loader. The slim image is graph-only. Release-candidate builds are pushed to GitHub Container Registry
as `ghcr.io/aerospike/firefly:VERSION` under the codename.

### License-clean dependency graph

```bash
mvn -ntp -Plicense-check verify
```

This profile is opt-in (CI runs it on every PR) and fails the build
if any compile/runtime dependency's license is not on the Apache-2.0
-compatible allowlist in the root `pom.xml`. The enforcement report is
written to `target/THIRD_PARTY.enforce.txt`. A separate inventory at
`THIRD_PARTY.txt` is generated during a default `verify` run.

## Testing

The default `mvn test` run is self-contained for unit tests (no live Aerospike required for the default Surefire selection):

```bash
mvn -ntp test
```

Tests that need a real Aerospike cluster, Docker, or other heavy fixtures are excluded or gated in the root POM Surefire configuration and in CI. The full matrix runs in [`.github/workflows/`](.github/workflows/) with the cluster helpers under [`.github/aerospike/`](.github/aerospike/).

Benchmark tests use [JMH](https://openjdk.org/projects/code-tools/jmh/).
They live under `src/test/java/.../benchmark/` in each module and are
skipped by default. Run them with:

```bash
mvn -ntp -pl aerospike-graph-gremlin test -Dtest='BenchmarkTest*'
```


## Contributing

We welcome pull requests. Before you start:

1. Read [`CONTRIBUTING.md`](CONTRIBUTING.md) for the development
   workflow and DCO sign-off requirement.
2. Check the [issue tracker][issues] for open discussions on the area
   you want to work in. For anything non-trivial, open an issue first
   so we can agree on the approach.
3. Follow the [Code of Conduct](CODE_OF_CONDUCT.md) in all project
   spaces.

For security-sensitive reports, follow the private disclosure process
in [`SECURITY.md`](SECURITY.md) instead of opening a public issue.

## License

Aerospike Graph Service is licensed under the [Apache License,
Version 2.0](LICENSE). Copyright notices and third-party attributions
are in [`NOTICE`](NOTICE). The machine-generated dependency inventory
is in `THIRD_PARTY.txt` (from a default `verify` run) and
`target/THIRD_PARTY.enforce.txt` (from `mvn -ntp -Plicense-check verify`).

## Trademarks

"Aerospike" is a registered trademark of Aerospike, Inc. Use of the
mark is governed by the
[Aerospike trademark policy](https://aerospike.com/legal/trademarks).
The Apache-2.0 license on the source and binaries in this repository
does not grant any trademark license.

"Apache", "Apache TinkerPop", "Gremlin", and "Apache Spark" are
trademarks of the Apache Software Foundation.

## Acknowledgements

This project stands on the shoulders of:

- [Apache TinkerPop](https://tinkerpop.apache.org): the Gremlin
  language, server, and provider framework.
- [Apache Spark](https://spark.apache.org): the OLAP and bulk-loader
  runtimes.
- [Netty](https://netty.io): the async I/O foundation.
- [The Aerospike Java client](https://github.com/aerospike/aerospike-client-java):
  the storage layer's transport.
- Every contributor to the many transitive dependencies listed in
  `NOTICE` and `THIRD_PARTY.txt`.

[tinkerpop]: https://tinkerpop.apache.org
[aerospike]: https://aerospike.com
[issues]: https://github.com/aerospike/aerospike-graph-service/issues
