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

### Quickstart with Docker Compose

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

The rest of this section shows how to run AGS in Docker. Pick one path below.
See [Deploy Aerospike Graph Service with Docker](https://aerospike.com/docs/graph/deploy/docker)
for the full deployment guide.

### Prerequisites

Before you run AGS in Docker, you need:

- An Aerospike feature-key file with the `graph-service` key enabled. See
  [feature-key file](https://aerospike.com/docs/database/manage/planning/feature-key)
  or [Deploy Aerospike Graph Service with Docker](https://aerospike.com/docs/graph/deploy/docker).
- Aerospike Database version 7.0 or later. See [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md).
- A namespace that already exists on the cluster with
  [`default-ttl`](https://aerospike.com/docs/database/reference/config#namespace__default-ttl)
  set to `0`. See [TTL on the Aerospike namespace](#ttl-on-the-aerospike-namespace) below.
- If access control is enabled on Aerospike, the AGS database user needs `sys-admin`
  and `read-write` privileges.

### Aerospike Database in Docker

Use this path if you do not already have Aerospike running. It starts Aerospike Database
and AGS together on a shared Docker network. Do not also run
[Run AGS with Docker](#run-ags-with-docker) afterward.

1. Pull the images:

```bash
docker pull aerospike/aerospike-server-enterprise:latest
docker pull aerospike/aerospike-graph-service:latest
```

2. Start Aerospike Database and AGS:

```bash
docker network create ags-net 2>/dev/null || true && \
docker rm -f aerospike graph 2>/dev/null || true && \
docker run -d --name aerospike --network ags-net \
  aerospike/aerospike-server-enterprise:latest && \
until docker exec aerospike asinfo -v 'status' 2>/dev/null | grep -q ok; do sleep 1; done && \
docker run -d --name graph --network ags-net --restart unless-stopped \
  -p 8182:8182 \
  -e aerospike.client.namespace="test" \
  -e aerospike.client.host="aerospike:3000" \
  aerospike/aerospike-graph-service:latest
```

`aerospike:3000` is the Aerospike container name on `ags-net` plus the default service
port. `test` is the default namespace in the Aerospike Docker image. Change these only
if you configured Aerospike differently. The Aerospike image must be Enterprise Edition.
The community image (`aerospike/aerospike-server`) does not include the `graph-service`
feature key support required by AGS.

### Run AGS with Docker

Use this path only when Aerospike is already running somewhere else (on the host, on
another machine, or in a container you started separately). Skip this section if you
used [Aerospike Database in Docker](#aerospike-database-in-docker) above.

1. Pull the AGS Docker image:

```bash
docker pull aerospike/aerospike-graph-service:latest
```

2. Run the AGS container:

```bash
docker run -d --name graph \
  -p 8182:8182 \
  -e aerospike.client.namespace="NAMESPACE" \
  -e aerospike.client.host="HOSTNAME:PORT" \
  aerospike/aerospike-graph-service:latest
```

- `HOSTNAME:PORT`: hostname or IP of an Aerospike seed node and its service port. The
  default Aerospike service port is `3000`. Examples: `db.example.com:3000` for a remote
  cluster, or `host.docker.internal:3000` when Aerospike runs on the host outside Docker.
  Do not use `localhost` here. Inside the AGS container, `localhost` refers to the AGS
  container itself, not your machine.
- `NAMESPACE`: name of an existing namespace on that cluster, for example `test`. List
  namespaces with [`asadm`](https://aerospike.com/docs/database/tools/asadm) (`show namespaces`).

If Aerospike is running in a Docker container, put AGS on the same Docker network and
use the Aerospike container name as `HOSTNAME` (for example `aerospike:3000`), or get
its IP address with:

```bash
docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' AEROSPIKE_CONTAINER_ID
```

Replace `AEROSPIKE_CONTAINER_ID` with the Aerospike container ID or name.

### Server output

AGS logs to `stdout`. When the container is started with `-d`, follow the logs with:

```bash
docker logs -f graph
```

After startup completes, the log includes:

```text
Channel started at port 8182.
```

### TTL on the Aerospike namespace

If AGS fails to start with a `default-ttl` error, set the namespace TTL to `0`:

1. Start the Aerospike Tools container:

```bash
docker run -it aerospike/aerospike-tools asadm -h HOSTNAME
```

Replace `HOSTNAME` with the hostname or IP of your Aerospike server. If Aerospike is
running on a Docker network (for example `ags-net` from [Aerospike Database in Docker](#aerospike-database-in-docker)):

```bash
docker run -it --network ags-net aerospike/aerospike-tools asadm -h aerospike
```

2. At the `Admin>` prompt, enable dynamic configuration:

```text
enable
```

3. Set `default-ttl` to `0`:

```text
manage config namespace NAMESPACE param default-ttl to 0
```

4. Restart AGS:

```bash
docker start graph
```

### Connect with a Gremlin driver

With AGS running and port 8182 published (as in the steps above), connect to it from
your application using any TinkerPop 3.7.x driver. Use exactly 3.7.x. A 3.8.x driver
will fail with a `Could not locate method` error.

#### Python

1. Install the driver:

```bash
pip install 'gremlinpython>=3.7,<3.8'
```

2. Connect to AGS and run a traversal:

```python
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

g = traversal().with_remote(
    DriverRemoteConnection("ws://localhost:8182/gremlin", "g")
)

# Write two vertices and an edge
alice = g.addV("person").property("name", "alice").next()
bob   = g.addV("person").property("name", "bob").next()
g.add_e("knows").from_(alice).to(bob).property("since", 2020).iterate()

# Read them back
friends = g.V().has("person", "name", "alice").out("knows").values("name").to_list()
print(friends)  # ['bob']
```

#### Java

1. Create a Maven project if you do not have one, then add the driver to your `pom.xml`:

```xml
<dependency>
  <groupId>org.apache.tinkerpop</groupId>
  <artifactId>gremlin-driver</artifactId>
  <version>3.7.3</version>
</dependency>
```

2. Connect to AGS and run a traversal (imports omitted for brevity):

```java
Cluster cluster = Cluster.build("localhost").port(8182).create();
GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));

List<Object> friends = g.V().has("person", "name", "alice")
                          .out("knows").values("name").toList();
```

Any other TinkerPop-compatible driver works the same way. A more complete walk-through,
including loading sample datasets, is in [`docs/SETUP.md`](docs/SETUP.md).

For more runnable end-to-end examples, notebooks, sample datasets, bulk-load
recipes, and application patterns see the companion repository
[`aerospike/aerospike-graph`](https://github.com/aerospike/aerospike-graph).


## Configuration

The Docker run commands above pass configuration as `-e` environment variables. For
deployments with more settings, you can instead write a properties file and mount it
into the container. Both approaches use the same property names.

A minimal `aerospike-graph.properties`:

```properties
aerospike.client.host=HOSTNAME:PORT
aerospike.client.namespace=NAMESPACE
```

Mount it with the `-v` flag instead of the `-e` flags:

```bash
docker run -d --name graph \
  -p 8182:8182 \
  -v $PWD/aerospike-graph.properties:/opt/aerospike-graph/aerospike-graph.properties \
  aerospike/aerospike-graph-service:latest
```

The mount path inside the container must be
`/opt/aerospike-graph/aerospike-graph.properties`. The local path (`$PWD/...`) can be
anywhere on your machine.

For the full list of available properties (auth, TLS, caching, indexing, metrics, and
JVM heap sizing), see [`docs/CONFIG_OPTIONS.md`](docs/CONFIG_OPTIONS.md) and the
[Configuration reference](https://aerospike.com/docs/graph/reference/config) on
aerospike.com.

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

Run the image you built. Replace `HOSTNAME:PORT` and `NAMESPACE` as in [Run AGS with Docker](#run-ags-with-docker):

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
