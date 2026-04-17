# Architecture

This page is a one-page map of the Aerospike Graph Service codebase:
what each Maven module does, how they fit together at runtime, and
where the interesting seams live. For deeper treatments of individual
subsystems see the per-topic docs linked throughout
[`docs/index.md`](index.md).

> Reminder: the internal codename is `firefly`. It is still the name
> of every Java package, the Maven `artifactId`, the Docker image
> name, and most config prefixes. "Aerospike Graph Service" is the
> public product name — treat the two as interchangeable.

## The 10,000-foot view

```
         ┌───────────────────────────────────────────────────┐
         │            Client (Gremlin driver / HTTP)         │
         └───────────────┬───────────────┬───────────────────┘
                         │               │
                 Gremlin │               │ HTTP / REST
                         ▼               ▼
         ┌───────────────────────────────────────────────────┐
         │           Aerospike Graph Service (JVM)           │
         │                                                   │
         │   ┌──────────────────────────────────────────┐    │
         │   │  TinkerPop Gremlin Server + Admin HTTP   │    │
         │   └───────────────┬──────────────────────────┘    │
         │                   │                               │
         │   ┌───────────────▼──────────────────────────┐    │
         │   │  FireflyGraph  (TinkerPop Graph impl)    │    │
         │   │  • traversal strategies                  │    │
         │   │  • call-step registry                    │    │
         │   │  • id manager / cache                    │    │
         │   │  • summary / metadata services           │    │
         │   └───────────────┬──────────────────────────┘    │
         │                   │                               │
         │   ┌───────────────▼──────────────────────────┐    │
         │   │  AerospikeConnection (record I/O)        │    │
         │   │  • bin layout + codecs                   │    │
         │   │  • secondary-index driven scans          │    │
         │   │  • batch / paged query operators         │    │
         │   └───────────────┬──────────────────────────┘    │
         └───────────────────┼───────────────────────────────┘
                             │ Aerospike native wire protocol
                             ▼
            ┌─────────────────────────────────────────┐
            │           Aerospike Database            │
            │  (1+ nodes, a single graph namespace)   │
            └─────────────────────────────────────────┘
```

Side processes (run out-of-process, talk to the same namespace):

```
  ┌──────────────────────────┐         ┌──────────────────────────┐
  │ aerospike-graph-bulk-    │         │ aerospike-graph-olap     │
  │   loader  (Spark job)    │         │   (Spark GraphComputer)  │
  └──────────────────────────┘         └──────────────────────────┘
             │                                   │
             └───────────── write / scan ────────┘
                                │
                                ▼
                 ┌──────────────────────────┐
                 │     Aerospike cluster    │
                 └──────────────────────────┘
```

## Maven modules

| Module                         | Role                                                                 | Entry point                                                    |
|--------------------------------|----------------------------------------------------------------------|----------------------------------------------------------------|
| `aerospike-graph-gremlin`      | The graph service itself: TinkerPop Graph implementation, Gremlin    | `com.aerospike.firefly.runtime.FireflyServer`                  |
|                                | Server wiring, admin HTTP, traversal strategies, call steps,         |                                                                |
|                                | record codecs, index logic.                                          |                                                                |
| `aerospike-graph-olap`         | Distributed `GraphComputer` implementation that runs Gremlin OLAP    | `com.aerospike.firefly.olap.DistributedGraphComputerMain`      |
|                                | jobs on top of Spark, reading and writing the same Aerospike         |                                                                |
|                                | namespace that the online service uses.                              |                                                                |
| `aerospike-graph-bulk-loader`  | Standalone Spark job for loading CSV/Parquet vertex and edge data    | `com.aerospike.firefly.bulkloader.SparkBulkLoaderMain`         |
|                                | directly into the Aerospike record layout, bypassing the Gremlin     |                                                                |
|                                | write path for throughput.                                           |                                                                |
| `aerospike-graph-test-common`  | Shared test scaffolding, fixtures, and provider plumbing.            | n/a (test-scope only)                                          |

All three main modules share the same groupId (`com.aerospike`) and
are versioned together.

## `aerospike-graph-gremlin` internals

This is where most of the interesting logic lives. Package layout:

| Package                                     | Responsibility                                                        |
|---------------------------------------------|------------------------------------------------------------------------|
| `com.aerospike.firefly.structure`           | TinkerPop `Graph` / `Vertex` / `Edge` implementations, id management. |
| `com.aerospike.firefly.process`             | Traversal strategies, custom `Step`s, call-step registry.             |
| `com.aerospike.firefly.process.call.*`      | Admin / management / metadata call steps (`g.call(...)` entry points).|
| `com.aerospike.firefly.io.aerospike`        | `AerospikeConnection` — the one place that talks to the Aerospike     |
|                                             | Java client. Bin layout, codecs, batch and scan policy live here.     |
| `com.aerospike.firefly.io.aerospike.indexes`| Secondary-index maintenance and index-driven query planning.          |
| `com.aerospike.firefly.runtime`             | Process bootstrap, config loading, background tasks, HTTP admin.      |
| `com.aerospike.firefly.security`            | JWT issuance / validation, audit logging hooks.                       |
| `com.aerospike.firefly.util.config`         | `ConfigurationHelper` — canonical list of every tunable config key.   |
| `com.aerospike.firefly.features`            | Feature-flag plumbing and version-gating helpers.                     |
| `com.aerospike.firefly.jsr223`              | Gremlin-language plugin so the service is usable from `gremlin.sh`.   |

### Request lifecycle (read path)

1. Client sends a Gremlin query over bytecode to the Gremlin Server.
2. TinkerPop compiles the bytecode into a `Traversal` and applies the
   registered strategies from `com.aerospike.firefly.process.*.strategy`
   (cached-adjacency, drop-merge, vertex-edge-local-count, etc.).
3. The rewritten traversal's steps pull from the Firefly iterators
   (`structure.iterator.*`), which in turn call into
   `AerospikeConnection`.
4. `AerospikeConnection` chooses a plan: single-record read, primary-key
   batch, secondary-index scan, or a pushed-down `QueryFilter`. Results
   flow back up as `FireflyVertex` / `FireflyEdge` records.
5. If summary / index metadata is stale, background tasks
   (`runtime.tasks.*`) refresh it asynchronously.

### Request lifecycle (write path)

Writes (`addV`, `addE`, property mutations) go through the same
`FireflyGraph` surface but bypass traversal strategies. Each mutation
is translated into a batched Aerospike operation (`Operation[]`) and
committed in the smallest number of RPCs possible, with transaction
boundaries defined by the in-process `FireflyTransaction`.

## OLAP path (`aerospike-graph-olap`)

For analytical traversals that would be unreasonable on the online
service — `PageRank`, `ConnectedComponents`, large aggregations —
there is a separate Spark-based `GraphComputer`. It reads the same
record layout directly (via `codec.RowCodec`), runs the vertex
program across a Spark cluster, and writes results back. The online
service is not involved in an OLAP job. See
[`BULK_LOADER_DESIGN.md`](BULK_LOADER_DESIGN.md) for the file-level
equivalent on the write side.

## Bulk loader (`aerospike-graph-bulk-loader`)

A Spark job that ingests CSV / Parquet data and writes vertex and
edge records directly into Aerospike in the layout the online service
expects. Bypasses the Gremlin write path for throughput; used for the
"initial load" of a graph that is too large to push through the online
API. See [`BULK_LOADER_DESIGN.md`](BULK_LOADER_DESIGN.md) for the
design and [`aerospike-graph-bulk-loader/README.md`](../aerospike-graph-bulk-loader/README.md)
for invocation.

## Runtime shape

- **Graph service**: a single JVM process per Gremlin Server instance.
  Memory-sized to fit the config cache and summary. Stateless across
  restarts — all durable state is in Aerospike.
- **Aerospike cluster**: 1 namespace per graph, any number of nodes.
  The service is node-agnostic and discovers the cluster through the
  Aerospike client's seed list.
- **OLAP / bulk-loader**: Spark executors, configured separately.
  No long-running state.

## Key extension seams

If you are reading this to figure out where to plug something in,
these are the natural entry points:

- **A new call step** (admin or query): register in
  `process.call.CallStepRegistry` and implement `AbstractCallStep`.
- **A new traversal-optimization strategy**: extend
  `AbstractTraversalStrategy` under
  `process.traversal.strategy.optimization`, add to the strategy
  registry in `FireflyGraph`.
- **A new config key**: add a constant to
  `util.config.ConfigurationHelper.Keys`, register a default in the
  `DEFAULT_VALUES` map, and (if numeric/bool) add a validator.
- **A new bin or set**: extend the `Bins` or `Sets` enums in
  `ConfigurationHelper.Keys`; the numeric byte is the on-disk key, so
  do **not** renumber existing entries.
- **A new authentication backend**: implement
  `security.auth.AuthenticationBackend` and wire it through the JWT
  service init in `security.jwt.JwtServiceInit`.

## See also

- [`SETUP.md`](SETUP.md) — how to actually run the thing.
- [`CONFIG_OPTIONS.md`](CONFIG_OPTIONS.md) — every tunable, annotated.
- [`ID_MANAGEMENT.md`](ID_MANAGEMENT.md) — how vertex / edge ids work.
- [`BULK_LOADER_DESIGN.md`](BULK_LOADER_DESIGN.md) — design of the
  standalone bulk-loader job.
- [`QUERY_TRACING.md`](QUERY_TRACING.md) — how traces bubble up for
  performance debugging.
