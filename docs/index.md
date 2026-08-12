# Aerospike Graph Service (AGS) documentation

> Official user documentation: install, deploy, query, and manage guides at [aerospike.com/docs/graph](https://aerospike.com/docs/graph).

This directory is the engineering and design reference for contributors to this repository (data model, configuration properties, caches, bulk loader internals, and similar topics). The top-level [`README.md`](../README.md) summarizes the product, points here and to [aerospike.com/docs/graph](https://aerospike.com/docs/graph), and covers building from source.

> **Codename.** Throughout the source tree and these docs you see
> the codename `firefly` (in Java package paths, artifact IDs,
> Docker image tags, and a few internal API names). It refers to the
> same product, Aerospike Graph Service. Mapping the codename onto
> every identifier was deliberately skipped to keep the diff small
> and preserve `git blame` history. The codename is not going away.

## Getting started

- [Public API Javadoc](api/): generated from the `aerospike-graph-api`
  module. Refreshed when [`.github/workflows/publish-docs.yml`](../.github/workflows/publish-docs.yml) runs (see that workflow for branch and path triggers).
- [Setup](SETUP.md): minimal properties file and connection options.
- [Architecture](ARCHITECTURE.md): one-page map of the modules and
  request lifecycle.
- [Compatibility matrix](COMPATIBILITY.md): supported Aerospike
  server / TinkerPop / JDK / Spark / client versions.
- [Running in Docker](DOCKER_USER_DOCUMENTATION.md): env-var quickstart,
  properties-file mode, and custom Gremlin Server YAML.
- [Configuration options](CONFIG_OPTIONS.md): every `aerospike.*`
  property recognized by the service, organized by audience.

## Data model and storage

- [Data model design](DATA_MODEL_DESIGN.md): how vertices, edges, and
  properties map onto Aerospike records under the `packed` on-disk
  layout.
- [ID management](ID_MANAGEMENT.md): `~id` semantics, user-supplied
  vs. auto-generated IDs, the `~id` / `T.id` / plain `id` distinction.
- [TTL](TTL.md): record expiration behavior.
- [Metadata set config](METADATA_SET_CONFIG.md): where the graph stores
  its own bookkeeping.

## Query engine and caching

- [Indexing design](INDEX_DESIGN.md): vertex label and vertex property
  indexes.
- [Using indexes](INDEX_USAGE.md): what traversals benefit, and how.
- [Traversal cache](TRAVERSAL_CACHE.md): query-plan caching.
- [Cache management](CACHE_MANAGEMENT.md): read-through caches, tuning,
  observability.
- [Query tracing](QUERY_TRACING.md): OpenTelemetry/Zipkin integration.
- [Supernode flag](SUPERNODE_FLAG.md): marking high-degree vertices.

## Bulk loading and OLAP

- [Bulk loader design](BULK_LOADER_DESIGN.md): how the Spark-backed
  bulk loader turns CSV / GraphML / GraphSON into Aerospike records
  without going through the query path.
- [Local graph computer](LOCAL_GRAPH_COMPUTER.md): running
  `GraphComputer` jobs against a single JVM.
- [Synthetic dataset capacity](SYNTHETIC_DATASET_CAPACITY.md): notes on
  generating benchmark graphs.

## Operations

- [Graph max memory](GRAPH_MAX_MEMORY.md): JVM heap sizing for the
  container defaults.
- [Metrics](METRICS.md): what's exported and where.
- [Usage stats](USAGE_STATS.md): local-only resource accounting stored
  in the same Aerospike cluster as the graph data.
- [Generation-check-based writes](GENERATION_CHECK_BASED_WRITES_DESIGN.md):
  optimistic concurrency protocol for point writes.

## Design essays (long-form)

- [Bidirectional search (part I)](blogs/BIDIRECTIONAL_SEARCH_PART_1.md)
