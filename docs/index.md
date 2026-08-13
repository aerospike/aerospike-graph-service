# Aerospike Graph Service (AGS) documentation

> Official user documentation: install, deploy, query, and manage guides at [aerospike.com/docs/graph](https://aerospike.com/docs/graph).

This site contains repository-specific deployment, configuration, and
compatibility information. For general install, deploy, query, and
manage guides, see [aerospike.com/docs/graph](https://aerospike.com/docs/graph).

## Database Editions and Features

AGS works with Aerospike Database Community Edition. If you choose
Enterprise or Standard Edition, provide the feature key required by
that database edition. Optional AGS capabilities such as MRT,
transactions, and expression indexes have separate database-version
and configuration requirements; see the
[compatibility matrix](COMPATIBILITY.md#optional-ags-features).

## User Reference

- [Setup](SETUP.md): minimal properties file and connection options.
- [Compatibility matrix](COMPATIBILITY.md): supported Aerospike
  server / TinkerPop / JDK / Spark / client versions.
- [Running in Docker](DOCKER_USER_DOCUMENTATION.md): env-var quickstart,
  properties-file mode, and custom Gremlin Server YAML.
- [Configuration options](CONFIG_OPTIONS.md): every `aerospike.*`
  property recognized by the service, organized by audience.
- [TTL](TTL.md): record expiration behavior.
- [Using indexes](INDEX_USAGE.md): what traversals benefit, and how.
- [Cache management](CACHE_MANAGEMENT.md): read-through caches, tuning,
  observability.
- [Query tracing](QUERY_TRACING.md): OpenTelemetry/Zipkin integration.
- [Supernode flag](SUPERNODE_FLAG.md): marking high-degree vertices.
- [Graph max memory](GRAPH_MAX_MEMORY.md): JVM heap sizing for the
  container defaults.
- [Metrics](METRICS.md): what's exported and where.
- [ID management](ID_MANAGEMENT.md): user-supplied and generated IDs.
- [Metadata set config](METADATA_SET_CONFIG.md): runtime metadata
  configuration.
- [Usage stats](USAGE_STATS.md): local resource accounting and controls.
- [Graph OLAP](GRAPH_OLAP.md): running Gremlin
  `GraphComputer` (OLAP) jobs against a Spark cluster.

## Engineering Reference

These documents describe implementation details for contributors and
are not product deployment guides.

- [Public API Javadoc](api/)
- [Architecture](ARCHITECTURE.md)
- [Data model design](DATA_MODEL_DESIGN.md)
- [Indexing design](INDEX_DESIGN.md)
- [Bulk loader design](BULK_LOADER_DESIGN.md)
- [Synthetic dataset capacity](SYNTHETIC_DATASET_CAPACITY.md)
- [Write consistency](WRITE_CONSISTENCY.md)
- [Bidirectional search (part I)](blogs/BIDIRECTIONAL_SEARCH_PART_1.md)
