# Index Design

Firefly leans on Aerospike secondary indexes for anything that isn't a
direct `~id` point read. This document describes which indexes exist,
when they're created, and which queries actually use them.

For the underlying record layout the indexes are defined over, see
[`DATA_MODEL_DESIGN.md`](DATA_MODEL_DESIGN.md).

## Index categories

Firefly uses four kinds of index, all implemented as native Aerospike
secondary indexes:

| Category                                              | Indexed bin                                               | Created when                                                                     | Purpose                                          |
| ----------------------------------------------------- | --------------------------------------------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------ |
| Supernode traversal indexes (`E_IN_IDX`, `E_OUT_IDX`) | `SUPERNODE_IN` / `SUPERNODE_OUT` map keys on the edge set | Always                                                                           | Support traversal of supernode adjacency data    |
| TTL indexes (`TTL_V_IDX`, `TTL_E_IDX`)                | TTL bin on vertex / edge set                              | When `aerospike.graph.ttl.enabled=true`                                          | Background sweep of expired records              |
| Vertex label index (`V_LABEL_IDX`)                    | `LABEL` bin on vertex set                                 | When `aerospike.graph.index.vertex.label.enabled=true`                           | `g.V().hasLabel(x)`                              |
| Vertex property indexes                               | `VP_DATA` bin on vertex set                               | Per-key, from configuration or the `sindex` service                              | Property lookups by key/value                    |
| Vertex expression (compound) indexes                  | Expression over `VP_DATA`                                 | From configuration; requires Aerospike ≥ the expression-index-supporting version | Multi-key equality lookups evaluated server-side |

Adjacency and TTL indexes are internal plumbing; vertex label, property,
and expression indexes are user-configurable.

## Vertex property indexes

Configured through these properties:

| Property                                          | Meaning                                                        |
| ------------------------------------------------- | -------------------------------------------------------------- |
| `aerospike.graph.index.vertex.properties`         | Create both `STRING` and `NUMERIC` indexes for each listed key |
| `aerospike.graph.index.vertex.properties.string`  | Create only the `STRING` index for each listed key             |
| `aerospike.graph.index.vertex.properties.numeric` | Create only the `NUMERIC` index for each listed key            |
| `aerospike.graph.index.vertex.compound`           | Comma-separated expression-index definitions                   |

Vertex property indexes target the `VP_DATA` map bin with
`IndexCollectionType.MAPKEYS` and one of `IndexType.STRING` or
`IndexType.NUMERIC`. Each configured key produces an index named
`<vp_index_prefix>_<key>_<STRING|NUMERIC>` (the prefix is graph-ID
scoped, see `AerospikeConnection.getVpIndexPrefix()`).

The index metadata is refreshed by a background task
(`FireflyIndexMetadata`) so indexes created after startup: for example
via the `sindex` service: become usable automatically without
restarting the graph.

### Supported predicates

| Value type | Equality (`eq`)       | Range (`gt`, `gte`, `lt`, `lte`, `between`) | Substring |
| ---------- | --------------------- | ------------------------------------------- | --------- |
| String     | Yes (`STRING` index)  | No                                          | No        |
| Number     | Yes (`NUMERIC` index) | Yes (`NUMERIC` index)                       | N/A       |

Substring / regex filtering always falls back to an unindexed scan.

### Query planning

Index use is driven by the shape of the traversal:

* `g.V().has(key, value)` / `g.V().hasLabel(label).has(key, value)`:
  the `FireflyGraphStepStrategy` folds trailing `has`/`hasLabel` steps
  into the graph step, and then
  `FireflyBatchReadHelper.getHasContainersWithCardinalityOrder` sorts
  the has-containers by observed cardinality (from the periodically
  refreshed cardinality metadata) so the most selective indexed
  predicate runs first. Remaining has-containers are applied
  server-side via a `filterExp`.
* `g.V(id)...`: a direct `~id` lookup is always a point read; no
  secondary index is involved.
* `g.V().has("~label", x)` and `g.V().hasLabel(x)` both resolve to the
  same label-index path when the vertex label index is enabled.

### Expression (compound) indexes

Expression indexes let multiple equality predicates be evaluated by the
server in a single index probe. They require Aerospike server support
for expression indexes (`FireflyAerospikeVersionCheck.supportsExpressionIndexes`)
and are opt-in via `aerospike.graph.index.vertex.compound`. At query
time, `FireflyIndexMetadata.getMatchingExpressionIndex` picks the best
matching expression index for a set of `has` predicates.

## Vertex label index

Enabled with `aerospike.graph.index.vertex.label.enabled=true`. It
indexes the `LABEL` bin on the vertex set as `NUMERIC` (vertex labels
are interned to `Long`: see [`DATA_MODEL_DESIGN.md`](DATA_MODEL_DESIGN.md#schema-interning)).
Used by `g.V().hasLabel(x)` and by any downstream step that folds a
label constraint into an eligible index query.

## Edge indexes are not supported

The two edge-facing index knobs are reserved but not implemented:

* `aerospike.graph.index.edge.properties`: setting this on graph
  startup raises `RuntimeException("Edge property indexes are not
  currently supported.")`.
* `aerospike.graph.index.edge.label.enabled`: attempting to create an
  edge label index raises `RuntimeException("Edge indexes are not
  currently supported.")`.

Edge lookups go through the adjacency indexes via a source vertex, or
via a direct `~id` read on the edge record. Global edge scans
(`g.E().has(...)`) always fall back to a full edge-set scan.

## Adjacency and TTL indexes

These are internal and users don't configure them directly:

* `E_IN_IDX` / `E_OUT_IDX`: `STRING` map-key indexes on `SUPERNODE_IN` and `SUPERNODE_OUT`
  bins of the edge set. They are used for supernode traversals; normal
  vertices use their on-record `IN_EDGES` and `OUT_EDGES` adjacency caches.
* `TTL_V_IDX` / `TTL_E_IDX`: `NUMERIC` indexes on the TTL bin of the
  vertex and edge sets. Created only when TTL is enabled. Used by the
  background TTL sweeper.

## Operational notes

* Index creation is asynchronous. `createIndexBackground` submits an
  `sindex-create` info command and returns; the index is not usable
  until Aerospike reports it as built.
* For bulk load: only the first bulk-loader graph initialization in a
  batch actually issues index-create requests (`db.shouldCreateIndexes()`).
  This avoids spamming `sindex-create` thousands of times.
* For operational scripts, the `sindex` service
  (`com.aerospike.firefly.process.call.sindex`) exposes
  `createVertexPropertyIndex` / `dropVertexPropertyIndex` /
  `createVertexLabelIndex` / `dropVertexLabelIndex` at runtime.

## See also

* [`DATA_MODEL_DESIGN.md`](DATA_MODEL_DESIGN.md): record and bin layout
  that these indexes are built over.
* [`ID_MANAGEMENT.md`](ID_MANAGEMENT.md): why `~id` is a point read and
  property-named `id` isn't.
