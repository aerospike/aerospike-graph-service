## Data Model Design

This document describes how Aerospike Graph Service (codename `firefly`)
lays out graph data on top of Aerospike. It is the reference for anyone
debugging on-disk records, writing bulk-loader input, reasoning about
storage cost, or planning a migration.

Everything below describes the `packed` data model, which is the only
data model supported in v0.x+. The configuration key
`aerospike.graph.data.model` exists and defaults to `"packed"`; no other
value is currently accepted by the runtime. Historical references to a
`linked` layout have been removed: they are not relevant to anything
shipping today.

### Design goals

The `packed` model was designed around four properties that fall
directly out of Aerospike's storage engine:

1. **Traversal hops resolve in one read, not N.** An Aerospike single-
   record operation returns all bins for a record in one round trip;
   that record can hold tens of kilobytes of CDT (collection data type)
   state. We store **adjacency on the vertex record itself** whenever
   we can, so `g.V(v).out('knows')` does not need a secondary lookup to
   discover which edges exist before fetching them.
2. **Edges are amortized across records, not record-per-edge.** An
   edge is _not_ its own Aerospike record by default. Up to
   `aerospike.graph.phat.edge.size` edges (default 10; allowed 1 – 100)
   share a single "packed edge" record: i.e. N logical edges become
   N/10 Aerospike reads for bulk edge-property fetches.
3. **Supernodes stay correct without crushing the fast path.** Vertices
   whose on-record adjacency would exceed the Aerospike record-size
   budget transparently degrade: the edge cache is disabled on the
   vertex, and the edge records themselves grow a reverse-adjacency
   map keyed by the supernode's id-hash. A secondary index on that map
   preserves `Direction.OUT` / `Direction.IN` lookups in O(matching
   edges) rather than O(edges touching the supernode).
4. **Labels and property keys are interned per graph.** Every label
   and property name is mapped to a small `Long` the first time it is
   seen and stored by that integer from then on. String names only
   appear in the schema record; they never repeat inside vertex or
   edge data.

### Aerospike namespace layout

Everything for a single graph lives in one Aerospike namespace, split
across a well-defined set of **sets** and **bins**. The table below
lists the main sets; the names are the short "english" forms emitted
by `ConfigurationHelper.Keys.Sets`.

| Set | Purpose |
|---|---|
| `VERTICES` | One record per vertex. Keyed by the user-supplied vertex id (or its hash). |
| `EDGES` | One record per _packed edge_, i.e. per group of up to `phat.edge.size` edges. Keyed by `storageId = floorDiv(packingId, phat.edge.size)`. |
| `IN_VP` / `OUT_VP` | Vertex-property spill sets. Used when a vertex property's value carries additional meta-properties that don't fit inline. |
| `SCHEMA` | Interning tables: vertex-label → Long, edge-label → Long, vertex-property-key → Long, edge-property-key → Long, vp-property-key → Long. |
| `ID_MANAGER` | Monotonic id counters and recycle buffers for vertex, edge, and vertex-property ids. |
| `METADATA` | Per-graph metadata: data-model name (`"packed"`), data-model version, first-boot marker. Used for cross-version compatibility checks. |
| `SUMMARY` | Background-updated counters used by the optimizer: label cardinality, property presence, supernode counts. |
| `INDEX_METADATA` | Descriptor records for user-defined property indexes. |
| `USAGE_STATS_SET` | Opt-in runtime statistics. |
| `OLAP_TEMP`, `OLAP_ALGORITHM_TEMP`, `OLAP_JOBS` | Scratch space and job records used by the Spark `GraphComputer`. |
| `BL_*` | Bulk-loader staging: duplicate-vid records, recovery state, bad-entry / bad-edge records. Only populated when the bulk loader runs. |

Bins inside `VERTICES` and `EDGES` records are short English strings
(`"LABEL"`, `"VP_DATA"`, `"IN_EDGES"`, `"EDGE_DATA"`, …) emitted by
`ConfigurationHelper.Keys.Bins`. The numeric byte codes on each enum
member are **not** used as bin names: they're reserved for future
binary-compact mode and are safe to ignore when reading records today.

### Schema interning

Aerospike's map CDT keys and values are MessagePack-encoded on the
wire, so every byte spent on a repeated string like `"name"` inside
every vertex is wire cost, storage cost, and CPU cost that never goes
away. The engine avoids that by assigning each distinct label /
property-key a small `Long` id the first time it is written and then
using that id everywhere on disk.

The mapping lives in the `SCHEMA` set, one record per kind:

| Kind | Record key (token) | First id | Subsequent ids |
|---|---|---|---|
| Vertex label | `_vxlsch` | `0` | `1, 2, 3, …` |
| Vertex property key | `_vxpsch` | `-32` | `-31, -30, …` |
| Vertex-property property key | `_vppsch` | `-32` | `-31, -30, …` |
| Edge label | `_elsch` | `-32` | `-31, -30, …` |
| Edge property key | `_epsch` | `-30` | `-29, -28, …` (`-31` and `-32` pre-reserved; not allocated from the counter) |

Each kind has its own counter bin (`COUNTER`) that tracks the next
available id. The counter walks **toward positive** for every kind:
`0, 1, 2, …` for vertex labels, `-32, -31, -30, …` for everything else.
Keeping vertex labels positive lets them sit in a plain numeric
secondary index on the vertex record's `LABEL` bin without colliding
with the negative-id space used by every other interning domain.

For edge properties specifically, **id `-32` is reserved for `T.label`**
and **id `-31` is reserved for the adjacent vertex id**. Both are
written at `SchemaManager` construction time and never appear in the
counter sequence: the counter for this kind starts at `-30`. These
reserved ids are what makes the supernode adjacency bin
(`SUPERNODE_IN` / `SUPERNODE_OUT`) readable with fixed, well-known map
keys instead of having to look up interned ids for label / adjacent-id
on every read.

The interning uses an Aerospike expression that does two things
atomically:

1. Increment a counter bin (`COUNTER`) iff the string is not already
   present in the schema map bin (`SCHEMA`).
2. Insert `(string → counter)` into `SCHEMA` with
   `MapWriteFlags.CREATE_ONLY`.

Concurrent writers racing to intern the same string therefore produce
**exactly one** schema assignment; the loser observes the winner's
entry on the next read. The in-memory `BiMap<String, Long>` in
`SchemaManager` is a cache; it is rebuilt opportunistically from the
`SCHEMA` record when a read misses.

The central `COUNTER` is monotonic across all schema growth; ids are
never reused. Schema assignments are permanent: a label `"person"`
that was assigned id `5` keeps id `5` forever, even if every
`person` vertex is deleted. This is required for correctness: on-disk
edge and vertex records reference those ids directly.

### Vertex records

A vertex is one Aerospike record in the `VERTICES` set. The record key
is derived from the user-supplied vertex id (or from an internally
generated `Long` id, if the user didn't supply one).

Relevant bins on a vertex record:

| Bin | Type | Purpose |
|---|---|---|
| `LABEL` | `Long` | Interned vertex label id (from `SCHEMA`). |
| `USER_KEY` | scalar | The user-supplied id, preserved verbatim so we can return it unchanged from `Element.id()`. |
| `ID_TYPE` | `Long` | Which Java type the user's id is: 1=Long, 2=Integer, 5=String. |
| `VP_DATA` | `Map<Long, Map<Object, List<Long>>>` | Vertex properties. Outer key = interned property key; inner map = value → list of vertex-property-ids (one id per cardinality-set instance). |
| `VP_HINTS` | `Map<Long, Map<Long, Object>>` | Type hints for non-obvious values: key = interned property key, inner-key = vertex-property-id, value = serialized type hint. Used so e.g. a `Date` round-trips as a `Date`, not a `Long`. |
| `VP_PROPERTIES` | nested `Map` | Vertex-property meta-properties (TinkerPop allows properties on properties). Empty map when unused. |
| `GEO_DATA` | `Map<Long, List<String>>` | Geospatial vertex properties stored as GeoJSON Point strings. Outer key = interned geo property key from `_geosch`. Geo cannot live in `VP_DATA` because Aerospike rejects GeoJSON as a map key. Meta-properties on geo properties are not supported. |
| `IN_EDGES` | `Map<String, List<Long>>` | On-record adjacency cache: edge-label → list of (cached) edge ids, for inbound edges. |
| `OUT_EDGES` | `Map<String, List<Long>>` | Same as `IN_EDGES`, outbound. |
| `ECACHE_OFF` | `boolean` | If true, this vertex is a supernode: `IN_EDGES` / `OUT_EDGES` are abandoned and the adjacency lives on the edge records instead. |
| `TTL` | `Long` | Optional per-record expiration, populated only when TTL is in use. |

The `IN_EDGES` / `OUT_EDGES` bins are the thing that lets a single-hop
traversal finish in one vertex read plus one packed-edge read. When the
cache is populated:

- `g.V(v).outE('knows')` reads the vertex record, extracts
  `OUT_EDGES.get('knows')`, and issues a batch-read for the packed-edge
  records those ids resolve to. At default settings that is 1 + ⌈k/10⌉
  Aerospike reads for `k` outbound `knows` edges.
- `g.V(v).out('knows')` does the same and then issues one more batch
  of reads to fetch the destination vertices.

### Edge ids and packed-edge records

An edge has an 8- or 16-byte id:

- **8 bytes** = `packingId` only. The `uniqueId` is defined to equal
  the `packingId`. This is the common case for non-recycled ids.
- **16 bytes** = 8 bytes `packingId` + 8 bytes `uniqueId`. Produced
  when the id allocator reuses a packing slot that was freed by an
  earlier edge deletion.

The two halves map to Aerospike like this:

- `storageId = floorDiv(packingId, phat.edge.size)`: this is the
  Aerospike record key in the `EDGES` set. The bulk-loader and the
  runtime pick edge ids so that up to `phat.edge.size` edges fall into
  the same `storageId`.
- `uniqueId` (really: the full 8- or 16-byte id): this is the _map
  key_ inside the packed-edge record's `EDGE_DATA` bin that identifies
  which edge you're talking about.

At the default `phat.edge.size = 10`, 10 edges share each Aerospike
record. Bulk workloads that stream lots of edges see a 10× reduction
in Aerospike record count (and hence Aerospike-side RAM metadata) for
edge storage. Single-hop point reads over the graph see a 10×
reduction in Aerospike round-trips per N edges of the same vertex.

Tuning note: `phat.edge.size` is an **immutable config**: it is
baked into every edge id the moment that id is allocated, so it cannot
be changed on an existing graph. Valid range 1 – 100. Larger values
mean fewer records and cheaper bulk reads, but more contention when
many writers are concurrently appending edges to the same packed
record (because Aerospike serializes writes to the same record).

### Edge records

Relevant bins on a packed-edge record:

| Bin | Type | Purpose |
|---|---|---|
| `EDGE_DATA` | `Map<ByteBuffer, List<Object>>` or `Map<ByteBuffer, Map<Long, Object>>` | Per-edge data, keyed by the full edge id bytes. Value shape depends on whether the edge is attached to a supernode: see below. |
| `SUPERNODE_IN` | nested `Map` | Reverse-adjacency map populated only for edges whose `inVertex` is a supernode. Schema: `edgeId → {vertexIdHash → {labelKey → labelValue, adjIdKey → adjVertexId, propertyKey1 → value1, …}}`. |
| `SUPERNODE_OUT` | same | Same, for edges whose `outVertex` is a supernode. |
| `TTL` | `Map<ByteBuffer, Long>` | Per-edge expiration if TTL is enabled. |

`EDGE_DATA` has two shapes:

1. **Non-supernode edges.** The value is a 5-element `List`:

   | Position | Constant | Content |
   |---|---|---|
   | 0 | `LABEL_POSITION` | Interned edge-label id (`Long`). |
   | 1 | `IN_V_POSITION` | Inbound vertex's user id, verbatim. |
   | 2 | `OUT_V_POSITION` | Outbound vertex's user id, verbatim. |
   | 3 | `PROPERTIES_POSITION` | `TreeMap<Long, Object>` of interned-key → value. |
   | 4 | `TYPE_HINTS_POSITION` | `HashMap<Long, Object>` of interned-key → type hint. |

2. **Supernode-attached edges.** The `IN_V` / `OUT_V` / `properties`
   content is redundant with what's in `SUPERNODE_IN` / `SUPERNODE_OUT`
   (because the supernode indexes live there), so `EDGE_DATA` stores
   only the `TYPE_HINTS` map. The label and endpoints are reconstructed
   from the supernode bin at read time.

All on-disk keys in property / type-hint maps are the interned
`Long`s, never strings. All interned-key lookups go through
`SchemaManager.getEdgePropertyString(long)` to convert back to TinkerPop
names on the way out.

### Supernode handling

A vertex is a supernode when on-record adjacency (`IN_EDGES` /
`OUT_EDGES`) would overflow the Aerospike record-size budget. The
threshold is controlled by `aerospike.graph.vertex.edge.cache.size`
(internal name `ON_RECORD_ID_LIMIT`); by default it is computed
dynamically as ~45% of the namespace's configured max record size,
capped at 90%. When multi-record transactions are enabled (`mrt` /
`transactions`), the cap is further tightened to **1023** because an
MRT can touch at most 4096 records and a vertex drop has to touch
`2·edges + 1` records.

Transition to supernode state is one-way per vertex, at least within a
data-model version:

1. A write detects that appending the new edge id would overflow the
   vertex record.
2. The vertex's `ECACHE_OFF` bin is set to `true` via a separate
   `UPDATE_ONLY` operation. Existing `IN_EDGES` / `OUT_EDGES` content
   is _not_ migrated away. It is ignored from that point on.
3. Every subsequent edge touching that vertex writes into the edge
   record's `SUPERNODE_IN` / `SUPERNODE_OUT` bin, keyed by the
   vertex's id-hash.

A pair of secondary indexes back this up:

- `E_IN_IDX`: `STRING` secondary index on the `SUPERNODE_IN` bin,
  collection type `MAPKEYS`. Lets `Direction.IN` lookups find all
  packed-edge records that reference a given supernode hash as an
  inbound endpoint.
- `E_OUT_IDX`: same, for `SUPERNODE_OUT`.

Reads against a supernode therefore do:

1. An `sindex` scan with `Filter.contains(supernodesOutBin, MAPKEYS,
   vertexHash)` to find packed-edge records touching that supernode.
2. A per-record read of `EDGE_DATA` + `SUPERNODE_OUT` to extract the
   edges. Predicate pushdown against the supernode map (label, adjacent
   id, properties) happens on the Aerospike server via
   `EdgeQueryHelper.phatEdgeHasContainerListToExpression`: so
   `g.V(supernode).outE('knows').has('weight', gt(0.5))` does not ship
   non-matching edges back to the JVM.

The `SUMMARY` set keeps a cardinality counter that tracks how many
supernodes exist per label; this is what the optimizer uses to decide
between a supernode scan and a vertex-record read.

### Vertex-property spill

Most vertex properties sit inline on the vertex record, inside `VP_DATA`
/ `VP_HINTS` / `VP_PROPERTIES`. For vertex properties that have their
own set of meta-properties large enough to risk overflowing the vertex
record (TinkerPop cardinality `SET` / `LIST` with many meta-properties
per element), the data spills into the `IN_VP` / `OUT_VP` sets, keyed
by the vertex-property id. The in-memory `FireflyVertex.vpProperties`
hides the split from the rest of the engine.

Vertex-property ids themselves come from the `ID_MANAGER` set, using
a `DecrementingNumericIdManager`: i.e. vertex-property ids are
guaranteed distinct from vertex ids (which decrement from a different
counter) and from edge packing ids (which increment).

### Id allocation

Each logical id kind has its own counter in the `ID_MANAGER` set:

| Id kind | Counter | Direction | Buffered |
|---|---|---|---|
| Vertex id (internal) | `_vxidctr` | Decrementing | Yes (`vertex.id.buffer.size`, default 100) |
| Edge packing id | `_epidctr` | Incrementing, grouped by `phat.edge.size` | Yes |
| Edge unique id | `_euidctr` | Incrementing | Yes |
| Vertex-property id | `_vxpidctr` | Decrementing | Yes (`property.id.buffer.size`) |

`RecyclingBufferedNumericIdManager` (or `MrtRecyclingBufferedNumericIdManager`
when MRT is on) lets the engine pull a block of ids out of the
counter, hand them out to local writers, and top the buffer up when it
runs low. Recycling is lossy-on-crash by design: a killed server
forfeits its outstanding buffer rather than risk double-issuing an id.

When MRT is enabled, edge-packing ids have a second layer: recycled
packing ids are tracked in a dedicated recycle buffer
(`edge.id.recycle.buffer.size`) because aborted transactions may have
committed new edges into a half-full pack.

### Per-graph metadata and version compatibility

The `METADATA` set holds a single record per graph with two fields
read by `DataModelVersioning.checkVersionCompatibility`:

- **Data-model name.** Currently always `"packed"`. The constant lives
  at `FireflyGraph.DATA_MODEL`.
- **Data-model version.** The current engine version
  (`FireflyGraph.FIREFLY_VERSION`) serialized as a
  `ComparableVersion`.

On every boot the engine compares the on-disk version against its own:

- If both sides are null, this is a fresh namespace: the engine writes
  its own name/version and proceeds.
- Major versions **must match exactly**. A mismatch throws
  `DataModelVersionMismatchException` and refuses to start; migrations
  are out-of-band.
- The engine's minor version must be **greater than or equal to** the
  on-disk minor. Booting an older minor against a newer on-disk record
  is rejected; the reverse is allowed (new engine, older data), so
  rolling upgrades can stay live through a minor bump.

This check runs before any user traversal and is the only place a
version mismatch can cause a startup to fail, so incompatible upgrades
fail fast and loudly rather than corrupting data.

### Secondary indexes

The engine maintains the following Aerospike secondary indexes,
scoped to the namespace. `E_IN_IDX` / `E_OUT_IDX` are always created
(the engine refuses to start without them); the rest are conditional
on the listed flags.

| Index | Set | Bin | Type | Collection | Conditional on | Purpose |
|---|---|---|---|---|---|---|
| `E_IN_IDX` | `EDGES` | `SUPERNODE_IN` | STRING | MAPKEYS | always | Supernode inbound traversal |
| `E_OUT_IDX` | `EDGES` | `SUPERNODE_OUT` | STRING | MAPKEYS | always | Supernode outbound traversal |
| `V_LABEL_IDX` | `VERTICES` | `LABEL` | NUMERIC | DEFAULT | `aerospike.graph.index.vertex.label.enabled=true` | `g.V().hasLabel(x)` without scan |
| `TTL_V_IDX` | `VERTICES` | `TTL` | NUMERIC | DEFAULT | `aerospike.graph.ttl.enabled=true` | TTL sweeps for vertex records |
| `TTL_E_IDX` | `EDGES` | `TTL` | NUMERIC | MAPVALUES | `aerospike.graph.ttl.enabled=true` | TTL sweeps for per-edge expirations |

Edge-label indexes are explicitly unsupported (the enable flag
currently throws at startup, tracked as a future enhancement). Today,
`g.E().hasLabel(...)` uses either the supernode indexes (if the edges
touch a supernode) or falls back to a scan of the `EDGES` set with a
pushdown filter on the label position inside `EDGE_DATA`.

User-defined property indexes are created via the admin API
(`aerospike.graph.admin.create-index`) and stored against the
`INDEX_METADATA` set; they're separate from the built-in ones above
and can be added / dropped without restarting the engine. See
[`INDEX_DESIGN.md`](INDEX_DESIGN.md) for the full index subsystem.

### Related code

All of the above is implemented in `aerospike-graph-gremlin`. The
highest-value entry points for reading the code:

- `com.aerospike.firefly.io.aerospike.AerospikeOperations#writeVertex`
 : vertex-write pipeline, including the on-record edge cache shape.
- `com.aerospike.firefly.io.aerospike.AerospikeOperations#writeEdgeToRecord`
 : packed-edge write pipeline, including the non-supernode vs.
  supernode `EDGE_DATA` shape.
- `com.aerospike.firefly.io.aerospike.AerospikeOperations#createFilterableSupernodeOperations`
 : the supernode `SUPERNODE_IN` / `SUPERNODE_OUT` write path.
- `com.aerospike.firefly.io.aerospike.schema.SchemaManager`: label /
  property-key interning and reserved ids.
- `com.aerospike.firefly.io.aerospike.DataModelVersioning`: the
  startup compatibility check.
- `com.aerospike.firefly.structure.id.FireflyPhatEdgeId`: edge-id
  shape and `storageId` derivation.
- `com.aerospike.firefly.util.config.ConfigurationHelper.Keys.{Sets,Bins,InternalConfigs}`
 : the authoritative list of set and bin names.

### See also

- [`INDEX_DESIGN.md`](INDEX_DESIGN.md): the user-facing index
  subsystem built on top of this data model.
- [`ID_MANAGEMENT.md`](ID_MANAGEMENT.md): id allocation in detail,
  including the recycling buffer and MRT interactions.
- [`SUPERNODE_FLAG.md`](SUPERNODE_FLAG.md): operational handling of
  supernodes from the user's perspective.
- [`BULK_LOADER_DESIGN.md`](BULK_LOADER_DESIGN.md): how the Spark
  bulk loader writes directly into the packed-edge format described
  above, bypassing the online write path.
- [`GRAPH_MAX_MEMORY.md`](GRAPH_MAX_MEMORY.md): record-size budget
  and its interaction with `ECACHE_OFF` transitions.
