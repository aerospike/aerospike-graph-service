# Write Consistency for Edges and Vertices

This document describes how the `packed` data model keeps edge and
vertex writes consistent, and the ordering and error-handling rules the
engine relies on. It reflects the current implementation in
`AerospikeOperations`.

## Background

Writing an edge touches more than one record:

- The edge record in the `EDGES` set (one record per _packed edge_).
- The on-record edge cache (`IN_EDGES` / `OUT_EDGES`) of each adjacent
  vertex, unless that vertex is a supernode, in which case adjacency
  lives on the edge record's `SUPERNODE_IN` / `SUPERNODE_OUT` maps
  instead.

Because these are separate records, a naive implementation could
observe partial state: an edge visible from one endpoint but not the
other, an edge cached on a vertex with no backing edge record, or an
edge orphaned onto a dropped vertex.

## Consistency goals

1. Never surface an edge in one direction but not the other.
2. Never corrupt existing data under concurrent writers.
3. Surface write failures to the caller so they can retry.
4. Surface the case where an adjacent vertex was dropped concurrently.

## How the engine meets these goals

There are two write paths, selected in `writeEdge` based on whether a
transaction is active.

### Transactional path (MRT or TinkerPop transactions)

When a multi-record transaction (`aerospike.graph.mrt.enabled`) or a
TinkerPop transaction (`aerospike.graph.tx.enabled`) is active, all
record writes for the edge are enrolled in the same Aerospike
transaction and commit atomically. Ordering does not matter for
correctness, so the edge record is written first to detect transaction
collisions early; the adjacent vertices are updated next, and the whole
set commits or aborts together. A collision on the target edge record
(`RECORD_TX_BLOCKED`) is handled by recycling the edge id, moving to a
new target record, and retrying. On any runtime error the transaction
is rolled back.

Because an MRT can touch at most 4096 records, the supernode promotion
threshold is tightened in this mode (see
[data model design](DATA_MODEL_DESIGN.md)).

### Non-transactional path

Without a transaction there is no atomic multi-record commit, so
consistency is achieved through **write ordering plus a read-time
existence check**:

- The edge is written into both adjacent vertices' edge caches first
  (`writeEdgeToVertex` for `IN` then `OUT`), and only then is the edge
  record written (`writeEdgeWithNoTransaction`).
- An edge is only ever surfaced to a caller if its edge record exists.
  Reads materialize edges from the `EDGES` records
  (`readEdges`), so a cache entry that points at a missing edge record
  is never returned as a live edge.

The consequence is that if the final edge-record write fails, the edge
simply does not exist as far as readers are concerned, even though an id
may remain in a vertex's edge cache. This preserves goal 1 (an edge is
either visible from both sides via its record, or not at all) and goal
2 (no existing data is corrupted). The failure is propagated to the
caller (goal 3); a record-size failure while appending to a vertex edge
cache is surfaced as a `VertexRecordSizeExceededException`.

The trade-off is a dangling id left in a vertex edge cache after a
failed edge-record write. It costs a few bytes and is inert because the
read path ignores ids with no backing edge record.

### Generation checks

Aerospike generation checks (`GenerationPolicy.EXPECT_GEN_EQUAL`) are
used narrowly, not as the general write-protection mechanism. The
current use is regenerating a supernode edge record during **edge
delete** (`removeEdge`): the record is re-read and rewritten under a
generation check, and the operation retries if a concurrent writer
changed the record in between. Most other writes use
`RecordExistsAction` constraints (`CREATE_ONLY` / `UPDATE_ONLY`) rather
than explicit generation checks.

## Removal ordering

### Edge removal

The edge record is removed (or, for supernode edges, its entry is
removed from the edge record) before the edge is removed from the
adjacent vertices' caches. Removing the backing record first means the
edge stops being visible immediately, satisfying goal 1. If a
subsequent cache cleanup fails, the leftover id is inert for the same
reason as above.

### Vertex removal

A vertex's incident edges are removed before the vertex record itself
(`removeVertex`). This guarantees no orphaned edges: if the final vertex
drop fails, the caller is told the removal did not complete and can
retry, and there are no edges pointing at a vertex that no longer
exists.
