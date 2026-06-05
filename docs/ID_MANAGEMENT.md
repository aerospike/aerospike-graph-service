# ID management

Firefly uses two distinct notions of ID:

* The **Gremlin-facing** `~id`: what `g.V(x)` or `g.E(y)` takes, and
  what `element.id()` returns. This is the stable identity a user or
  client deals with.
* The **storage** ID: what actually gets written into Aerospike record
  keys and bins. For vertices these are identical; for edges they are
  derived (see [Internals](#internals-for-curious-readers)).

The rest of this document explains the Gremlin semantics first, then
the internals for people reading the source.

## `~id` vs `id` in Gremlin

`~id` is a special keyword in Gremlin. It enables direct lookup of a
vertex or edge by its canonical identifier: a single-record Aerospike
read, not a secondary-index scan. `T.id` is an alias for `~id`.

- For **vertices**, `~id` may be supplied by the user or auto-generated
  by Firefly.
- For **edges**, `~id` is always auto-generated. It cannot be
  user-supplied.
- Plain `id` (without the leading tilde) is not a special keyword. It
  is an ordinary property, stored and looked up like any other.

## System-generated IDs

Auto-generated `~id` values start at `-1` and descend (`-1`, `-2`, …).
This keeps the positive-integer range fully available for user-supplied
IDs without any conflict-check overhead: if you hand-assign `1`, `2`,
`3`, the system will never collide with your range.

## Examples

```groovy
g.addV("person").property(T.id, 1).property("name", "Alice").iterate()
// Creates a vertex with label=person, ~id=1, name=Alice.

g.addV("person").property(T.id, 2).property("name", "Bob").iterate()
g.addV("person").property(T.id, 3).property("name", "Carol").iterate()

g.addV("person").property(T.id, 3).property("name", "Dave").iterate()
// Error: a vertex with ~id=3 already exists.

g.V(1).next()
// Returns Alice: direct record lookup on ~id=1.

g.V(1, 2, 3).toList()
// Returns [Alice, Bob, Carol]: single batch read of three records.

g.addV("person").property("id", 4).property("name", "Dave").iterate()
// Creates a vertex with property id=4 (ordinary property, not ~id).

g.V(4).next()
// Fails: there is no vertex with ~id=4.

g.V().has("id", 4).next()
// Returns Dave: requires a secondary-index query or scan.
```

## Takeaway

If you care about lookup latency, use `T.id` / `~id`. Properties named
`id` cost a scan or an index hit; `~id` is a point read.

---

## Internals for curious readers

These details are not part of the stable user contract: they're here
so someone reading the code has a map.

### Vertex IDs

A vertex's `~id` and its Aerospike record key are the same value. Any
`Long` or `String` (or anything coercible) accepted by Gremlin lands in
the record key verbatim.

Auto-generated vertex IDs come from a `DecrementingNumericIdManager`
backed by a single counter record in the graph's metadata set. The
counter starts at `-1` and is buffered client-side
(`RecyclingBufferedNumericIdManager`), so new-vertex inserts don't
round-trip for an ID on every call.

### Edge IDs

Edge `~id`s are always system-generated. An edge ID is *not* the same
value as the record key that stores it: it's a two-part logical ID:

```
edgeId = (packingId, uniqueId)

  packingId : identifies the group of edges sharing a phat-edge record
  uniqueId  : distinguishes edges within that group
```

The serialised form on the wire is either 8 bytes (`packingId` alone,
when `packingId == uniqueId`) or 16 bytes (`packingId || uniqueId`).
See `FireflyPhatEdgeId`.

The Aerospike record key for an edge: the *storage* ID: is derived:

```java
storageId = Math.floorDiv(packingId, phatEdgeSize)
```

where `phatEdgeSize` is the `aerospike.graph.phat.edge.size`
configuration. That's how Firefly packs multiple logical edges into a
single Aerospike record. See
[`DATA_MODEL_DESIGN.md`](DATA_MODEL_DESIGN.md#edge-ids-and-packed-edge-records)
for the full layout.

### Vertex-property IDs

Vertex-property records (used when a vertex's properties spill into a
dedicated record, or for multi-cardinality properties) have their own
ID counter, distinct from the vertex-ID and edge-ID spaces. It is
managed the same way as vertex IDs (decrementing, buffered) but from an
independent metadata record.

### Source

* `com.aerospike.firefly.structure.id.FireflyIdFactory`: parses and
  validates user-supplied IDs, routes to the right manager for
  auto-generated ones.
* `com.aerospike.firefly.structure.id.FireflyPhatEdgeId`: edge-ID
  encoding and the `packingId → storageId` derivation.
* `com.aerospike.firefly.io.aerospike.id.DecrementingNumericIdManager`,
  `...RecyclingBufferedNumericIdManager`: the counter plumbing.

## See also

* [`DATA_MODEL_DESIGN.md`](DATA_MODEL_DESIGN.md): record layout for
  vertices, edges, and phat edges.
* [`INDEX_DESIGN.md`](INDEX_DESIGN.md): what a non-`~id` lookup costs.
