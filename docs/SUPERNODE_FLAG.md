# The `~supernode` flag

`~supernode` is a virtual property you can set on a vertex to tell
Aerospike Graph Service, up front, that this vertex will be a supernode,
so do not keep its adjacency list inline on the vertex record.

## Background

Under the `packed` data model, a vertex record stores a small
"edge cache" of adjacency information inline
(`aerospike.graph.vertex.edge.cache.size`). When the cache fills up,
the vertex is promoted to a supernode: its adjacency moves to dedicated
`SUPERNODE_IN` / `SUPERNODE_OUT` bins and is resolved through the
`E_IN_IDX` / `E_OUT_IDX` secondary indexes instead.

The automatic path is fine, but it means every edge insert up to that
threshold is writing into, and eventually invalidating, the inline
cache before the promotion kicks in. For vertices you already know are
going to be high-degree (hubs, celebrities, categories, etc.), that's
wasted work. Setting `~supernode` bypasses it: the vertex is treated as
a supernode from the moment the flag is set.

See [`DATA_MODEL_DESIGN.md`](DATA_MODEL_DESIGN.md#supernode-handling)
for the underlying record layout.

## Usage

```groovy
Vertex v = g.addV("hub").next()
g.V(v.id()).property("~supernode", true).iterate()
```

From that point on, edge inserts on `v` skip the inline cache entirely
and go straight to the supernode path.

## Semantics

- **Write-only.** You cannot read `~supernode` back. Reading it returns
  nothing.
- **Value is ignored.** Any value you set: `true`, `"yes"`, `1`, `""`
 : is treated as "this vertex is a supernode". Only the presence of
  the key matters.
- **One-way.** Once set, it cannot be cleared. A vertex that has been
  marked as a supernode stays a supernode for the lifetime of the
  record. This matches the natural direction of the promotion: once
  adjacency has moved to the supernode bins, there is no cheap way to
  fold it back into an inline cache.

## When to use it

Set `~supernode` when you **know** a vertex will end up above the edge
cache threshold: ideally before you start bulk-inserting its edges.
Common cases:

- Bulk-loading a dataset where some nodes obviously dominate (a
  "category" vertex connected to millions of items, a user connected
  to all sessions, etc.).
- Hand-building a schema vertex or a hub that every other vertex
  attaches to.

If you're not sure, don't bother. The automatic promotion will do the
right thing. You would only be pre-optimizing.

## See also

* [`DATA_MODEL_DESIGN.md`](DATA_MODEL_DESIGN.md): how supernode
  adjacency is stored.
* [`INDEX_DESIGN.md`](INDEX_DESIGN.md): the `E_IN_IDX` / `E_OUT_IDX`
  indexes that supernode traversal relies on.
