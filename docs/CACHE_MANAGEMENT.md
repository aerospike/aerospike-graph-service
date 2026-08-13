# Cache Management

Aerospike Graph Service provides cache management services to control caching behavior at runtime.

Official overview: [Caching](https://aerospike.com/docs/graph/manage/cache).

## Cache Coverage

AGS cache stores Aerospike graph records that are read during traversal execution:

- Vertex records
- Edge records

Query result sets are not cached as reusable "result cache" objects. Cache entries are record-level objects used by traversal execution.

Record caching is split into two internal caches:

- Full-record cache: records read with full properties.
- Empty-properties cache: records read without properties (lightweight reads).

In `TRANSACTIONAL` mode these are per request (`transactionalCache`, `transactionalEmptyPropsCache`).
In `GLOBAL` mode these are process-shared per graph instance (`globalCache`, `globalEmptyPropsCache`).

In `GLOBAL` mode, AGS also maintains a supernode edge-ID cache used by supernode traversals:

- Key: `(vertexId, direction, labels)`
- Value: `List<edgeId>`
- Scope: GLOBAL mode only
- Capacity control: bounded by `cache_weight` as cache entry count for this specific cache

## When To Use GLOBAL Cache

GLOBAL cache mode is most appropriate when **all** of the following are true:

1. Workloads are read-only, or read-write operations are independent enough that shared cached reads do not introduce correctness risks for your use case.
2. The hot working set can fit in memory, so AGS instances should be sized with enough RAM to hold the cache effectively.
3. Aerospike database access is the current performance bottleneck, and reducing backend reads is expected to improve end-to-end latency/throughput.
4. There is available RAM on AGS instances to allocate to process-level caching without putting memory pressure on other workloads.

## Cache Modes

The cache supports two modes:

- **TRANSACTIONAL** (default): Thread-local caches that are reset on each new traversal. In AGS terms, this is effectively per request/query execution.
- **GLOBAL**: Caches shared across threads within a graph instance. Caches are not reset between traversals. Best for read-heavy workloads with repeated access to the same data.

Each graph instance maintains its own caches. Switching cache modes on one graph does not affect caches in other graphs.

## Cache Weight

The `cache_weight` parameter specifies cache **weight units** (not raw bytes). This value is also persisted in the `aerospike.graph.cache.weight` configuration option.

For record caches, one weight unit is approximately 200 bytes (used for estimation only). Actual memory depends on record shape and cache contents.
`weighted_size` reports current consumed weight units. `cache_weight` is the configured limit in weight units.
`estimated_memory_bytes` is derived from cache internals as an estimate and should be treated as directional, not exact heap accounting.

- Default for TRANSACTIONAL: 1,000,000 weight units
- Default for GLOBAL: 20,000,000 weight units

## Write And Invalidation Semantics

When writes are executed through AGS traversals, cache behavior differs by mode:

- TRANSACTIONAL: cache is per request and reset per request, so stale carry-over risk is minimal.
- GLOBAL: AGS does not provide strict global coherence guarantees across all write/read interleavings.

If data changes outside AGS (another service writing to Aerospike), AGS does not automatically track those changes in any cache mode. Use `admin/cache/reset` when you need a hard refresh.

## Runtime Persistence

Cache mode and cache weight changes made via admin services are persisted in graph configuration and are retained across AGS restart.

## Getting Started

Recommended onboarding workflow:

1. Start with default `TRANSACTIONAL` mode and default `cache_weight`.
2. Measure baseline latency and backend load.
3. Switch to `GLOBAL` only when all criteria from `When To Use GLOBAL Cache` are true.
4. Observe `weighted_size`, `estimated_entry_count`, hit/miss counts, and memory estimates.
5. Increase `cache_weight` only as needed.
6. Use `reset` operationally when external writers may have changed hot data.

## Configuration Keys

Cache management keys to document in config reference:

| Key | Default | Allowed values | Notes |
|-----|---------|----------------|-------|
| `aerospike.graph.cache.mode` | `TRANSACTIONAL` | `TRANSACTIONAL`, `GLOBAL` | Can be changed at runtime via admin service. |
| `aerospike.graph.cache.weight` | `1000000` | Integer `>= 1` | Weight units, not bytes. Runtime changes persist. |

## Admin Services

### Get Cache Status

Returns the current cache mode and usage statistics.

```gremlin
g.call("aerospike.graph.admin.cache.status").next()
```

Requires the `READ` role.

Returns a Map with the following fields:
- `mode`: Current cache mode (TRANSACTIONAL or GLOBAL)
- `cache_weight`: Current cache weight in weight units
- `estimated_entry_count`: Approximate number of cached records (entries)
- `weighted_size`: Total weighted size in weight units
- `estimated_memory_bytes`: Estimated memory usage in bytes
- `estimated_memory_formatted`: Human-readable memory usage (e.g., "1.5 MB")
- `hit_count`: Number of cache hits
- `miss_count`: Number of cache misses

In `GLOBAL` mode the status also includes the supernode edge-ID cache
counters:
- `supernode_edge_cache_entries`: Number of entries in the supernode edge-ID cache
- `supernode_edge_cache_edge_ids`: Total edge IDs held across those entries
- `supernode_edge_cache_memory_bytes`: Estimated memory usage in bytes
- `supernode_edge_cache_hits`: Supernode edge-ID cache hits
- `supernode_edge_cache_misses`: Supernode edge-ID cache misses

Example output:
```
{
    'mode': 'TRANSACTIONAL',
    'cache_weight': 1000000,
    'estimated_entry_count': 0,
    'weighted_size': 0,
    'estimated_memory_bytes': 0,
    'estimated_memory_formatted': '0 B',
    'hit_count': 0,
    'miss_count': 0
}
```

### Set Cache Mode

Changes the cache mode at runtime. Requires the `ADMIN` role.

```gremlin
// Switch to GLOBAL mode with default weight (20000000)
g.call("aerospike.graph.admin.cache.set-mode")
  .with("mode", "GLOBAL")
  .next()

// Switch to GLOBAL mode with custom weight
g.call("aerospike.graph.admin.cache.set-mode")
  .with("mode", "GLOBAL")
  .with("cache_weight", "50000000")
  .next()

// Switch back to TRANSACTIONAL mode
g.call("aerospike.graph.admin.cache.set-mode")
  .with("mode", "TRANSACTIONAL")
  .next()
```

Parameters:
- `mode` (required): The cache mode to set. Valid values: `TRANSACTIONAL` or `GLOBAL` (case-insensitive)
- `cache_weight` (optional): Cache weight units (not raw bytes). Default: 1000000 for TRANSACTIONAL, 20000000 for GLOBAL

Returns a Map with the following fields:
- `status`: "success" if the operation completed
- `previous_mode`: The cache mode before the change
- `current_mode`: The new cache mode
- `cache_weight`: The cache weight being used (weight units)

Example output:
```
{
    'status': 'success',
    'previous_mode': 'TRANSACTIONAL',
    'current_mode': 'GLOBAL',
    'cache_weight': 20000000
}
```

**Note:** When switching modes:
- From GLOBAL to TRANSACTIONAL: Global caches are cleared
- From TRANSACTIONAL to GLOBAL: Transactional caches are cleared and global caches are initialized

### Reset Cache

Clears and reinitializes the current caches. Requires the `ADMIN` role.

```gremlin
g.call("aerospike.graph.admin.cache.reset").next()
```

Returns a Map with the following fields:
- `status`: "success" if the operation completed
- `mode`: The current cache mode
- `previous_entry_count`: Entry count before reset
- `previous_weighted_size`: Weighted size before reset
- `current_entry_count`: Entry count after reset (should be 0)
- `current_weighted_size`: Weighted size after reset (should be 0)

Example output:
```
{
    'status': 'success',
    'mode': 'GLOBAL',
    'previous_entry_count': 1523,
    'previous_weighted_size': 45690,
    'current_entry_count': 0,
    'current_weighted_size': 0
}
```

## HTTP Endpoints

The cache services are also available via HTTP REST endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /<graphId>/admin/cache/status` | Get cache status |
| `GET /<graphId>/admin/cache/set-mode?mode=GLOBAL` | Set cache mode |
| `GET /<graphId>/admin/cache/set-mode?mode=GLOBAL&cache_weight=50000000` | Set cache mode with custom weight |
| `GET /<graphId>/admin/cache/reset` | Reset cache |

## Use Cases

### High-Read Workloads

For applications with repeated reads of the same vertices/edges, GLOBAL mode can significantly improve performance:

```gremlin
// Enable global caching
g.call("aerospike.graph.admin.cache.set-mode")
  .with("mode", "GLOBAL")
  .with("cache_weight", "50000000")
  .next()

// Now repeated traversals will benefit from cached data
g.V().has("name", "John").out("knows").toList()  // First call populates cache
g.V().has("name", "John").out("knows").toList()  // Subsequent calls use cache
```

### Monitoring Cache Effectiveness

Monitor cache usage to tune the cache size:

```gremlin
// Check cache statistics
g.call("aerospike.graph.admin.cache.status").next()
```

If `estimated_entry_count` is consistently near the maximum or memory usage is high, consider increasing the `cache_weight`.
If `weighted_size` is consistently near `cache_weight`, consider increasing `cache_weight`.

### Cache Invalidation

When data changes externally, or you need to ensure fresh reads:

```gremlin
// Reset all caches
g.call("aerospike.graph.admin.cache.reset").next()
```
