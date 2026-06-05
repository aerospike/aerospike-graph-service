# Traversal cache

> Internal design note for contributors. Describes the
> `FireflyTraversalCacheStrategy` implementation in the query engine.

<img width="800" height="200" src="img/traversal_cache.drawio.svg" alt="Diagram of traversal cache flow from Gremlin traversal through cache to Aerospike reads and writes">

## Overview

`FireflyTraversalCacheStrategy` adds a traversal-specific Guava cache for
supported traversals.

For each supported traversal, the strategy assigns a unique ID, prepends a
step that carries that ID, and registers a cache in
`ConcurrentHashMap<UUID, TraversalCache>`.

## Lifecycle

A step at the end of the traversal removes the cache when the traversal
completes.

## Storage layer integration

The active traversal is exposed to the `AerospikeConnection` storage layer
through a thread-local. While that traversal is active, Aerospike reads and
writes are routed through its cache.

## Prefetch

When prefetch tasks are enabled, they run at the start of the traversal
(eagerly or in the background, depending on configuration).

Given a function from `(Traversal)` to `(approximate Aerospike record set)`,
a prefetch task tells the backend to load the records the traversal is
likely to need into the cache.

When prefetch metadata is accurate, longer-running traversals tend toward
in-memory execution.
