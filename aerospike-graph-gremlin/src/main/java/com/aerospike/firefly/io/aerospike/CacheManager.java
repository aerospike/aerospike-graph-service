package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.Key;
import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.structure.id.FireflyId;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Manages caches for graph operations.
 * <p>
 * This class encapsulates two types of caches:
 * <ul>
 *   <li>{@code transactionCache} - Cache for full record reads with all properties</li>
 *   <li>{@code emptyPropsCache} - Cache for reads without properties (lightweight reads)</li>
 * </ul>
 * <p>
 * The caching behavior depends on the {@link CacheMode}:
 * <ul>
 *   <li>{@link CacheMode#TRANSACTIONAL} - Thread-local caches that are reset per traversal/query</li>
 *   <li>{@link CacheMode#GLOBAL} - Static caches shared across all threads and operations</li>
 * </ul>
 */
public class CacheManager {

    /**
     * Cache key for supernode edge IDs.
     * Combines vertex ID, direction, and edge labels to uniquely identify a set of edges.
     */
    public static class SupernodeEdgeCacheKey {
        private final String vertexKeyHash;
        private final Direction direction;
        private final Set<String> labels;
        private final int hashCode;

        public SupernodeEdgeCacheKey(final FireflyId vertexId, final Direction direction, final Set<String> labels) {
            this.vertexKeyHash = vertexId.getKeyHashString();
            this.direction = direction;
            this.labels = labels;
            this.hashCode = Objects.hash(vertexKeyHash, direction, labels);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SupernodeEdgeCacheKey that = (SupernodeEdgeCacheKey) o;
            return Objects.equals(vertexKeyHash, that.vertexKeyHash) &&
                    direction == that.direction &&
                    Objects.equals(labels, that.labels);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public String toString() {
            return "SupernodeEdgeCacheKey{" +
                    "vertexKeyHash='" + vertexKeyHash + '\'' +
                    ", direction=" + direction +
                    ", labels=" + labels +
                    '}';
        }
    }

    /**
     * Defines the caching mode for graph operations.
     */
    public enum CacheMode {
        /**
         * Transactional caching mode - cache is scoped to the current transaction/traversal.
         * Caches are thread-local and reset on each new traversal.
         */
        TRANSACTIONAL,

        /**
         * Global caching mode - cache is shared across all operations and threads.
         * Caches are not reset between traversals.
         */
        GLOBAL
    }

    /**
     * Default base cache weight (1 million).
     */
    public static final long DEFAULT_BASE_CACHE_WEIGHT = 1_000_000L;

    /**
     * Default cache weight for TRANSACTIONAL mode.
     */
    public static final long DEFAULT_TRANSACTIONAL_CACHE_WEIGHT = DEFAULT_BASE_CACHE_WEIGHT;

    /**
     * Default cache weight for GLOBAL mode (20 million).
     */
    public static final long DEFAULT_GLOBAL_CACHE_WEIGHT = DEFAULT_BASE_CACHE_WEIGHT * 20;

    // Thread-local caches for TRANSACTIONAL mode
    private final ThreadLocal<FireflyCache> transactionalCache = new ThreadLocal<>();
    private final ThreadLocal<FireflyCache> transactionalEmptyPropsCache = new ThreadLocal<>();

    // Caches for GLOBAL mode (per graph)
    private volatile FireflyCache globalCache;
    private volatile FireflyCache globalEmptyPropsCache;

    // Cache for supernode edge IDs (only used in GLOBAL mode)
    // Key: SupernodeEdgeCacheKey (vertexId + direction + labels)
    // Value: List of edge IDs from secondary index query
    private volatile Cache<SupernodeEdgeCacheKey, List<FireflyId>> globalSupernodeEdgeIdCache;

    // Track total number of cached edge IDs for memory estimation
    private volatile long totalCachedSupernodeEdgeIds = 0;

    private CacheMode cacheMode = CacheMode.TRANSACTIONAL;
    private long cacheWeight = DEFAULT_TRANSACTIONAL_CACHE_WEIGHT;

    /**
     * Gets the current cache mode.
     *
     * @return the current cache mode
     */
    public CacheMode getCacheMode() {
        return cacheMode;
    }

    /**
     * Sets the cache mode with default cache weight.
     * <p>
     * Default weight is {@link #DEFAULT_TRANSACTIONAL_CACHE_WEIGHT} for TRANSACTIONAL mode
     * and {@link #DEFAULT_GLOBAL_CACHE_WEIGHT} for GLOBAL mode.
     *
     * @param db        the AerospikeConnection to use for cache initialization
     * @param cacheMode the cache mode to set
     */
    public void setCacheMode(final AerospikeConnection db, final CacheMode cacheMode) {
        final long defaultWeight = getDefaultWeightForMode(cacheMode);
        setCacheMode(db, cacheMode, defaultWeight);
    }

    /**
     * Sets the cache mode with a custom cache weight.
     * <p>
     * When changing from {@link CacheMode#GLOBAL} to {@link CacheMode#TRANSACTIONAL},
     * the global caches are cleared.
     * <p>
     * When changing from {@link CacheMode#TRANSACTIONAL} to {@link CacheMode#GLOBAL},
     * the transactional caches are cleared and global caches are initialized.
     * <p>
     * When staying in {@link CacheMode#GLOBAL} but changing the cache weight,
     * the global caches are reinitialized with the new weight.
     *
     * @param db          the AerospikeConnection to use for cache initialization
     * @param cacheMode   the cache mode to set (null to keep current mode)
     * @param cacheWeight the cache weight in bytes
     */
    public void setCacheMode(final AerospikeConnection db, final CacheMode cacheMode, final long cacheWeight) {
        final CacheMode newMode = (cacheMode != null) ? cacheMode : this.cacheMode;
        
        // No changes needed
        if (this.cacheMode == newMode && this.cacheWeight == cacheWeight) {
            return;
        }

        // Handle mode transitions and weight changes
        if (this.cacheMode == CacheMode.GLOBAL && newMode == CacheMode.TRANSACTIONAL) {
            // Switching from GLOBAL to TRANSACTIONAL: clear global cache
            clearGlobalCache();
        } else if (this.cacheMode == CacheMode.TRANSACTIONAL && newMode == CacheMode.GLOBAL) {
            // Switching from TRANSACTIONAL to GLOBAL: clear transactional, init global
            clearTransactionalCache();
            initGlobalCache(db, cacheWeight);
        } else if (this.cacheMode == CacheMode.GLOBAL && this.cacheWeight != cacheWeight) {
            // Staying in GLOBAL but changing weight: reinit global cache
            clearGlobalCache();
            initGlobalCache(db, cacheWeight);
        }
        // For TRANSACTIONAL mode weight changes, no action needed (thread-local caches recreate automatically)

        this.cacheMode = newMode;
        this.cacheWeight = cacheWeight;
    }

    /**
     * Gets the current cache weight.
     *
     * @return the current cache weight in bytes
     */
    public long getCacheWeight() {
        return cacheWeight;
    }

    /**
     * Gets the default cache weight for a given mode.
     *
     * @param mode the cache mode
     * @return the default cache weight for that mode
     */
    public static long getDefaultWeightForMode(final CacheMode mode) {
        return (mode == CacheMode.GLOBAL) ? DEFAULT_GLOBAL_CACHE_WEIGHT : DEFAULT_TRANSACTIONAL_CACHE_WEIGHT;
    }

    private void clearTransactionalCache() {
        transactionalCache.remove();
        transactionalEmptyPropsCache.remove();
    }

    private synchronized void clearGlobalCache() {
        globalCache = null;
        globalEmptyPropsCache = null;
        globalSupernodeEdgeIdCache = null;
        totalCachedSupernodeEdgeIds = 0;
    }

    private synchronized void initGlobalCache(final AerospikeConnection db, final long cacheWeight) {
        final UUID uuid = UUID.randomUUID();
        globalCache = new ReadThroughRecordCache(db, uuid, cacheWeight);
        globalEmptyPropsCache = new ReadThroughRecordCache(db, uuid, cacheWeight);
        // Initialize supernode edge ID cache
        globalSupernodeEdgeIdCache = Caffeine.newBuilder()
                .maximumSize(cacheWeight)
                .recordStats()
                .build();
    }

    /**
     * Gets the transaction cache based on the current cache mode.
     *
     * @return the transaction cache, or null if not set
     */
    public FireflyCache getTransactionCache() {
        if (cacheMode == CacheMode.GLOBAL) {
            return globalCache;
        }
        return transactionalCache.get();
    }

    /**
     * Gets the empty properties cache based on the current cache mode.
     *
     * @return the empty properties cache, or null if not set
     */
    public FireflyCache getEmptyPropsCache() {
        if (cacheMode == CacheMode.GLOBAL) {
            return globalEmptyPropsCache;
        }
        return transactionalEmptyPropsCache.get();
    }

    /**
     * Invalidates the specified key in both caches if they are present.
     *
     * @param key the key to invalidate
     */
    public void invalidate(final Key key) {
        final FireflyCache cache = getTransactionCache();
        if (cache != null) {
            cache.invalidate(key);
        }
        final FireflyCache noPropsCache = getEmptyPropsCache();
        if (noPropsCache != null) {
            noPropsCache.invalidate(key);
        }
    }

    /**
     * Resets the caches for a new traversal/query.
     * <p>
     * This method only resets caches in {@link CacheMode#TRANSACTIONAL} mode.
     * In {@link CacheMode#GLOBAL} mode, this method does nothing as global caches should not be reset.
     *
     * @param db the AerospikeConnection to use for cache creation and metrics recording
     */
    public void resetCache(final AerospikeConnection db) {
        if (cacheMode == CacheMode.GLOBAL) {
            return;
        }

        final UUID uuid = UUID.randomUUID();

        final FireflyCache txnCache = transactionalCache.get();
        if (txnCache != null) {
            db.lastQueryHitCount = txnCache.getHitCount();
            db.lastQueryMissCount = txnCache.getMissCount();
            txnCache.invalidateAll();
        } else {
            transactionalCache.set(new ReadThroughRecordCache(db, uuid, cacheWeight));
        }

        final FireflyCache propsCache = transactionalEmptyPropsCache.get();
        if (propsCache != null) {
            db.lastQueryHitCount = propsCache.getHitCount() + db.lastQueryHitCount;
            db.lastQueryMissCount = propsCache.getMissCount() + db.lastQueryMissCount;
            propsCache.invalidateAll();
        } else {
            transactionalEmptyPropsCache.set(new ReadThroughRecordCache(db, uuid, cacheWeight));
        }
    }

    /**
     * Returns the total estimated entry count across both caches.
     *
     * @return total estimated entry count
     */
    public long getTotalEstimatedEntryCount() {
        long count = 0;
        final FireflyCache cache = getTransactionCache();
        if (cache != null) {
            count += cache.getEstimatedEntryCount();
        }
        final FireflyCache noPropsCache = getEmptyPropsCache();
        if (noPropsCache != null) {
            count += noPropsCache.getEstimatedEntryCount();
        }
        return count;
    }

    /**
     * Returns the total weighted size across both caches.
     *
     * @return total weighted size
     */
    public long getTotalWeightedSize() {
        long size = 0;
        final FireflyCache cache = getTransactionCache();
        if (cache != null) {
            size += cache.getWeightedSize();
        }
        final FireflyCache noPropsCache = getEmptyPropsCache();
        if (noPropsCache != null) {
            size += noPropsCache.getWeightedSize();
        }
        return size;
    }

    /**
     * Returns the total estimated memory usage across all caches in bytes.
     * Includes record caches and supernode edge ID cache.
     *
     * @return total estimated memory usage in bytes
     */
    public long getTotalEstimatedMemoryUsageBytes() {
        long bytes = 0;
        final FireflyCache cache = getTransactionCache();
        if (cache != null) {
            bytes += cache.getEstimatedMemoryUsageBytes();
        }
        final FireflyCache noPropsCache = getEmptyPropsCache();
        if (noPropsCache != null) {
            bytes += noPropsCache.getEstimatedMemoryUsageBytes();
        }
        // Add supernode edge ID cache memory estimate
        bytes += getSupernodeEdgeCacheEstimatedMemoryBytes();
        return bytes;
    }

    /**
     * Returns the estimated memory usage of the supernode edge ID cache in bytes.
     * Estimates ~40 bytes per FireflyId (object overhead + references + hash bytes)
     * plus ~100 bytes overhead per cache entry (key + list structure).
     *
     * @return estimated memory usage in bytes
     */
    public long getSupernodeEdgeCacheEstimatedMemoryBytes() {
        if (globalSupernodeEdgeIdCache == null) {
            return 0;
        }
        final long entryCount = globalSupernodeEdgeIdCache.estimatedSize();
        // ~100 bytes per entry for key and list overhead
        // ~40 bytes per FireflyId (object header + byte[] hash + references)
        return (entryCount * 100) + (totalCachedSupernodeEdgeIds * 40);
    }

    /**
     * Returns the total number of edge IDs cached in the supernode cache.
     *
     * @return total cached edge ID count
     */
    public long getSupernodeEdgeCacheTotalEdgeIds() {
        return totalCachedSupernodeEdgeIds;
    }

    /**
     * Returns a human-readable string of the estimated memory usage.
     *
     * @return formatted memory usage string (e.g., "1.5 MB", "256 KB")
     */
    public String getTotalEstimatedMemoryUsageFormatted() {
        final long bytes = getTotalEstimatedMemoryUsageBytes();
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.1f MB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
        }
    }

    /**
     * Returns the total hit count across both caches.
     *
     * @return total hit count
     */
    public long getTotalHitCount() {
        long count = 0;
        final FireflyCache cache = getTransactionCache();
        if (cache != null) {
            count += cache.getHitCount();
        }
        final FireflyCache noPropsCache = getEmptyPropsCache();
        if (noPropsCache != null) {
            count += noPropsCache.getHitCount();
        }
        return count;
    }

    /**
     * Returns the total miss count across both caches.
     *
     * @return total miss count
     */
    public long getTotalMissCount() {
        long count = 0;
        final FireflyCache cache = getTransactionCache();
        if (cache != null) {
            count += cache.getMissCount();
        }
        final FireflyCache noPropsCache = getEmptyPropsCache();
        if (noPropsCache != null) {
            count += noPropsCache.getMissCount();
        }
        return count;
    }

    // ==================== Supernode Edge ID Cache Methods ====================

    /**
     * Gets cached supernode edge IDs for the given vertex, direction, and labels.
     * Only works in GLOBAL cache mode.
     *
     * @param vertexId  the supernode vertex ID
     * @param direction the edge direction (IN or OUT)
     * @param labels    the edge labels to filter by (empty set means all labels)
     * @return the cached list of edge IDs, or null if not cached or not in GLOBAL mode
     */
    public List<FireflyId> getSupernodeEdgeIds(final FireflyId vertexId, final Direction direction, final Set<String> labels) {
        if (cacheMode != CacheMode.GLOBAL || globalSupernodeEdgeIdCache == null) {
            return null;
        }
        final SupernodeEdgeCacheKey key = new SupernodeEdgeCacheKey(vertexId, direction, labels);
        return globalSupernodeEdgeIdCache.getIfPresent(key);
    }

    /**
     * Caches supernode edge IDs for the given vertex, direction, and labels.
     * Only works in GLOBAL cache mode.
     *
     * @param vertexId  the supernode vertex ID
     * @param direction the edge direction (IN or OUT)
     * @param labels    the edge labels to filter by
     * @param edgeIds   the list of edge IDs to cache
     */
    public void putSupernodeEdgeIds(final FireflyId vertexId, final Direction direction, final Set<String> labels, final List<FireflyId> edgeIds) {
        if (cacheMode != CacheMode.GLOBAL || globalSupernodeEdgeIdCache == null) {
            return;
        }
        final SupernodeEdgeCacheKey key = new SupernodeEdgeCacheKey(vertexId, direction, labels);
        globalSupernodeEdgeIdCache.put(key, edgeIds);
        // Track total edge IDs for memory estimation
        totalCachedSupernodeEdgeIds += edgeIds.size();
    }

    /**
     * Returns the number of cached supernode edge ID entries.
     *
     * @return number of cached entries, or 0 if not in GLOBAL mode
     */
    public long getSupernodeEdgeCacheEntryCount() {
        if (globalSupernodeEdgeIdCache == null) {
            return 0;
        }
        return globalSupernodeEdgeIdCache.estimatedSize();
    }

    /**
     * Returns the hit count for the supernode edge ID cache.
     *
     * @return hit count, or 0 if not in GLOBAL mode
     */
    public long getSupernodeEdgeCacheHitCount() {
        if (globalSupernodeEdgeIdCache == null) {
            return 0;
        }
        return globalSupernodeEdgeIdCache.stats().hitCount();
    }

    /**
     * Returns the miss count for the supernode edge ID cache.
     *
     * @return miss count, or 0 if not in GLOBAL mode
     */
    public long getSupernodeEdgeCacheMissCount() {
        if (globalSupernodeEdgeIdCache == null) {
            return 0;
        }
        return globalSupernodeEdgeIdCache.stats().missCount();
    }
}
