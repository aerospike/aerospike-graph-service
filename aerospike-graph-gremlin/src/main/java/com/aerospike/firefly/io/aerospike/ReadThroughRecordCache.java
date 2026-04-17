/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.io.FireflyCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.jspecify.annotations.NullMarked;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class ReadThroughRecordCache extends FireflyCache {
    private final AtomicLong hitCounter = new AtomicLong(0);
    private final AtomicLong missCounter = new AtomicLong(0);

    private final Cache<Key, Record> cache;
    private final AerospikeConnection db;

    @NullMarked
    @SuppressWarnings("unchecked")
    class Weigher implements com.github.benmanes.caffeine.cache.Weigher<Key, Record> {
        @Override
        public int weigh(final Key key, final Record record) {
            // Basic weight for key/record.
            int size = 5;

            // Simple weight function.
            if (record.bins.containsKey(db.getConfig().outEdgesBin)) {
                // Add 1 to weight for every out edge.
                size += getEdgeCountOnVertexRecord((Map<String, List<?>>) record.getMap(db.getConfig().outEdgesBin));
            }

            if (record.bins.containsKey(db.getConfig().inEdgesBin)) {
                // Add 1 to weight for every in edge.
                size += getEdgeCountOnVertexRecord((Map<String, List<?>>) record.getMap(db.getConfig().inEdgesBin));
            }

            if (record.bins.containsKey(db.getConfig().propertiesBin)) {
                // Add 3 to weight for every property.
                size += 3 * record.getMap(db.getConfig().propertiesBin).size();
            }

            if (record.bins.containsKey(db.getConfig().vpPropertyBin)) {
                // Add 3 to weight for every vertex property property.
                size += 3 * record.getMap(db.getConfig().vpPropertyBin).size();
            }

            if (record.bins.containsKey(db.getConfig().vertexPropertyTHBin)) {
                // Add 3 to weight for every vertex property.
                size += 3 * record.getMap(db.getConfig().vertexPropertyTHBin).size();
            }

            return size;
        }

        private int getEdgeCountOnVertexRecord(final Map<String, List<?>> edgeCacheMap) {
            int size = 0;
            for (final List<?> edges : edgeCacheMap.values()) {
                size += edges.size();
            }
            return size;
        }
    }

    public ReadThroughRecordCache(final AerospikeConnection db, final UUID uuid) {
        this(db, uuid, db.getConfig().fireflyReadThroughCacheWeight);
    }

    public ReadThroughRecordCache(final AerospikeConnection db, final UUID uuid, final long cacheWeight) {
        super(uuid);
        this.db = db;
        cache = Caffeine.newBuilder().
                maximumWeight(cacheWeight).
                recordStats().
                weigher(new Weigher()).
                build();
    }

    @Override
    public Record read(final Policy policy, final Key key) {
        return readInternal(policy, key, null);
    }

    @Override
    public Record read(final WritePolicy policy, final Key key, final Operation[] operations) {
        return readInternal(policy, key, operations);
    }

    private Record readInternal(final Policy policy, final Key key, final Operation[] operations) {
        final Record cachedRecord = cache.getIfPresent(key);
        if (cachedRecord != null) {
            hitCounter.incrementAndGet();
            return cachedRecord;
        }

        missCounter.incrementAndGet();

        final Record record = (operations == null)
                ? db.skipCacheRead(key, policy)
                : db.skipCacheRead(key, (WritePolicy) policy, operations);

        insert(key, record);
        return record;
    }

    @Override
    public Record[] read(final Key[] keys, final BatchPolicy policy, final Operation... operations) {
        return readBatchInternal(keys, policy, operations);
    }

    private Record[] readBatchInternal(final Key[] keys, final BatchPolicy policy, final Operation... operations) {
        final List<Key> allKeys = List.of(keys);
        final Map<Key, Record> results = new HashMap<>(cache.getAllPresent(new HashSet<>(allKeys)));
        final List<Key> missingKeys = new ArrayList<>();
        for (final Key key : allKeys) {
            if (!results.containsKey(key)) {
                missingKeys.add(key);
            }
        }

        final int batchSize = db.getConfig().aerospikeBatchReadSize;
        for (int i = 0; i < missingKeys.size(); i += batchSize) {
            final List<Key> subList = missingKeys.subList(i, Math.min(i + batchSize, missingKeys.size()));

            // Execute batch read. subList ids are read from the database.
            final Record[] fetchedRecords = db.skipCacheRead(subList.toArray(new Key[0]), policy, operations);

            for (int j = 0; j < fetchedRecords.length; j++) {
                final Key key = subList.get(j);
                final Record record = fetchedRecords[j];
                results.put(key, record);

                // Insert to cache since these are new.
                insert(key, record);
            }
        }

        // Update metrics.
        hitCounter.addAndGet(allKeys.size() - missingKeys.size());
        missCounter.addAndGet(missingKeys.size());

        // Place results into cache.
        final Record[] finalRecords = new Record[allKeys.size()];

        for (int i = 0; i < allKeys.size(); i++) {
            finalRecords[i] = results.get(allKeys.get(i));
        }
        return finalRecords;
    }

    /**
     * Write data into Aerospike.
     *
     * @param key  Key to write
     * @param bins Data to write
     */
    @Override
    public void write(final WritePolicy writePolicy, final Key key, final Bin... bins) {
        cache.invalidate(key);
        db.checkedPut(writePolicy, key, bins);
    }

    /**
     * Delete an entry from Aerospike, and remove it from the cache
     *
     * @param key Key to remove
     */
    @Override
    public void remove(final Key key) {
        cache.invalidate(key);
        db.delete(key, null);
    }

    /**
     * Remove a cache entry by Key
     *
     * @param key Key to remove
     */
    @Override
    public void invalidate(final Key key) {
        cache.invalidate(key);
    }

    /**
     * Add a Key/Record cache entry
     *
     * @param key    Key to add
     * @param record Record to add
     */
    @Override
    public void insert(final Key key, final Record record) {
        if (key == null || record == null) {
            return;
        }
        cache.put(key, record);
    }

    /**
     * @return number of entries in cache
     */
    public long size() {
        return cache.estimatedSize();
    }

    /**
     * @return Caffeine CacheStats
     */
    public CacheStats stats() {
        return cache.stats();
    }

    /**
     * Statistic on data served from cache
     *
     * @return hit count
     */
    @Override
    public long getHitCount() {
        return hitCounter.get();
    }

    /**
     * Statistic on data served that was not in the cache
     *
     * @return miss count
     */
    @Override
    public long getMissCount() {
        return missCounter.get();
    }

    /**
     * Returns the estimated number of entries in the cache.
     *
     * @return estimated entry count
     */
    @Override
    public long getEstimatedEntryCount() {
        return cache.estimatedSize();
    }

    /**
     * Returns the weighted size of all entries in the cache.
     * The weight is calculated based on the number of edges and properties per record.
     *
     * @return weighted size, or 0 if eviction policy is not available
     */
    @Override
    public long getWeightedSize() {
        return cache.policy().eviction()
                .map(eviction -> eviction.weightedSize().orElse(0L))
                .orElse(0L);
    }

    /**
     * Returns an estimated memory usage of the cache in bytes.
     * This is an approximation based on the weighted size.
     * <p>
     * The estimation uses approximately 200 bytes per weight unit, which accounts for:
     * <ul>
     *   <li>Key object overhead (~100 bytes)</li>
     *   <li>Record object and bin map overhead (~50 bytes)</li>
     *   <li>Average data per weight unit (~50 bytes)</li>
     * </ul>
     *
     * @return estimated memory usage in bytes
     */
    @Override
    public long getEstimatedMemoryUsageBytes() {
        // Approximate bytes per weight unit
        final long bytesPerWeightUnit = 200L;
        return getWeightedSize() * bytesPerWeightUnit;
    }

    /**
     * Invalidate entire cache.
     */
    @Override
    public void invalidateAll() {
        hitCounter.set(0);
        missCounter.set(0);
        cache.invalidateAll();
    }
}
