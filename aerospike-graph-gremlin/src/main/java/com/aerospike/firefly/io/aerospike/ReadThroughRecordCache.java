package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.io.FireflyCache;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */

public class ReadThroughRecordCache extends FireflyCache {
    private final AtomicLong hitCounter = new AtomicLong(0);
    private final AtomicLong missCounter = new AtomicLong(0);


    // Key->Record. getIfPresent will return null if the key is not in the cache.
    private final Cache<Key, Record> cache;
    private final AerospikeConnection db;

    class Weigher implements com.google.common.cache.Weigher<Key, Record> {
        @Override
        public int weigh(final Key key, final Record record) {
            // Basic weight for key/record.
            int size = 5;

            // Simple weight function.
            if (record.bins.containsKey(db.OUT_EDGES_BIN)) {
                // Add 1 to weight for every out edge.
                size += getEdgeCountOnVertexRecord((Map<String, List>) record.getMap(db.OUT_EDGES_BIN));
            }

            if (record.bins.containsKey(db.IN_EDGES_BIN)) {
                // Add 1 to weight for every in edge.
                size += getEdgeCountOnVertexRecord((Map<String, List>) record.getMap(db.IN_EDGES_BIN));
            }

            if (record.bins.containsKey(db.PROPERTIES_BIN)) {
                // Add 3 to weight for every property.
                size += 3 * record.getMap(db.PROPERTIES_BIN).size();
            }

            if (record.bins.containsKey(db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN)) {
                // Add 3 to weight for every vertex property.
                size += 3 * record.getMap(db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN).size();
            }

            return size;
        }

        private int getEdgeCountOnVertexRecord(final Map<String, List> edgeCacheMap) {
            int size = 0;
            for (final List edges : edgeCacheMap.values()) {
                size += edges.size();
            }
            return size;
        }
    }

    public ReadThroughRecordCache(final AerospikeConnection db, final UUID uuid) {
        super(uuid);
        this.db = db;
        cache = CacheBuilder.newBuilder().
                maximumWeight(db.FIREFLY_READ_THROUGH_CACHE_WEIGHT).
                recordStats().
                weigher(new Weigher()).
                build();
    }

    @Override
    public Record read(final Key key) {
        final Record or = cache.getIfPresent(key);
        if (or != null) {
            hitCounter.incrementAndGet();
            return or;
        } else {
            missCounter.incrementAndGet();
            final Policy policy = new Policy();
            policy.sendKey = false;
            final Record record = db.getClient().get(policy, key);
            insert(key, record);
            return record;
        }
    }

    @Override
    public Record[] read(final Key[] keys, final BatchPolicy policy) {
        final List<Key> allKeys = List.of(keys);
        final Map<Key, Record> results = new HashMap<>(cache.getAllPresent(new HashSet<>(allKeys)));
        final List<Key> missingKeys = allKeys.stream().filter(key -> !results.containsKey(key)).collect(Collectors.toList());
        final Set<Key> missingKeySet = new HashSet<>(missingKeys);

        // Batch reading in Aerospike is capped based on settings in the server.
        for (int i = 0; i < missingKeySet.size(); i += db.AEROSPIKE_BATCH_READ_SIZE) {
            // Generate sub list using current index and batch size.
            final List<Key> subList = missingKeySet.stream().skip(i).limit(db.AEROSPIKE_BATCH_READ_SIZE).collect(Collectors.toList());

            // Execute batch read. subList ids are read from the database.
            final Record[] records = db.getClient().get(policy, subList.toArray(new Key[0]));
            for (int j = 0; j < records.length; j++) {
                results.put(subList.get(j), records[j]);
            }
        }

        // Update metrics.
        hitCounter.addAndGet(allKeys.size() - missingKeys.size());
        missCounter.addAndGet(missingKeys.size());

        // Place results into cache.
        final Record[] records = new Record[allKeys.size()];
        for (int i = 0; i < allKeys.size(); i++) {
            final Key key = allKeys.get(i);
            final Record record = results.get(key);
            records[i] = record;
            insert(key, record);
        }

        return records;
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
        db.getClient().delete(null, key);
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
        return cache.size();
    }

    /**
     * @return Guava CacheStats
     */
    public CacheStats stats() {
        return cache.stats();
    }


    /**
     * Statistic on data served from cache
     *
     * @return hit count
     */
    public long getHitCount() {
        return hitCounter.get();
    }

    /**
     * Statistic on data served that was not in the cache
     *
     * @return miss count
     */
    public long getMissCount() {
        return missCounter.get();
    }

    /**
     * Invalidate entire cache.
     */
    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }
}
