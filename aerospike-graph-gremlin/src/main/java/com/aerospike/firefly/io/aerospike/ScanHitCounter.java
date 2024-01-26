package com.aerospike.firefly.io.aerospike;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class ScanHitCounter {
    public final Map<String, AtomicLong> hitCount;
    final Map<UUID, String> scansByKey;
    final Map<UUID, AtomicLong> scanTimings;


    public ScanHitCounter() {
        this.hitCount = new ConcurrentHashMap<>();
        this.scansByKey = new ConcurrentHashMap<>();
        this.scanTimings = new ConcurrentHashMap<>();
    }

    /**
     * Associate a UUID with a key. This is used to track the key that triggered a scan.
     *
     * @param uuid the UUID of the scan
     * @param key  the key the scan was triggered on
     */
    public void associateUUID(final UUID uuid, final String key) {
        this.scansByKey.put(uuid, key);
    }

    /**
     * Set the start and stop times for a scan. This is used to track the time it took to complete a scan.
     *
     * @param uuid      the UUID of the scan
     * @param startTime the start time of the scan
     */
    public void setScanTimings(final UUID uuid, final long startTime, final long stopTime) {
        this.scanTimings.computeIfAbsent(uuid, k -> new AtomicLong(0)).set(stopTime - startTime);
    }

    /**
     * Increment the hit count for a key.
     *
     * @param key the key to increment
     * @return the new value
     */
    public long increment(String key) {
        if (key == null) {
            return 0;
        }
        return hitCount.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
    }

    /**
     * Get the hit count for a key.
     *
     * @param key the key to get the hit count for
     * @return the hit count
     */
    public long get(String key) {
        return hitCount.getOrDefault(key, new AtomicLong(0)).get();
    }

    /**
     * @return map of hit counts
     */
    public Map<String, AtomicLong> stats() {
        return hitCount;
    }

    /**
     * @return map of scan times
     */
    public Map<UUID, AtomicLong> getScanTimings() {
        return scanTimings;
    }

    /**
     * Get the key that triggered a scan.
     * @param scanId
     * @return the key that triggered the scan
     */
    public Object getKeyForUUID(UUID scanId) {
        return scansByKey.getOrDefault(scanId, "NO KEY");
    }
}
