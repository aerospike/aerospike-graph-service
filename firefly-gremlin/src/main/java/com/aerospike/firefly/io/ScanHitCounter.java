package com.aerospike.firefly.io;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class ScanHitCounter {
    final Map<String, AtomicLong> hitCount;
    final Map<UUID, String> scansByKey;
    final Map<UUID, AtomicLong> scanTimings;


    private ScanHitCounter() {
        this.hitCount = new ConcurrentHashMap<>();
        this.scansByKey = new ConcurrentHashMap<>();
        this.scanTimings = new ConcurrentHashMap<>();
    }

    public void associateUUID(final UUID uuid, final String key) {
        this.scansByKey.put(uuid, key);
    }

    public void setScanTimings(final UUID uuid, final long startTime, final long stopTime) {
        this.scanTimings.computeIfAbsent(uuid, k -> new AtomicLong(0)).set(stopTime - startTime);
    }

    public static ScanHitCounter create() {
        return new ScanHitCounter();
    }

    public long increment(String key) {
        if (key == null) {
            return 0;
        }
        return hitCount.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
    }

    public long get(String key) {
        return hitCount.getOrDefault(key, new AtomicLong(0)).get();
    }

    public Map<String, AtomicLong> stats() {
        return hitCount;
    }

    public Map<UUID, AtomicLong>  getScanTimings() {
        return scanTimings;
    }
}
