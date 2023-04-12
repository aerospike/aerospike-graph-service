package com.aerospike.firefly.io;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class ScanHitCounter {
    final Map<String, AtomicLong> stats;


    private ScanHitCounter() {
        this.stats = new HashMap<>();
    }

    public static ScanHitCounter create() {
        return new ScanHitCounter();
    }

    public long increment(String key) {
        if (key == null) {
            return 0;
        }
        return stats.computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();
    }

    public long get(String key) {
        return stats.getOrDefault(key, new AtomicLong(0)).get();
    }
}
