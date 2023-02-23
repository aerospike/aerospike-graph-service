package com.aerospike.firefly.io;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class ScanHitCounter {
    private final int WARNING_THRESHOLD;
    private final int MAX_SIZE; // track the thousand most hit keys
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final Function<Map.Entry<String, Long>, Void> onWarning;
    private final int timeout;
    final Cache<String, AtomicLong> stats;


    private ScanHitCounter(final int timeout, final int keysToTrack, final int scanHitWarningThreshold, Function<Map.Entry<String, Long>, Void> onWarning) {
        this.onWarning = onWarning;
        this.MAX_SIZE = keysToTrack;
        this.timeout = timeout;
        this.WARNING_THRESHOLD = scanHitWarningThreshold;
        this.stats = CacheBuilder.newBuilder()
                .expireAfterAccess(timeout, TimeUnit.SECONDS)
                .build();
    }

    public static ScanHitCounter create(final int timeout, final int maxSize, final int scanHitWarningThreshold, Function<Map.Entry<String, Long>, Void> onWarning) {
        return new ScanHitCounter(timeout, maxSize, scanHitWarningThreshold, onWarning);
    }

    public long increment(String key) {
        if (key == null)
            return 0; // ignore null keys
        long val = stats.asMap().computeIfAbsent(key, k -> new AtomicLong(0)).incrementAndGet();

        if (stats.size() > MAX_SIZE) {
            stats.asMap().entrySet().stream()
                    .sorted((e1, e2) -> {
                        if (e2.getValue().equals(e1.getValue()))
                            return 0;
                        else
                            return e1.getValue().get() > e2.getValue().get() ? 1 : -1;
                    })
                    .limit(stats.size() - MAX_SIZE)
                    .forEach(e -> stats.invalidate(e.getKey()));
        }
        if (val > this.WARNING_THRESHOLD) {
            onWarning.apply(new AbstractMap.SimpleEntry<>(key, val));
        }
        return val;
    }

    public long get(String key) {
        return stats.asMap().getOrDefault(key, new AtomicLong(0)).get();
    }
}
