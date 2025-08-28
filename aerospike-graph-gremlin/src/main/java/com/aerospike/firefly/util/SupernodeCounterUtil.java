package com.aerospike.firefly.util;

import java.util.concurrent.atomic.LongAdder;

public class SupernodeCounterUtil {
    private final int windowSeconds;
    private final Bucket[] buckets;

    private static final class Bucket {
        volatile long epochSecond = Long.MIN_VALUE;
        final LongAdder adder = new LongAdder();
    }

    private static SupernodeCounterUtil instance;

    public static synchronized SupernodeCounterUtil getInstance(final int windowSeconds) {
        if (instance == null) {
            instance = new SupernodeCounterUtil(windowSeconds);
        }
        if (instance.windowSeconds != windowSeconds) {
            throw new IllegalStateException("SupernodeCounterUtil already initialized with a different window size: " + instance.windowSeconds + ". Please contact support.");
        }
        return instance;
    }

    private SupernodeCounterUtil(final int windowSeconds) {
        this.windowSeconds = windowSeconds;
        this.buckets = new Bucket[windowSeconds];
        for (int i = 0; i < windowSeconds; i++) {
            // Initialize buckets.
            buckets[i] = new Bucket();
        }
    }

    public void add() {
        final long nowSec = getCurrentEpochSecond();
        final int bucketIndex = (int) (nowSec % windowSeconds);
        if (buckets[bucketIndex].epochSecond != nowSec) {
            // If bucket is stale, reset it.
            buckets[bucketIndex].adder.reset();
        }
        buckets[bucketIndex].adder.increment();
    }

    public long getCount() {
        final long nowSec = getCurrentEpochSecond();
        long total = 0L;
        for (int i = 0; i < windowSeconds; i++) {
            final Bucket b = buckets[i];
            final long age = nowSec - b.epochSecond;

            // Count only buckets within window.
            if (age >= 0 && age < windowSeconds) {
                total += b.adder.sum();
            }
        }
        return total;
    }

    private long getCurrentEpochSecond() {
        return System.currentTimeMillis() / 1000;
    }
}
