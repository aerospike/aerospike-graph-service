package com.aerospike.firefly.bulkloader.spark.resilience;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public interface AerospikeRetry {
    CompletableFuture withRetries(final CompletableFuture supplier, final ScheduledExecutorService sec);
}