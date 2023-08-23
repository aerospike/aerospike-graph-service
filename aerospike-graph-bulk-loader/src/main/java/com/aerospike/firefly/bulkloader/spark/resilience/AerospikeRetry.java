package com.aerospike.firefly.bulkloader.spark.resilience;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

public interface AerospikeRetry {
    <T> CompletionStage<T> withRetries(final Supplier<CompletionStage<T>> task, final ScheduledExecutorService sec);
}
