package com.aerospike.firefly.bulkloader.spark.resilience;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyLoadingException;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class ExponentialBackoffRetry implements AerospikeRetry, Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExponentialBackoffRetry.class);
    private final Retry retry;

    public ExponentialBackoffRetry(final String taskName) {
        final Predicate<Throwable> retryPredicate = e ->
                (e instanceof FireflyLoadingException) && ((FireflyLoadingException) e).isRetryable();

        final RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(DatasetOperations.RETRY_LIMIT)
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(300, 2))
                .retryOnException(retryPredicate)
                .build();
        final RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        retry = retryRegistry.retry(taskName==null ? "aerospike-bulkloader-retry" : taskName, retryConfig);
        subscribe();
    }

    public <T> CompletionStage<T> withRetries(final Supplier<CompletionStage<T>> supplier,
                                              final ScheduledExecutorService sc) {
        return retry.executeCompletionStage(sc, supplier);
    }

    private void subscribe() {
        retry.getEventPublisher().onRetry(event -> LOGGER.info("Retry #" + event.getNumberOfRetryAttempts() + " failed with exception: " + event.getLastThrowable().getMessage()));
        retry.getEventPublisher().onError(event -> LOGGER.error("Retry #" + event.getNumberOfRetryAttempts() + " errored with exception: " + event.getLastThrowable().getMessage()));
        retry.getEventPublisher().onSuccess(event -> LOGGER.info("Retry #" + event.getNumberOfRetryAttempts() + " succeeded"));
    }

}
