package com.aerospike.firefly.bulkloader.spark.resilience;

import com.aerospike.firefly.bulkloader.exception.FireflyLoadingException;
import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

public class ExponentialBackoffRetry implements AerospikeRetry, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExponentialBackoffRetry.class);
    private final Retry retry;

    public ExponentialBackoffRetry(Optional<String> name) {
        final Predicate<Throwable> quotaPredicate = e ->
                (e instanceof FireflyLoadingException) && ((FireflyLoadingException) e).isRetryable();

        final RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(DatasetOperations.RETRY_LIMIT)
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(300, 2))
                .retryOnException(quotaPredicate)
                .build();
        final RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        retry = retryRegistry.retry(name.orElse("aerospike-bulkloader-retry"), retryConfig);
        subscribe();
    }

    public CompletableFuture<T> withRetries(final CompletableFuture task,
                                            final ScheduledExecutorService sc) {
        return retry.executeCompletionStage(sc, () -> task).toCompletableFuture();
    }

    private void subscribe() {
        retry.getEventPublisher().onRetry(event -> LOGGER.info("Retry #" + event.getNumberOfRetryAttempts() + " failed with exception: " + event.getLastThrowable().getMessage()));
        retry.getEventPublisher().onError(event -> LOGGER.error("Retry #" + event.getNumberOfRetryAttempts() + " failed with exception: " + event.getLastThrowable().getMessage()));
        retry.getEventPublisher().onSuccess(event -> LOGGER.info("Retry #" + event.getNumberOfRetryAttempts() + " succeeded"));
    }

}
