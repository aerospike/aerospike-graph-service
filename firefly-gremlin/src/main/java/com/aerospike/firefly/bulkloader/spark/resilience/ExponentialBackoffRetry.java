package com.aerospike.firefly.bulkloader.spark.resilience;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExponentialBackoffRetry implements AerospikeRetry, Serializable {
    public static final Set<Integer> DEFAULT_AEROSPIKE_RETRY_ERROR_CODES = Stream.of(
                    ResultCode.INVALID_QUOTA,
                    ResultCode.QUOTA_EXCEEDED,
                    ResultCode.NO_MORE_CONNECTIONS, ResultCode.ASYNC_QUEUE_FULL,
                    ResultCode.SERVER_MEM_ERROR,
                    ResultCode.DEVICE_OVERLOAD,
                    ResultCode.TIMEOUT,
                    ResultCode.MAX_ERROR_RATE,
                    ResultCode.KEY_BUSY,
                    ResultCode.SERVER_NOT_AVAILABLE)
            .collect(Collectors.toCollection(HashSet::new));
    private static final Logger LOGGER = LoggerFactory.getLogger(ExponentialBackoffRetry.class);
    private final Retry retry;
    private final Set<Integer> errorCodes;

    public ExponentialBackoffRetry(Optional<String> name) {
        this.errorCodes = DEFAULT_AEROSPIKE_RETRY_ERROR_CODES;
        final Predicate<Throwable> quotaPredicate = e ->
                (e instanceof AerospikeException) && isRetriable((AerospikeException) e);

        final RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(DatasetOperations.RETRY_LIMIT)
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(300, 2))
                .retryOnException(quotaPredicate)
                .build();
        final RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        retry = retryRegistry.retry(name.orElse("aerospike-bulkloader-retry"), retryConfig);
        subscribe();
    }

    public ExponentialBackoffRetry(Optional<String> name, int maxRetry, int initialIntervalMillis, Collection<Integer> errorCodes) {
        assert (maxRetry > 0);
        assert (initialIntervalMillis > 0);
        assert (!errorCodes.isEmpty());
        this.errorCodes = new HashSet<>(errorCodes);
        final Predicate<Throwable> quotaPredicate = e ->
                (e instanceof AerospikeException) && isRetriable((AerospikeException) e);

        final RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(maxRetry)
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(initialIntervalMillis, 2))
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

    private boolean isRetriable(final AerospikeException e) {
        return errorCodes.contains(e.getResultCode());
    }

    private void subscribe() {
        retry.getEventPublisher().onRetry(event -> LOGGER.info("Retry #" + event.getNumberOfRetryAttempts() + " failed with exception: " + event.getLastThrowable().getMessage()));
        retry.getEventPublisher().onError(event -> LOGGER.error("Retry #" + event.getNumberOfRetryAttempts() + " failed with exception: " + event.getLastThrowable().getMessage()));
        retry.getEventPublisher().onSuccess(event -> LOGGER.info("Retry #" + event.getNumberOfRetryAttempts() + " succeeded"));
    }

}
