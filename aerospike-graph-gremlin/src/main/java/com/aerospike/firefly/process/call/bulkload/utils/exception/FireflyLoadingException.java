package com.aerospike.firefly.process.call.bulkload.utils.exception;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;

import java.util.Set;

/**
 * Wrapper Exception class for errors encountered when Bulk Loading that contains the context for whether the Bulk
 * Loading API call that returned the Exception can be retried.
 */
public class FireflyLoadingException extends RuntimeException {
    static final private Set<Integer> RETRYABLE_CODES = Set.of(
            ResultCode.ASYNC_QUEUE_FULL,
            ResultCode.CLIENT_ERROR,
            ResultCode.CLUSTER_KEY_MISMATCH,
            ResultCode.DEVICE_OVERLOAD,
            ResultCode.LOST_CONFLICT,
            ResultCode.NO_RESPONSE,
            ResultCode.OK,
            ResultCode.PARTITION_UNAVAILABLE,
            ResultCode.QUERY_GENERIC,
            ResultCode.QUERY_TIMEOUT,
            ResultCode.QUOTA_EXCEEDED,
            ResultCode.SERVER_ERROR,
            ResultCode.SERVER_NOT_AVAILABLE,
            ResultCode.TIMEOUT,
            ResultCode.NO_MORE_CONNECTIONS,
            ResultCode.INVALID_NODE_ERROR,
            ResultCode.BATCH_FAILED
    );

    final private AerospikeException aerospikeException;

    public FireflyLoadingException(final AerospikeException cause) {
        super(cause);
        this.aerospikeException = cause;
    }

    public FireflyLoadingException(final AerospikeException cause, final String message) {
        super(message, cause);
        this.aerospikeException = cause;
    }

    public boolean isRetryable() {
        return RETRYABLE_CODES.contains(this.aerospikeException.getResultCode());
    }

    public AerospikeException getCause() {
        return this.aerospikeException;
    }
}
