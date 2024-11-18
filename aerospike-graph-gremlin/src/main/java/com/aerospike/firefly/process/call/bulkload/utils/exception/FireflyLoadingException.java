package com.aerospike.firefly.process.call.bulkload.utils.exception;

import com.aerospike.client.ResultCode;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;

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

    final private AerospikeGraphException aerospikeGraphException;

    public FireflyLoadingException(final AerospikeGraphException cause) {
        super(cause);
        this.aerospikeGraphException = cause;
    }

    public FireflyLoadingException(final AerospikeGraphException cause, final String message) {
        super(message, cause);
        this.aerospikeGraphException = cause;
    }

    public boolean isRetryable() {
        return isRetryable(this.aerospikeGraphException);
    }

    public AerospikeGraphException getCause() {
        return this.aerospikeGraphException;
    }

    public static boolean isRetryable(final AerospikeGraphException ae) {
        return RETRYABLE_CODES.contains(ae.errorCode);
    }
}
