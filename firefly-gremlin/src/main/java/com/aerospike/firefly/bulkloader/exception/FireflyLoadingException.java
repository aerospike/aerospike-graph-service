package com.aerospike.firefly.bulkloader.exception;

import com.aerospike.client.AerospikeException;

/**
 * Wrapper Exception class for errors encountered when Bulk Loading that contains the context for whether the Bulk
 * Loading API call that returned the Exception can be retried.
 */
public class FireflyLoadingException extends RuntimeException {
    final private boolean isRetryable;
    final private AerospikeException aerospikeException;

    public FireflyLoadingException(final AerospikeException cause, final boolean isRetryable) {
        super(cause);
        this.isRetryable = isRetryable;
        this.aerospikeException = cause;
    }

    public FireflyLoadingException(final String message, final AerospikeException cause, boolean isRetryable) {
        super(message, cause);
        this.isRetryable = isRetryable;
        this.aerospikeException = cause;
    }

    public boolean isRetryable() {
        return this.isRetryable;
    }

    public AerospikeException getCause() {
        return this.aerospikeException;
    }
}
