package com.aerospike.firefly.util.exceptions;

public class ThreadLimitExceededException extends AerospikeGraphException {
    public ThreadLimitExceededException() {
        super(GraphError.THREAD_LIMIT_EXCEEDED, String.format(GraphError.getMessage(GraphError.THREAD_LIMIT_EXCEEDED)));
    }
}
