package com.aerospike.firefly.util.exceptions;

import org.apache.maven.artifact.versioning.ComparableVersion;

public class ThreadLimitExceededException extends AerospikeGraphException {
    public ThreadLimitExceededException() {
        super(GraphError.THREAD_LIMIT_EXCEEDED, String.format(GraphError.getMessage(GraphError.THREAD_LIMIT_EXCEEDED)));
    }
}
