package com.aerospike.firefly.process.call.bulkload.utils.exception;

public class FireflyBulkLoaderPreflightException extends FireflyBulkLoaderException {

    public FireflyBulkLoaderPreflightException(String message) {
        super(message);
    }
    public FireflyBulkLoaderPreflightException(Throwable t) {
        super(t);
    }
}
