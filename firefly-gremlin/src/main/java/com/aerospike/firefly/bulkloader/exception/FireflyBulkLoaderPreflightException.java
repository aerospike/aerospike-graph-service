package com.aerospike.firefly.bulkloader.exception;

public class FireflyBulkLoaderPreflightException extends FireflyBulkLoaderException {

    public FireflyBulkLoaderPreflightException(String message) {
        super(message);
    }

    public FireflyBulkLoaderPreflightException(Throwable t) {
        super(t);
    }
}
