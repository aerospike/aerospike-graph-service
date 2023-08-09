package com.aerospike.firefly.process.call.bulkload.utils.exception;

public class FireflyBulkLoaderException extends RuntimeException {

    public FireflyBulkLoaderException(String message) {
        super(message);
    }
    public FireflyBulkLoaderException(Throwable t) {
        super(t);
    }
}
