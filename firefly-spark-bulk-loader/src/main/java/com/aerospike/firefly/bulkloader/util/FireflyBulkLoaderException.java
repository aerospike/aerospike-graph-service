package com.aerospike.firefly.bulkloader.util;

public class FireflyBulkLoaderException extends RuntimeException {

    public FireflyBulkLoaderException(String message) {
        super(message);
    }
    public FireflyBulkLoaderException(Throwable t) {
        super(t);
    }
}
