package com.aerospike.firefly.spark.bulkloader.util;

public class FireflyBulkLoaderException extends RuntimeException {

    public FireflyBulkLoaderException(String message) {
        super(message);
    }
    public FireflyBulkLoaderException(Throwable t) {
        super(t);
    }
}
