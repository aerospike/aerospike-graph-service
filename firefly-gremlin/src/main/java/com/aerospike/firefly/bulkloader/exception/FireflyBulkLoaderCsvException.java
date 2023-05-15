package com.aerospike.firefly.bulkloader.exception;

public class FireflyBulkLoaderCsvException extends FireflyBulkLoaderException {

    public FireflyBulkLoaderCsvException(String message) {
        super(message);
    }

    public FireflyBulkLoaderCsvException(Throwable t) {
        super(t);
    }
}
