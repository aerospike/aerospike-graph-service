package com.aerospike.firefly.process.call.bulkload.utils.exception;

public class FireflyBulkLoaderCsvException extends FireflyBulkLoaderException {

    public FireflyBulkLoaderCsvException(String message) {
        super(message);
    }
    public FireflyBulkLoaderCsvException(Throwable t) {
        super(t);
    }
}
