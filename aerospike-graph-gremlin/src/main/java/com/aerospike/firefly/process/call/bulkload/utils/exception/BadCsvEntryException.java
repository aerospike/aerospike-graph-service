package com.aerospike.firefly.process.call.bulkload.utils.exception;

public class BadCsvEntryException extends RuntimeException {

    public BadCsvEntryException(String message) {
        super(message);
    }
    public BadCsvEntryException(Throwable t) {
        super(t);
    }
}
