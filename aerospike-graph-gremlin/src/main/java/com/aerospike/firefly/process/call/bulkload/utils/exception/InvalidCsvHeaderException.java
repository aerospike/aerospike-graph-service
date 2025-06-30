package com.aerospike.firefly.process.call.bulkload.utils.exception;

public class InvalidCsvHeaderException extends RuntimeException {

    public InvalidCsvHeaderException(String message) {
        super(message);
    }
    public InvalidCsvHeaderException(Throwable t) {
        super(t);
    }
}
