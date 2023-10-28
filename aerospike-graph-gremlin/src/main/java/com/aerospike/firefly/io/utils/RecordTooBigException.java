package com.aerospike.firefly.io.utils;

import com.aerospike.client.AerospikeException;

import static com.aerospike.firefly.io.utils.ExceptionMessages.RECORD_TOO_BIG;

public class RecordTooBigException extends RuntimeException {

    public RecordTooBigException(final AerospikeException cause) {
        super(RECORD_TOO_BIG, cause);
    }
}
