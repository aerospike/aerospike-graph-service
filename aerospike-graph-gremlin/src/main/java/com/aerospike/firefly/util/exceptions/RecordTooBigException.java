package com.aerospike.firefly.util.exceptions;

import com.aerospike.client.AerospikeException;


public class RecordTooBigException extends AerospikeGraphException {

    public RecordTooBigException(final AerospikeException cause) {
        super(GraphError.RECORD_SIZE_EXCEEDED, cause);
    }
}
