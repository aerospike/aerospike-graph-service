package com.aerospike.firefly.util.exceptions;

import com.aerospike.client.AerospikeException;


public class AerospikeGraphRecordSizeExceededException extends AerospikeGraphException {

    AerospikeGraphRecordSizeExceededException(final AerospikeException cause) {
        super(GraphError.RECORD_SIZE_EXCEEDED, cause);
    }
}
