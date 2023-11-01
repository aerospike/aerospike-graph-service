package com.aerospike.firefly.runtime.exceptions;

import com.aerospike.client.AerospikeException;

public class RecordTooBigException extends RuntimeException {
    public static final String RECORD_TOO_BIG = "Error: Vertex / Edge exceeded max size. " +
            "This can be due to too many Properties / Edges added to an Element. Consider breaking " +
            "this Vertex / Edge into more Elements.";

    public RecordTooBigException(final AerospikeException cause) {
        super(RECORD_TOO_BIG, cause);
    }
}
