package com.aerospike.firefly.util.exceptions;

public class AerospikeMrtNotSupportedException extends AerospikeGraphException {

    public AerospikeMrtNotSupportedException() {
        super(GraphError.MRT_NOT_SUPPORTED);
    }
}
