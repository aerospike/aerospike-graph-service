package com.aerospike.firefly.util.exceptions;

public class AerospikeCompoundIndexNotSupportedException extends AerospikeGraphException {

    public AerospikeCompoundIndexNotSupportedException() {
        super(GraphError.EXPRESSION_INDEX_NOT_SUPPORTED);
    }
}
