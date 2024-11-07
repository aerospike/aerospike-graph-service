package com.aerospike.firefly.util.exceptions;

public class AerospikeGraphElementNotFoundException extends AerospikeGraphException {

    public AerospikeGraphElementNotFoundException() {
        super(GraphError.ELEMENT_NOT_FOUND);
    }
}
