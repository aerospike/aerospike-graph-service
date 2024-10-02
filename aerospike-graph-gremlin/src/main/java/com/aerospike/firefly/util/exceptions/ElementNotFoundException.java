package com.aerospike.firefly.util.exceptions;

import com.aerospike.client.AerospikeException;

public class ElementNotFoundException extends AerospikeGraphException {

    public ElementNotFoundException(final AerospikeException cause) {
        super(GraphError.ELEMENT_NOT_FOUND, cause);
    }
}
