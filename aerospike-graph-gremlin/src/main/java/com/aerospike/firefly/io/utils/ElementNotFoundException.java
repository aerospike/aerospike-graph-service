package com.aerospike.firefly.io.utils;

import com.aerospike.client.AerospikeException;
import com.aerospike.firefly.structure.FireflyElement;

import static com.aerospike.firefly.io.utils.ExceptionMessages.ELEMENT_NOT_FOUND;

public class ElementNotFoundException extends RuntimeException {
    public ElementNotFoundException(final FireflyElement element, final AerospikeException cause) {
        super(ELEMENT_NOT_FOUND + element, cause);
    }

    public ElementNotFoundException(final AerospikeException cause) {
        super(ELEMENT_NOT_FOUND, cause);
    }
}
