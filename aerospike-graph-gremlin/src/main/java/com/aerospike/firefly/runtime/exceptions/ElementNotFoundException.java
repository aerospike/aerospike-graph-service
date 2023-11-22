package com.aerospike.firefly.runtime.exceptions;

import com.aerospike.client.AerospikeException;
import com.aerospike.firefly.structure.FireflyElement;


public class ElementNotFoundException extends RuntimeException {
    public static final String ELEMENT_NOT_FOUND = "Error: Element was dropped and no longer exists.";

    public ElementNotFoundException(final FireflyElement element, final AerospikeException cause) {
        super(ELEMENT_NOT_FOUND + element, cause);
    }

    public ElementNotFoundException(final AerospikeException cause) {
        super(ELEMENT_NOT_FOUND, cause);
    }

    public ElementNotFoundException() {
        super(ELEMENT_NOT_FOUND);
    }
}
