package com.aerospike.firefly.runtime.exceptions;

import static com.aerospike.firefly.structure.FireflyElement.TTL_PROPERTY_KEY;

public class TtlNotEnabledException extends IllegalArgumentException {
    private static final String MESSAGE = "TTL must be enabled to set '" + TTL_PROPERTY_KEY + "'.";
    public TtlNotEnabledException() {
        super(MESSAGE);
    }
}
