package com.aerospike.firefly.util.exceptions;

public class TtlArgumentException extends AerospikeGraphException {
    public TtlArgumentException(final Object value) {
        super(GraphError.TTL_ILLEGAL_ARGUMENT, String.format(GraphError.getMessage(GraphError.TTL_ILLEGAL_ARGUMENT), value, value.getClass()));
    }
}
