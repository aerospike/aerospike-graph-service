package com.aerospike.firefly.util.exceptions;

public class TxNotEnabledException extends AerospikeGraphException {
    public TxNotEnabledException(final String graphName) {
        super(GraphError.TX_NOT_ENABLED, String.format(GraphError.getMessage(GraphError.TX_NOT_ENABLED), graphName));
    }
}
