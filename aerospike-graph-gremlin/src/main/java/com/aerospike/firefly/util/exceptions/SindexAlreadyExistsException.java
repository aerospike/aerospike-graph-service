package com.aerospike.firefly.util.exceptions;

import java.util.List;

public class SindexAlreadyExistsException extends AerospikeGraphException {
    public final String sindexName;

    public SindexAlreadyExistsException(final String sindexName) {
        super(GraphError.SINDEX_ALREADY_EXISTS, String.format(GraphError.getMessage(GraphError.SINDEX_ALREADY_EXISTS), sindexName));
        this.sindexName = sindexName;
    }

    public SindexAlreadyExistsException(final List<String> sindexNames) {
        super(GraphError.SINDEX_ALREADY_EXISTS, String.format(GraphError.getMessage(GraphError.SINDEX_ALREADY_EXISTS) , String.join(", ", sindexNames)));
        this.sindexName = String.join(", ", sindexNames);
    }

}
