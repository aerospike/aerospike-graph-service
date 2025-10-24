package com.aerospike.firefly.util.exceptions;

public class VertexAlreadyExistsFailure extends AerospikeGraphException {
    public VertexAlreadyExistsFailure(final String errorMessage) {
        super(GraphError.WRITE_VERTEX_DOUBLE_FAILURE, String.format(
                GraphError.getMessage(GraphError.WRITE_VERTEX_DOUBLE_FAILURE),
                errorMessage));
    }
}
