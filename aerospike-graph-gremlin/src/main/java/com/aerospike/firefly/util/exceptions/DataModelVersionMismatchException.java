package com.aerospike.firefly.util.exceptions;

import org.apache.maven.artifact.versioning.ComparableVersion;

public class DataModelVersionMismatchException extends AerospikeGraphException {
    public DataModelVersionMismatchException(final ComparableVersion diskVersion, final ComparableVersion classVersion) {
        super(GraphError.DATA_MODEL_VERSION_MISMATCH, String.format(GraphError.getMessage(GraphError.DATA_MODEL_VERSION_MISMATCH), diskVersion, classVersion));
    }
}
