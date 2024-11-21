package com.aerospike.firefly.util.exceptions;

public class SindexRecentlyDroppedException extends AerospikeGraphException {
    public SindexRecentlyDroppedException(final String sindexName, final long metadataUpdateFrequencySeconds) {
        super(GraphError.SINDEX_RECENTLY_DROPPED, String.format(GraphError.getMessage(GraphError.SINDEX_RECENTLY_DROPPED), sindexName, metadataUpdateFrequencySeconds));
    }
}
