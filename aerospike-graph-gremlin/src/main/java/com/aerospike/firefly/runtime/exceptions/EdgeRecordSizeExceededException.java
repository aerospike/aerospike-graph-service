package com.aerospike.firefly.runtime.exceptions;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;

public class EdgeRecordSizeExceededException extends RuntimeException {
    private static final String ADD_EDGE_BASE_MESSAGE = "Record size exceeded for Edge pack when attempting to add Edge with ID %s";
    private static final String ADD_PROPERTY_BASE_MESSAGE = "Record size exceeded for Edge pack when writing Property key %s for Edge with ID %s";
    public final long edgePackCount;
    public final long propertyCount;
    private EdgeRecordSizeExceededException(final String message, final AerospikeException cause,
                                            final long edgePackCount, final long propertyCount) {
        super(message, cause);
        this.edgePackCount = edgePackCount;
        this.propertyCount = propertyCount;
    }

    public static EdgeRecordSizeExceededException fromAddingEdge(final AerospikeException cause,
                                                                 final AerospikeConnection db,
                                                                 final Key key,
                                                                 final FireflyId edgeId) {
        final String baseMessage = String.format(ADD_EDGE_BASE_MESSAGE, getUserIdString(edgeId.getUserId()));
        final Map<?, Map<?, ?>> phatEdgePropertyMap = getPhatEdgeProperties(db, key);
        return new EdgeRecordSizeExceededException(buildMessage(baseMessage, phatEdgePropertyMap), cause,
                phatEdgePropertyMap.size(), getEdgePropertyTotalCount(phatEdgePropertyMap));
    }

    public static EdgeRecordSizeExceededException fromAddingProperty(final AerospikeException cause,
                                                                     final AerospikeConnection db,
                                                                     final Key key,
                                                                     final FireflyId edgeId,
                                                                     final String propertyKey) {
        final String baseMessage = String.format(ADD_PROPERTY_BASE_MESSAGE, propertyKey, getUserIdString(edgeId.getUserId()));
        final Map<?, Map<?, ?>> phatEdgePropertyMap = getPhatEdgeProperties(db, key);
        return new EdgeRecordSizeExceededException(buildMessage(baseMessage, phatEdgePropertyMap), cause,
                phatEdgePropertyMap.size(), getEdgePropertyTotalCount(phatEdgePropertyMap));
    }

    public static Map<?, Map<?, ?>> getPhatEdgeProperties(final AerospikeConnection db, final Key key) {
        final Operation getProperties = Operation.get(db.PROPERTIES_BIN);
        return (Map<?, Map<?, ?>>) db.operate(null, key, getProperties).getMap(db.PROPERTIES_BIN);
    }

    private static String buildMessage(final String baseMessage, final Map<?, Map<?, ?>> phatEdgePropertyMap) {
        final StringBuilder builder = new StringBuilder(baseMessage);
        builder.append("\nPacked Edge count: ");
        builder.append(phatEdgePropertyMap.keySet().size());
        for (final Map.Entry<?, Map<?, ?>> entry : phatEdgePropertyMap.entrySet()) {
            builder.append("\nEdge ID " + getUserIdString(entry.getKey()) + " property count: ");
            builder.append(entry.getValue().size());
        }
        return builder.toString();
    }

    private static long getEdgePropertyTotalCount(final Map<?, Map<?, ?>> phatEdgePropertyMap) {
        return phatEdgePropertyMap.values().stream().mapToLong(Map::size).sum();
    }

    public static String getUserIdString(final Object userId) {
        return Base64.getEncoder().encodeToString(((ByteBuffer) userId).array());
    }
}
