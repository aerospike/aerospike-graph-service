package com.aerospike.firefly.bulkloader.graph;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyLoadingException;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.aerospike.firefly.bulkloader.SparkBulkLoaderMain.exponentialBackoff;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.RETRY_LIMIT;

/**
 * Class containing graph operation functions (Vertex/Edge load/write)
 */
public class GraphOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphOperations.class);

    public static void updateEdgeMap(final Set<Object> supernodes,
                                                 final Object vertexId,
                                                 final FireflyId cachedEdgeId,
                                                 final String edgeLabel,
                                                 final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> edgeMap) {
        synchronized (GraphOperations.class) {
            if (!supernodes.contains(vertexId)) {
                edgeMap
                        .computeIfAbsent(vertexId, k -> new ConcurrentHashMap<>())
                        .computeIfAbsent(edgeLabel, k -> new HashSet<>())
                        .add(Value.get(cachedEdgeId.getCachedId()));
                edgeMap
                        .get(vertexId)
                        .computeIfAbsent(edgeLabel, k -> new HashSet<>())
                        .add(Value.get(cachedEdgeId.getCachedId()));
            }
        }
    }

    static private void writeEdgesToFireflyVertex(final FireflyGraph graph,
                                                  final Object vertexId,
                                                  final Direction direction,
                                                  final String label,
                                                  final List<Value> edgeIds,
                                                  final boolean allowDetachedEdges,
                                                  final Set<byte[]> invalidEdgeIds) {
        int tryCount = 0;
        while (true) {
            try {
                graph.bulkWriteEdgesToVertexCache(graph.getIdFactory().createId(vertexId, FireflyVertex.class), direction, edgeIds, label);
                break;
            } catch (final FireflyLoadingException e) {
                final AerospikeException cause = e.getCause();
                if (!e.isRetryable()) {
                    if (cause.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                        if (allowDetachedEdges) {
                            LOGGER.warn("Failed to write edges with label " + label + " into " + direction +
                                    " edge cache for vertex ID " + vertexId + " due to vertex record key not found.", cause);
                            for (final Value edgeId: edgeIds) {
                                final byte[] edgeIdByte = (byte[]) edgeId.getObject();
                                invalidEdgeIds.add(edgeIdByte);
                            }
                            break;
                        } else {
                            LOGGER.error("Failed to write edges with label " + label + " into " + direction +
                                    " edge cache for vertex ID " + vertexId + " due to vertex record key not found.", cause);
                            throw cause;
                        }
                    } else if (cause.getResultCode() == ResultCode.RECORD_TOO_BIG) {
                        LOGGER.error("Failed to write edges with label " + label + " into " + direction +
                                " edge cache for vertex ID " + vertexId + " due to vertex record size too big.", cause);
                    } else {
                        LOGGER.error("Failed to write edges with label " + label + " into " + direction +
                                " edge cache for vertex ID " + vertexId, cause);
                    }
                    throw cause;
                } else if (++tryCount > RETRY_LIMIT) {
                    LOGGER.error("Failed to write edges with label " + label + " into " + direction +
                            " edge cache for vertex ID " + vertexId + " after " + tryCount + " attempts.", cause);
                    throw cause;
                } else {
                    LOGGER.warn("Failed to write edges with label " + label + " into " + direction +
                            " edge cache for vertex ID " + vertexId +
                            ". Attempt count: " + tryCount, cause);
                    exponentialBackoff(tryCount);
                }
            }
        }
    }

    public static void flushEdgeMap(final FireflyGraph graph,
                                    final Direction direction,
                                    final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> edgeMap,
                                    final boolean allowDetachedEdges,
                                    final Set<byte[]> invalidEdgeIds) {
        for (Map.Entry<Object, ConcurrentHashMap<String, Set<Value>>> vertexIdToLabelMaps : edgeMap.entrySet()) {
            final Object vertexId = vertexIdToLabelMaps.getKey();
            final ConcurrentHashMap<String, Set<Value>> labelMaps = vertexIdToLabelMaps.getValue();
            for (Map.Entry<String, Set<Value>> labelToEdgeIds : labelMaps.entrySet()) {
                try {
                    writeEdgesToFireflyVertex(graph, vertexId, direction, labelToEdgeIds.getKey(),
                            new ArrayList<>(labelToEdgeIds.getValue()), allowDetachedEdges, invalidEdgeIds);
                } catch (final RuntimeException e) {
                    LOGGER.error("Exception occurred while loading edges '{}' into vertex with id '{}'. Error message '{}'.",
                            labelToEdgeIds.getValue(), vertexId, e.getMessage(), e);
                    throw e;
                }
            }
        }
        edgeMap.clear();
    }

    public static void dropDetachedEdges(final FireflyGraph graph, final Set<byte[]> invalidEdgeIds) {
        final Set<FireflyIdComposite> invalidEdges = new HashSet<>();
        invalidEdgeIds.forEach(idBytes -> {
            final FireflyIdComposite id = new FireflyIdComposite(graph.getBaseGraph(), idBytes);
            invalidEdges.add(id);
        });
        final GraphTraversalSource g = graph.traversal();

        for (final FireflyIdComposite id : invalidEdges) {
            int tryCount = 0;
            while (true) {
                try {
                    g.E(id).drop().iterate();
                    break;
                } catch (final AerospikeException e) {
                    final FireflyLoadingException fle = new FireflyLoadingException(e);
                    if (!fle.isRetryable()) {
                        LOGGER.error("Unexpected exception when attempting to purge detached Edge: ", e);
                        throw e;
                    } else if (++tryCount > RETRY_LIMIT) {
                        LOGGER.error("Failed to purge detached Edge after " + tryCount + " attempts.", e);
                        throw e;
                    } else {
                        LOGGER.warn("Failed to purge detached Edge. Attempt count: " + tryCount, e);
                        exponentialBackoff(tryCount);
                    }
                }
            }
        }
    }
}
