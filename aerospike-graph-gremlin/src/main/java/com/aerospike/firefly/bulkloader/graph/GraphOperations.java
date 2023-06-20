package com.aerospike.firefly.bulkloader.graph;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.firefly.bulkloader.exception.FireflyLoadingException;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
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
                                                  final List<Value> edgeIds) {
        int tryCount = 0;
        boolean successful = false;
        while (!successful) {
            try {
                graph.bulkWriteEdgesToVertexCache(graph.getIdFactory().createId(vertexId, FireflyVertex.class), direction, edgeIds, label);
                successful = true;
            } catch (final FireflyLoadingException e) {
                final AerospikeException cause = e.getCause();
                if (!e.isRetryable()) {
                    if (cause.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                        LOGGER.error("Failed to write edges with label " + label + " into " + direction +
                                " edge cache for vertex ID " + vertexId + " due to vertex record key not found.", e);
                    } else if (cause.getResultCode() == ResultCode.RECORD_TOO_BIG) {
                        LOGGER.error("Failed to write edges with label " + label + " into " + direction +
                                " edge cache for vertex ID " + vertexId + " due to vertex record size too big.", e);
                    } else {
                        LOGGER.error("Failed to write edges with label " + label + " into " + direction +
                                " edge cache for vertex ID " + vertexId, e);
                    }
                    throw cause;
                } else if (++tryCount > RETRY_LIMIT) {
                    LOGGER.error("Failed to write edges with label " + label + " into " + direction +
                            " edge cache for vertex ID " + vertexId + " after " + tryCount + " attempts.", e);
                    throw cause;
                } else {
                    LOGGER.warn("Failed to write edges with label " + label + " into " + direction +
                            " edge cache for vertex ID " + vertexId +
                            ". Write was not in doubt; retrying with all IDs. Attempt count: " + tryCount, e);
                    exponentialBackoff(tryCount);
                }
            }
        }
    }

    public static void flushEdgeMap(final FireflyGraph graph,
                                    final Direction direction,
                                    final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> edgeMap) {
        for (Map.Entry<Object, ConcurrentHashMap<String, Set<Value>>> vertexIdToLabelMaps : edgeMap.entrySet()) {
            final Object vertexId = vertexIdToLabelMaps.getKey();
            final ConcurrentHashMap<String, Set<Value>> labelMaps = vertexIdToLabelMaps.getValue();
            for (Map.Entry<String, Set<Value>> labelToEdgeIds : labelMaps.entrySet()) {
                try {
                    writeEdgesToFireflyVertex(graph, vertexId, direction, labelToEdgeIds.getKey(),
                            new ArrayList<>(labelToEdgeIds.getValue()));
                } catch (final RuntimeException e) {
                    LOGGER.error("Exception occurred while loading edges '{}' into vertex with id '{}'. Error message '{}'.",
                            labelToEdgeIds.getValue(), vertexId, e.getMessage(), e);
                    throw e;
                }
            }
        }
        edgeMap.clear();
    }
}
