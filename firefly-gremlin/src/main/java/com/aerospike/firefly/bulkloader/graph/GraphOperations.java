package com.aerospike.firefly.bulkloader.graph;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aerospike.firefly.bulkloader.SparkBulkLoaderMain.exponentialBackoff;

/**
 * Class containing all graph operation functions (Vertex/Edge load/write)
 * All functions are statically implemented to avoid creations of objects within Spark's distributed computing transformations
 */
public class GraphOperations {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphOperations.class);
    private static final int EDGE_CACHE_FLUSH_THRESHOLD = 100000;
    private static final int RETRY_LIMIT = 100;
    
    public static void loadEdgeMap(final FireflyGraph graph,
                                   final Set<Object> supernodes,
                                   final Object vertexId,
                                   final FireflyId cachedEdgeId,
                                   final String edgeLabel,
                                   final Direction direction,
                                   final AtomicInteger edgeCount,
                                   final Map<Object, Map<String, List<Value>>> edgeMap,
                                   final boolean ignoreElementCreationFailed) {
        synchronized (GraphOperations.class) {
            if (!supernodes.contains(vertexId)) {
                if (!edgeMap.containsKey(vertexId)) {
                    edgeMap.put(vertexId, new ConcurrentHashMap<>());
                }
                final Map<String, List<Value>> labelEdgeIds = edgeMap.get(vertexId);
                if (!labelEdgeIds.containsKey(edgeLabel)) {
                    labelEdgeIds.put(edgeLabel, Collections.synchronizedList(new ArrayList<>()));
                }
                final List<Value> edgeIds = labelEdgeIds.get(edgeLabel);
                edgeIds.add(Value.get(cachedEdgeId.getCachedId()));
                final int count = edgeCount.incrementAndGet();
                if (count > EDGE_CACHE_FLUSH_THRESHOLD) {
                    flushEdgeMap(graph, direction, edgeMap, ignoreElementCreationFailed);
                    edgeCount.set(0);
                }
            }
        }
    }

    static private void writeEdgesToFireflyVertex(final FireflyGraph graph,
                                                  final Object vertexId,
                                                  final Direction direction,
                                                  final String label,
                                                  final List<Value> edgeIds,
                                                  final boolean ignoreElementCreationFailed) {
        int tryCount = 0;
        boolean successful = false;
        while (!successful) {
            try {
                graph.bulkWriteEdgesToVertexCache(graph.getIdFactory().createId(vertexId, FireflyVertex.class), direction, edgeIds, label);
                successful = true;
            } catch (final AerospikeException e) {
                if (++tryCount > RETRY_LIMIT) {
                    LOGGER.error("Failed to write edges with label " + label + " into " + direction +
                            " edge cache for vertex ID " + vertexId + " after " + tryCount + " attempts.", e);
                    if (!ignoreElementCreationFailed) {
                        throw e;
                    } else {
                        break;
                    }
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
                                    final Map<Object, Map<String, List<Value>>> edgeMap,
                                    final boolean ignoreElementCreationFailed) {
        for (Map.Entry<Object, Map<String, List<Value>>> vertexIdToLabelMaps : edgeMap.entrySet()) {
            final Object vertexId = vertexIdToLabelMaps.getKey();
            final Map<String, List<Value>> labelMaps = vertexIdToLabelMaps.getValue();
            for (Map.Entry<String, List<Value>> labelToEdgeIds : labelMaps.entrySet()) {
                try {
                    writeEdgesToFireflyVertex(graph, vertexId, direction, labelToEdgeIds.getKey(),
                            labelToEdgeIds.getValue(), ignoreElementCreationFailed);
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
