package com.aerospike.firefly.bulkloader.graph;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aerospike.firefly.bulkloader.SparkBulkLoader.exponentialBackoff;

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
                }

                if (e.getInDoubt()) {
                    LOGGER.warn("Failed to write edges with label " + label + " into " + direction +
                            " edge cache for vertex ID " + vertexId +
                            ". Write was in doubt; attempting to recover by retrying not written IDs. Attempt count: " +
                            tryCount, e);

                    int doubtTryCount = 0;
                    boolean doubtSuccessful = false;
                    while (!doubtSuccessful) {
                        try {
                            final FireflyVertex fireflyVertex = graph.readVertex(graph.getIdFactory().createId(vertexId, FireflyVertex.class));
                            doubtSuccessful = true;

                            final Iterator<Edge> edges = fireflyVertex.edges(direction, label);
                            final HashSet<FireflyId> writtenEdgeIds = new HashSet<>();
                            edges.forEachRemaining(edge -> writtenEdgeIds.add(((FireflyEdge) edge).id));

                            final List<Value> edgeIdsToRemove = Collections.synchronizedList(new ArrayList<>());
                            for (final Value edgeId : edgeIds) {
                                final FireflyIdComposite id = (FireflyIdComposite) graph.getIdFactory().createId(edgeId.getObject(), FireflyEdge.class);
                                if (writtenEdgeIds.contains(id.getEdgeId())) {
                                    edgeIdsToRemove.add(edgeId);
                                }
                            }
                            LOGGER.warn("About to retry with {} edge IDs removed from list of size {}.",
                                    edgeIdsToRemove.size(), edgeIds.size());
                            edgeIds.removeAll(edgeIdsToRemove);
                        } catch (final AerospikeException doubtE) {
                            if (++doubtTryCount > RETRY_LIMIT) {
                                LOGGER.error("Failed to read in doubt edge IDs after " + doubtTryCount + " attempts.", e);
                                if (!ignoreElementCreationFailed) {
                                    throw e;
                                } else {
                                    break;
                                }
                            } else {
                                LOGGER.warn("Failed to read in doubt edge IDs. Attempting to read again. Attempt count: " +
                                        doubtTryCount);
                                exponentialBackoff(doubtTryCount);
                            }
                        }
                    }

                    if (edgeIds.isEmpty()) {
                        // All the edge IDs were added to the cache - stop retrying.
                        LOGGER.warn("In doubt write to edge cache was actually successful. Stopping retries.");
                        successful = true;
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
                writeEdgesToFireflyVertex(graph, vertexId, direction, labelToEdgeIds.getKey(),
                        labelToEdgeIds.getValue(), ignoreElementCreationFailed);
            }
        }
        edgeMap.clear();
    }
}
