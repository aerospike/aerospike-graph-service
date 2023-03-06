package com.aerospike.firefly.bulkloader.spark.executorservice;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.firefly.bulkloader.SparkBulkLoader;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge;
import com.aerospike.firefly.bulkloader.util.FireflyBulkLoaderException;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class EdgeWriteThread implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeWriteThread.class);
    private final Set<Long> supernodes;
    private final boolean ignoreFailedProperties;
    private final boolean useProvidedId;
    private final boolean keepProvidedId;
    private final String providedIdPropertyName;
    private final boolean ignoreElementCreationFailed;
    private final String nullValue;
    private final FireflyGraph graph;
    private final AtomicInteger outEdgeCount;
    private final AtomicInteger inEdgeCount;
    private final Map<Long, Map<String, List<Value>>> vertexOutEdgeMap;
    private final Map<Long, Map<String, List<Value>>> vertexInEdgeMap;
    private final GenericRowWithSchema row;
    private static final int RETRY_LIMIT = 100;
    
    public EdgeWriteThread(Set<Long> supernodes, boolean ignoreFailedProperties, boolean useProvidedId, boolean keepProvidedId, String providedIdPropertyName, boolean ignoreElementCreationFailed, String nullValue, FireflyGraph graph, AtomicInteger outEdgeCount, AtomicInteger inEdgeCount, Map<Long, Map<String, List<Value>>> vertexOutEdgeMap, Map<Long, Map<String, List<Value>>> vertexInEdgeMap, GenericRowWithSchema row) {
        this.supernodes = supernodes;
        this.ignoreFailedProperties = ignoreFailedProperties;
        this.useProvidedId = useProvidedId;
        this.keepProvidedId = keepProvidedId;
        this.providedIdPropertyName = providedIdPropertyName;
        this.ignoreElementCreationFailed = ignoreElementCreationFailed;
        this.nullValue = nullValue;
        this.graph = graph;
        this.outEdgeCount = outEdgeCount;
        this.inEdgeCount = inEdgeCount;
        this.vertexOutEdgeMap = vertexOutEdgeMap;
        this.vertexInEdgeMap = vertexInEdgeMap;
        this.row = row;
    }

    @Override
    public void run() {
        try {
            final SparkFireflyEdge sparkEdge = SparkFireflyEdge.createEdge(this.row, this.ignoreFailedProperties,
                    this.useProvidedId, this.keepProvidedId, this.providedIdPropertyName, this.nullValue, this.graph);
            final FireflyId edgeId = sparkEdge.getFireflyId(this.graph.getBaseGraph().EDGE_AERO_SET);
            final long inVertexId = sparkEdge.getInVertexId();
            final long outVertexId = sparkEdge.getOutVertexId();
            final String edgeLabel = sparkEdge.getLabel();
            int tryCount = 0;
            boolean succeeded = false;
            while (!succeeded) {
                try {
                    this.graph.bulkWriteEdge(sparkEdge.getId(), edgeLabel, sparkEdge.getProperties(),
                            inVertexId, outVertexId);
                    succeeded = true;
                } catch (final AerospikeException e) {
                    if (++tryCount > RETRY_LIMIT) {
                        LOGGER.error("Failed to write edge " + outVertexId + "--" + edgeLabel + "->" +
                                inVertexId + " after " + tryCount + " attempts.", e);
                        if (!ignoreElementCreationFailed) {
                            throw e;
                        } else {
                            break;
                        }
                    } else {
                        LOGGER.warn("Failed to write edge " + outVertexId + "--" + edgeLabel + "->" +
                                inVertexId + ". Attempting to write edge again. Attempt count: "
                                + tryCount + ".", e);
                        SparkBulkLoader.exponentialBackoff(tryCount);
                        continue;
                    }
                }

                // Write edge to vertices' edge caches.
                if (!graph.getBaseGraph().EDGE_CACHE_DISABLED_GLOBALLY) {
                    SparkBulkLoader.loadEdgeMap(graph, supernodes, outVertexId,
                            graph.getIdFactory().createCompositeEdgeId(edgeId, graph.getIdFactory().createId(inVertexId, FireflyVertex.class)),
                            edgeLabel, Direction.OUT, outEdgeCount, vertexOutEdgeMap,
                            ignoreElementCreationFailed);
                    SparkBulkLoader.loadEdgeMap(graph, supernodes, inVertexId,
                            graph.getIdFactory().createCompositeEdgeId(edgeId, graph.getIdFactory().createId(outVertexId, FireflyVertex.class)),
                            edgeLabel, Direction.IN, inEdgeCount, vertexInEdgeMap,
                            ignoreElementCreationFailed);
                }
            }
        } catch (final FireflyBulkLoaderException e) {
            LOGGER.warn("Failed to load edge for row: " + Arrays.toString(row.values()), e);
            if (!ignoreElementCreationFailed) {
                throw e;
            }
        }
    }
}
