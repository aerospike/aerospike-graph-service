package com.aerospike.firefly.bulkloader.spark.executorservice;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.firefly.bulkloader.SparkBulkLoader;
import com.aerospike.firefly.bulkloader.graph.GraphOperations;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge;
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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class EdgeWriteThread implements Callable<Boolean> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeWriteThread.class);
    private final Set<Object> supernodes;
    private final boolean keepProvidedId;
    private final String providedIdPropertyName;
    private final boolean ignoreElementCreationFailed;
    private final String nullValue;
    private final FireflyGraph graph;
    private final AtomicInteger outEdgeCount = new AtomicInteger(0);
    private final AtomicInteger inEdgeCount = new AtomicInteger(0);
    private final Map<Object, Map<String, List<Value>>> vertexOutEdgeMap;
    private final Map<Object, Map<String, List<Value>>> vertexInEdgeMap;
    private final GenericRowWithSchema fireflyRow;
    private final GenericRowWithSchema fireflyMetadataRow;
    private static final int RETRY_LIMIT = 100;
    private final int partitionId;
    
    public EdgeWriteThread(final Set<Object> supernodes,
                           final boolean keepProvidedId,
                           final String providedIdPropertyName,
                           final boolean ignoreElementCreationFailed,
                           final String nullValue, FireflyGraph graph,
                           final Map<Object, Map<String, List<Value>>> vertexOutEdgeMap,
                           final Map<Object, Map<String, List<Value>>> vertexInEdgeMap,
                           final GenericRowWithSchema rowForFirefly,
                           final int partitionId,
                           final GenericRowWithSchema fireflyMetadataRow) {
        this.supernodes = supernodes;
        this.keepProvidedId = keepProvidedId;
        this.providedIdPropertyName = providedIdPropertyName;
        this.ignoreElementCreationFailed = ignoreElementCreationFailed;
        this.nullValue = nullValue;
        this.graph = graph;
        this.vertexOutEdgeMap = vertexOutEdgeMap;
        this.vertexInEdgeMap = vertexInEdgeMap;
        this.fireflyRow = rowForFirefly;
        this.partitionId = partitionId;
        this.fireflyMetadataRow = fireflyMetadataRow;
    }

    /**
     * Function to write edge to Aerospike.
     *
     * @return true if error occurred while writing edge, false otherwise.
     */
    @Override
    public Boolean call() {
        Thread.currentThread().setName("Write-edge-thread-for-partitionId-" + this.partitionId);
        final SparkFireflyEdge sparkEdge = SparkFireflyEdge.createEdge(this.fireflyRow, this.keepProvidedId, this.providedIdPropertyName, this.nullValue, this.graph, false);
        final FireflyId edgeId = sparkEdge.getFireflyId(this.graph.getBaseGraph());
        final Object inVertexId = sparkEdge.getInVertexId();
        final Object outVertexId = sparkEdge.getOutVertexId();
        final String edgeLabel = sparkEdge.getLabel();
        int tryCount = 0;
        while (true) {
            try {
                this.graph.bulkWriteEdge((Long) sparkEdge.getId(), edgeLabel, sparkEdge.getProperties(),
                        inVertexId, outVertexId, supernodes.contains(inVertexId), supernodes.contains(outVertexId));
            } catch (final AerospikeException e) {
                if (e.getResultCode() == ResultCode.RECORD_TOO_BIG) {
                    // No point in retrying this kind of error.
                    LOGGER.error("Record too big for edge with id '{}', label '{}', properties '{}'. FireflyRow value: '{}', FireflyMetadataRow value: '{}'",
                            sparkEdge.getFireflyId(this.graph.getBaseGraph()), sparkEdge.getLabel(), sparkEdge.getProperties(), Arrays.toString(fireflyRow.values()), Arrays.toString(fireflyMetadataRow.values()));
                    // If ignoreElementCreationFailed is true and an error occurred, we should ignore the error (return false).
                    // If ignoreElementCreationFailed is false and an error occurred, we should return that an error occurred (return true).
                    return !this.ignoreElementCreationFailed;
                }
                if (++tryCount > RETRY_LIMIT) {
                    LOGGER.error("Failed to write edge " + outVertexId + "--" + edgeLabel + "->" +
                            inVertexId + " after " + tryCount + " attempts.", e);
                    // If ignoreElementCreationFailed is true and an error occurred, we should ignore the error (return false).
                    // If ignoreElementCreationFailed is false and an error occurred, we should return that an error occurred (return true).
                    return !this.ignoreElementCreationFailed;
                } else {
                    LOGGER.warn("Failed to write edge " + outVertexId + "--" + edgeLabel + "->" +
                            inVertexId + ". Attempting to write edge again. Attempt count: "
                            + tryCount + ".", e);
                    SparkBulkLoader.exponentialBackoff(tryCount);
                    continue;
                }
            }

            // Write edge to vertices' edge caches.
            if (!this.graph.getBaseGraph().EDGE_CACHE_DISABLED_GLOBALLY) {
                GraphOperations.loadEdgeMap(this.graph, this.supernodes, outVertexId,
                        this.graph.getIdFactory().createCompositeEdgeId(edgeId, graph.getIdFactory().createId(inVertexId, FireflyVertex.class)),
                        edgeLabel, Direction.OUT, this.outEdgeCount, this.vertexOutEdgeMap,
                        this.ignoreElementCreationFailed);
                GraphOperations.loadEdgeMap(this.graph, this.supernodes, inVertexId,
                        this.graph.getIdFactory().createCompositeEdgeId(edgeId, this.graph.getIdFactory().createId(outVertexId, FireflyVertex.class)),
                        edgeLabel, Direction.IN, this.inEdgeCount, this.vertexInEdgeMap,
                        this.ignoreElementCreationFailed);
            }

            // Everything succeeded, return false.
            return false;
        }
    }
}
