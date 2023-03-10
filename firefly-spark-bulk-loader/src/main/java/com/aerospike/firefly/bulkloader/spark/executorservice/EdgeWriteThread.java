package com.aerospike.firefly.bulkloader.spark.executorservice;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.firefly.bulkloader.SparkBulkLoader;
import com.aerospike.firefly.bulkloader.graph.GraphOperations;
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
    private final Set<Object> supernodes;
    private final boolean ignoreFailedProperties;
    private final boolean useProvidedId;
    private final boolean keepProvidedId;
    private final String providedIdPropertyName;
    private final boolean ignoreElementCreationFailed;
    private final String nullValue;
    private final FireflyGraph graph;
    private final AtomicInteger outEdgeCount = new AtomicInteger(0);
    private final AtomicInteger inEdgeCount = new AtomicInteger(0);
    private final Map<Object, Map<String, List<Value>>> vertexOutEdgeMap;
    private final Map<Object, Map<String, List<Value>>> vertexInEdgeMap;
    private final GenericRowWithSchema row;
    private static final int RETRY_LIMIT = 100;
    private final int partitionId;
    
    public EdgeWriteThread(final Set<Object> supernodes,
                           final boolean ignoreFailedProperties,
                           final boolean useProvidedId,
                           final boolean keepProvidedId,
                           final String providedIdPropertyName,
                           final boolean ignoreElementCreationFailed,
                           final String nullValue, FireflyGraph graph,
                           final Map<Object, Map<String, List<Value>>> vertexOutEdgeMap,
                           final Map<Object, Map<String, List<Value>>> vertexInEdgeMap,
                           final GenericRowWithSchema row,
                           final int partitionId) {
        this.supernodes = supernodes;
        this.ignoreFailedProperties = ignoreFailedProperties;
        this.useProvidedId = useProvidedId;
        this.keepProvidedId = keepProvidedId;
        this.providedIdPropertyName = providedIdPropertyName;
        this.ignoreElementCreationFailed = ignoreElementCreationFailed;
        this.nullValue = nullValue;
        this.graph = graph;
        this.vertexOutEdgeMap = vertexOutEdgeMap;
        this.vertexInEdgeMap = vertexInEdgeMap;
        this.row = row;
        this.partitionId = partitionId;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Write-edge-thread-for-partitionId-" + this.partitionId);
        try {
            final SparkFireflyEdge sparkEdge = SparkFireflyEdge.createEdge(this.row, this.ignoreFailedProperties,
                    this.useProvidedId, this.keepProvidedId, this.providedIdPropertyName, this.nullValue, this.graph);
            final FireflyId edgeId = sparkEdge.getFireflyId(this.graph.getBaseGraph().EDGE_AERO_SET);
            final Object inVertexId = sparkEdge.getInVertexId();
            final Object outVertexId = sparkEdge.getOutVertexId();
            final String edgeLabel = sparkEdge.getLabel();
            int tryCount = 0;
            boolean succeeded = false;
            while (!succeeded) {
                try {
                    this.graph.bulkWriteEdge((Long) sparkEdge.getId(), edgeLabel, sparkEdge.getProperties(),
                            inVertexId, outVertexId);
                    succeeded = true;
                } catch (final AerospikeException e) {
                    if (++tryCount > RETRY_LIMIT) {
                        LOGGER.error("Failed to write edge " + outVertexId + "--" + edgeLabel + "->" +
                                inVertexId + " after " + tryCount + " attempts.", e);
                        if (!this.ignoreElementCreationFailed) {
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
            }
        } catch (final FireflyBulkLoaderException e) {
            LOGGER.warn("Failed to load edge for row: " + Arrays.toString(this.row.values()), e);
            if (!this.ignoreElementCreationFailed) {
                throw e;
            }
        }
        LOGGER.info("Finished " + Thread.currentThread().getName() + " for edge id = " + this.row.getAs("~id"));
    }
}
