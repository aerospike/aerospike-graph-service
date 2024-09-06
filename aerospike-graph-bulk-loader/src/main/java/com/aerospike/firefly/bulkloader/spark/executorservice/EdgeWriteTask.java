package com.aerospike.firefly.bulkloader.spark.executorservice;

import com.aerospike.client.Value;
import com.aerospike.firefly.bulkloader.graph.GraphOperations;
import com.aerospike.firefly.bulkloader.spark.EdgeOperations;
import com.aerospike.firefly.bulkloader.spark.resilience.ExponentialBackoffRetry;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

public class EdgeWriteTask {
    final ExponentialBackoffRetry retry;
    private final Logger LOGGER = LoggerFactory.getLogger(EdgeWriteTask.class);
    private final Set<Object> supernodes;
    private final boolean keepProvidedId;
    private final String providedIdPropertyName;
    private final String nullValue;
    private final FireflyGraph graph;
    private final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexOutEdgeMap;
    private final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexInEdgeMap;
    private final GenericRowWithSchema fireflyRow;
    private final GenericRowWithSchema fireflyMetadataRow;
    private final boolean edgeCacheEnabled;
    private final FireflyId edgeId;
    private final SparkFireflyEdge sparkEdge;
    private final Object inVertexId;
    private final Object outVertexId;
    private final String edgeLabel;
    private final boolean inVertexSupernode;
    private final boolean outVertexSupernode;

    public EdgeWriteTask(
            ExponentialBackoffRetry retry,
            final Set<Object> supernodes,
            final boolean keepProvidedId,
            final String providedIdPropertyName,
            final String nullValue, final FireflyGraph graph,
            final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexOutEdgeMap,
            final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexInEdgeMap,
            final GenericRowWithSchema rowForFirefly,
            final GenericRowWithSchema fireflyMetadataRow,
            final boolean usePersistedEdgeId) {
        this.retry = retry;
        this.supernodes = supernodes;
        this.keepProvidedId = keepProvidedId;
        this.providedIdPropertyName = providedIdPropertyName;
        this.nullValue = nullValue;
        this.graph = graph;
        this.vertexOutEdgeMap = vertexOutEdgeMap;
        this.vertexInEdgeMap = vertexInEdgeMap;
        this.fireflyRow = rowForFirefly;
        this.fireflyMetadataRow = fireflyMetadataRow;
        this.edgeCacheEnabled = this.graph.getBaseGraph().GLOBAL_EDGE_CACHE_ENABLED_FLAG;
        sparkEdge = SparkFireflyEdge.createEdge(fireflyRow, keepProvidedId,
                providedIdPropertyName, nullValue, graph, false,
                EdgeOperations.getEdgeIdSupplied(fireflyMetadataRow, usePersistedEdgeId));
        edgeId = sparkEdge.getFireflyId(this.graph.getBaseGraph());
        inVertexId = sparkEdge.getInVertexId();
        outVertexId = sparkEdge.getOutVertexId();
        edgeLabel = sparkEdge.getLabel();
        // If the edge cache is not enabled, then every edge must be written as if it were attached to a supernode.
        inVertexSupernode = !edgeCacheEnabled || supernodes.contains(inVertexId);
        outVertexSupernode = !edgeCacheEnabled || supernodes.contains(outVertexId);
    }

    public CompletionStage<Void> write(ScheduledExecutorService service) {
        final Supplier<CompletionStage<Void>> supplier = () -> CompletableFuture.supplyAsync(() -> {
            this.graph.bulkWriteEdge((byte[]) sparkEdge.getId(), edgeLabel, sparkEdge.getProperties(),
                    inVertexId, outVertexId, inVertexSupernode, outVertexSupernode);
            return null;
        }, service);
        return retry.withRetries(supplier, service).exceptionally(e -> {
            // Log the error when no longer retrying
            LOGGER.error(String.format("Exception occurred during Edge writing %s", this), e);
            throw new RuntimeException(e);
        });
    }

    public void updateCacheMap() {
        if (edgeCacheEnabled) {
            GraphOperations.updateEdgeMap(supernodes, outVertexId,
                    graph.getIdFactory().createCompositeEdgeId(edgeId, graph.getIdFactory().createId(inVertexId, FireflyVertex.class)),
                    edgeLabel, vertexOutEdgeMap);
            GraphOperations.updateEdgeMap(supernodes, inVertexId,
                    graph.getIdFactory().createCompositeEdgeId(edgeId, graph.getIdFactory().createId(outVertexId, FireflyVertex.class)),
                    edgeLabel, vertexInEdgeMap);
        }
    }

    @Override
    public String toString() {
        return "EdgeWriteTask{" +
                "supernodes=" + supernodes +
                ", keepProvidedId=" + keepProvidedId +
                ", providedIdPropertyName='" + providedIdPropertyName + '\'' +
                ", nullValue='" + nullValue + '\'' +
                ", graph=" + graph +
                ", vertexOutEdgeMap=" + vertexOutEdgeMap +
                ", vertexInEdgeMap=" + vertexInEdgeMap +
                ", metadataRow=" + fireflyMetadataRow +
                '}';
    }
}
