package com.aerospike.firefly.bulkloader.spark.executorservice;

import com.aerospike.client.Value;
import com.aerospike.firefly.bulkloader.graph.GraphOperations;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class EdgeWriteTask {
    final ExponentialBackoffRetry retry;
    private final Logger LOGGER = LoggerFactory.getLogger(EdgeWriteTask.class);
    private final Set<Object> supernodes;
    private final boolean keepProvidedId;
    private final String providedIdPropertyName;
    private final String nullValue;
    private final FireflyGraph graph;
    private final AtomicInteger outEdgeCount = new AtomicInteger(0);
    private final AtomicInteger inEdgeCount = new AtomicInteger(0);
    private final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexOutEdgeMap;
    private final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexInEdgeMap;
    private final GenericRowWithSchema fireflyRow;
    private final GenericRowWithSchema fireflyMetadataRow;
    private final int partitionId;


    public EdgeWriteTask(
            ExponentialBackoffRetry retry,
            final Set<Object> supernodes,
            final boolean keepProvidedId,
            final String providedIdPropertyName,
            final String nullValue, final FireflyGraph graph,
            final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexOutEdgeMap,
            final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexInEdgeMap,
            final GenericRowWithSchema rowForFirefly,
            final int partitionId,
            final GenericRowWithSchema fireflyMetadataRow) {
        this.retry = retry;
        this.supernodes = supernodes;
        this.keepProvidedId = keepProvidedId;
        this.providedIdPropertyName = providedIdPropertyName;
        this.nullValue = nullValue;
        this.graph = graph;
        this.vertexOutEdgeMap = vertexOutEdgeMap;
        this.vertexInEdgeMap = vertexInEdgeMap;
        this.fireflyRow = rowForFirefly;
        this.partitionId = partitionId;
        this.fireflyMetadataRow = fireflyMetadataRow;
    }

    public CompletionStage<Void> write(ScheduledExecutorService service) {
        final boolean edgeCacheEnabled = this.graph.getBaseGraph().GLOBAL_EDGE_CACHE_ENABLED_FLAG;
        final Supplier<CompletionStage<Void>> supplier = () -> CompletableFuture.supplyAsync(() -> {
            SparkFireflyEdge sparkEdge = SparkFireflyEdge.createEdge(this.fireflyRow, this.keepProvidedId,
                    this.providedIdPropertyName, this.nullValue, this.graph, false);
            final FireflyId edgeId = sparkEdge.getFireflyId(this.graph.getBaseGraph());
            final Object inVertexId = sparkEdge.getInVertexId();
            final Object outVertexId = sparkEdge.getOutVertexId();
            final String edgeLabel = sparkEdge.getLabel();
            // If the edge cache is not enabled, then every edge must be written as if it were attached to a supernode.
            final boolean inVertexSupernode = !edgeCacheEnabled || supernodes.contains(inVertexId);
            final boolean outVertexSupernode = !edgeCacheEnabled || supernodes.contains(outVertexId);
            this.graph.bulkWriteEdge((byte[]) sparkEdge.getId(), edgeLabel, sparkEdge.getProperties(),
                    inVertexId, outVertexId, inVertexSupernode, outVertexSupernode);
            if (edgeCacheEnabled) {
                GraphOperations.updateEdgeMap(this.supernodes, outVertexId,
                        this.graph.getIdFactory().createCompositeEdgeId(edgeId, graph.getIdFactory().createId(inVertexId, FireflyVertex.class)),
                        edgeLabel, this.vertexOutEdgeMap);
                GraphOperations.updateEdgeMap(this.supernodes, inVertexId,
                        this.graph.getIdFactory().createCompositeEdgeId(edgeId, this.graph.getIdFactory().createId(outVertexId, FireflyVertex.class)),
                        edgeLabel, this.vertexInEdgeMap);
            }
            return null;
        }, service);
        return retry.withRetries(supplier, service).exceptionally(e -> {
            // Log the error when no longer retrying
            LOGGER.error(String.format("Exception occurred during Edge writing %s", this), e);
            throw new RuntimeException(e);
        });
    }

    @Override
    public String toString() {
        return "EdgeWriteTask{" +
                "supernodes=" + supernodes +
                ", keepProvidedId=" + keepProvidedId +
                ", providedIdPropertyName='" + providedIdPropertyName + '\'' +
                ", nullValue='" + nullValue + '\'' +
                ", graph=" + graph +
                ", outEdgeCount=" + outEdgeCount +
                ", inEdgeCount=" + inEdgeCount +
                ", vertexOutEdgeMap=" + vertexOutEdgeMap +
                ", vertexInEdgeMap=" + vertexInEdgeMap +
                ", fireflyRow=" + fireflyRow +
                ", fireflyMetadataRow=" + fireflyMetadataRow +
                ", partitionId=" + partitionId +
                '}';
    }
}
