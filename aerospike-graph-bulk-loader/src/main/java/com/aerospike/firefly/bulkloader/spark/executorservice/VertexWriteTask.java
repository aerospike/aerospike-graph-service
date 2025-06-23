package com.aerospike.firefly.bulkloader.spark.executorservice;

import com.aerospike.firefly.bulkloader.spark.resilience.ExponentialBackoffRetry;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

public class VertexWriteTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(VertexWriteTask.class);
    private final String nullValue;
    private final FireflyGraph graph;
    private final GenericRowWithSchema fireflyRow;
    private final GenericRowWithSchema metadataRow;
    private final ExponentialBackoffRetry retry;
    private final Set<Object> supernodes;
    final boolean edgeCacheEnabled;
    final SparkFireflyVertex sparkVertex;
    final FireflyId fireflyId;
    final int partitionId;
    final Optional<Map<String, List<FireflyId>>> toEdgeCache;
    final Optional<Map<String, List<FireflyId>>> fromEdgeCache;

    public VertexWriteTask(
            final ExponentialBackoffRetry retry,
            final String nullValue,
            final FireflyGraph graph,
            final GenericRowWithSchema fireflyRow,
            final GenericRowWithSchema metadataRow,
            final Set<Object> supernodes,
            final int partitionId) {
        this.retry = retry;
        this.nullValue = nullValue;
        this.graph = graph;
        this.fireflyRow = fireflyRow;
        this.metadataRow = metadataRow;
        this.supernodes = supernodes;
        this.edgeCacheEnabled = this.graph.getBaseGraph().GLOBAL_EDGE_CACHE_ENABLED_FLAG;
        sparkVertex = SparkFireflyVertex.createVertex(this.fireflyRow, this.nullValue);
        fireflyId = sparkVertex.getFireflyId(this.graph.getBaseGraph());
        this.partitionId = partitionId;
        this.toEdgeCache = sparkVertex.getToEdgeCache(this.graph.getBaseGraph());
        this.fromEdgeCache = sparkVertex.getFromEdgeCache(this.graph.getBaseGraph());
    }

    public CompletionStage<Void> writeIncremental(final ScheduledExecutorService service) {
        final Supplier<CompletionStage<Void>> supplier = () -> CompletableFuture.supplyAsync(() -> {
            graph.bulkWriteMergeVertex(sparkVertex.getId(),
                    sparkVertex.getLabel(),
                    sparkVertex.getProperties(),
                    partitionId,
                    sparkVertex.getCardinalities(),
                    isSupernode());
            return null;
        }, service);
        return retry.withRetries(supplier, service).exceptionally(e -> {
            // Log the error when no longer retrying
            LOGGER.error(String.format("Exception occurred during Vertex writing %s", this), e);
            throw new RuntimeException(e);
        });
    }

    public CompletionStage<Void> write(final ScheduledExecutorService service) {
        final Supplier<CompletionStage<Void>> supplier = () -> CompletableFuture.supplyAsync(() -> {
            this.graph.bulkWriteVertex(fireflyId, sparkVertex.getLabel(),
                    sparkVertex.getProperties(), isSupernode(), partitionId, toEdgeCache, fromEdgeCache);
            return null;
        }, service);
        return retry.withRetries(supplier, service).exceptionally(e -> {
            // Log the error when no longer retrying
            LOGGER.error(String.format("Exception occurred during Vertex writing %s", this), e);
            throw new RuntimeException(e);
        });
    }

    private boolean isSupernode() {
        return !edgeCacheEnabled || supernodes.contains(sparkVertex.getId());
    }

    @Override
    public String toString() {
        return "VertexWriteTask{" +
                "nullValue='" + nullValue + '\'' +
                ", graph=" + graph +
                ", fireflyRow=" + fireflyRow +
                ", metadataRow=" + metadataRow +
                '}';
    }
}
