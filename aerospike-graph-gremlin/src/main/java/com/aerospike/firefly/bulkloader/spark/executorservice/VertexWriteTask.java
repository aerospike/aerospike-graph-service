package com.aerospike.firefly.bulkloader.spark.executorservice;

import com.aerospike.firefly.bulkloader.spark.resilience.ExponentialBackoffRetry;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final int partitionId;
    private final ExponentialBackoffRetry retry;
    private final Set<Object> supernodes;

    public VertexWriteTask(
            com.aerospike.firefly.bulkloader.spark.resilience.ExponentialBackoffRetry retry,
            final String nullValue,
            final FireflyGraph graph,
            final GenericRowWithSchema fireflyRow,
            final int partitionId,
            final GenericRowWithSchema metadataRow,
            final Set<Object> supernodes) {
        this.retry = retry;
        this.nullValue = nullValue;
        this.graph = graph;
        this.fireflyRow = fireflyRow;
        this.partitionId = partitionId;
        this.metadataRow = metadataRow;
        this.supernodes = supernodes;
    }

    public CompletionStage<Void> write(final ScheduledExecutorService service) {
        final boolean edgeCacheEnabled = this.graph.getBaseGraph().GLOBAL_EDGE_CACHE_ENABLED_FLAG;
        final Supplier<CompletionStage<Void>> supplier = () -> CompletableFuture.supplyAsync(() -> {
            final SparkFireflyVertex sparkVertex = SparkFireflyVertex.createVertex(this.fireflyRow, this.nullValue);
            this.graph.bulkWriteVertex(sparkVertex.getFireflyId(this.graph.getBaseGraph()), sparkVertex.getLabel(),
                    sparkVertex.getProperties(), !edgeCacheEnabled || supernodes.contains(sparkVertex.getId()));
            return null;
        }, service);
        return retry.withRetries(supplier, service).exceptionally(e -> {
            // Log the error when no longer retrying
            LOGGER.error(String.format("Exception occurred during Vertex writing %s", this), e);
            throw new RuntimeException(e);
        });
    }

    @Override
    public String toString() {
        return "VertexWriteTask{" +
                "nullValue='" + nullValue + '\'' +
                ", graph=" + graph +
                ", fireflyRow=" + fireflyRow +
                ", metadataRow=" + metadataRow +
                ", partitionId=" + partitionId +
                '}';
    }
}
