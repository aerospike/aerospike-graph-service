package com.aerospike.firefly.bulkloader.spark.executorservice;

import com.aerospike.firefly.bulkloader.spark.resilience.ExponentialBackoffRetry;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class VertexWriteTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(VertexWriteTask.class);
    private final String nullValue;
    private final FireflyGraph graph;
    private final GenericRowWithSchema fireflyRow;
    private final GenericRowWithSchema metadataRow;
    private final int partitionId;
    final ExponentialBackoffRetry retry ;

    public VertexWriteTask(
            com.aerospike.firefly.bulkloader.spark.resilience.ExponentialBackoffRetry retry,
            final String nullValue,
            final FireflyGraph graph,
            final GenericRowWithSchema fireflyRow,
            final int partitionId,
            final GenericRowWithSchema metadataRow) {
        this.retry = retry;
        this.nullValue = nullValue;
        this.graph = graph;
        this.fireflyRow = fireflyRow;
        this.partitionId = partitionId;
        this.metadataRow = metadataRow;
    }


    public CompletableFuture<?> write(ScheduledExecutorService service) {
        return retry.withRetries(
                CompletableFuture.supplyAsync(() -> {
                    SparkFireflyVertex sparkVertex = SparkFireflyVertex.createVertex(this.fireflyRow, this.nullValue);
                    this.graph.bulkWriteVertex(sparkVertex.getFireflyId(this.graph.getBaseGraph()), sparkVertex.getLabel(), sparkVertex.getProperties(),false);
                    return null;
                }, service), service).exceptionally(e -> {
                    LOGGER.error(String.format("Exception occurred in writing vertex %s", this), e);  // Log the error when no longer retrying
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
