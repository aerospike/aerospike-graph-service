package com.aerospike.firefly.bulkloader.spark;

import com.aerospike.firefly.bulkloader.spark.executorservice.VertexWriteTask;
import com.aerospike.firefly.bulkloader.spark.resilience.ExponentialBackoffRetry;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyVertex;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper;
import com.aerospike.firefly.process.call.bulkload.utils.exception.BadCsvEntryException;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static com.aerospike.firefly.bulkloader.SparkBulkLoaderMain.exponentialBackoff;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.COLUMNS_TO_REMOVE;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.RETRY_LIMIT;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.processBatch;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyElement.ID_HEADER;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ALLOWED_BAD_ENTRY_COUNT;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ALLOWED_DUPLICATE_VERTEX_ID_COUNT;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.DISABLE_VERTEX_WRITE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.RECOVERY_FAILURE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.VERIFY_OUTPUT_DATA;
import static com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyLoadingException.isRetryable;

public class VertexOperations implements Serializable {
    public static final List<String> REQUIRED_VERTEX_HEADERS = List.of(ID_HEADER);
    private static final Logger LOGGER = LoggerFactory.getLogger(VertexOperations.class);
    private final BulkLoaderConfigHelper config;
    public List<String> vertexPaths;

    public VertexOperations(final BulkLoaderConfigHelper config, final List<String> vertexCSVFiles) {
        this.config = Objects.requireNonNull(config);
        this.vertexPaths = Objects.requireNonNull(vertexCSVFiles);
    }

    private void writeVertices(final Dataset<Row> unionVertexDS,
                               final Set<Object> supernodes,
                               final Set<Long> completedVertexPartitions,
                               final boolean readOnly) {
        unionVertexDS.foreachPartition(rowIterator -> {
            final int partitionId = TaskContext.getPartitionId();
            if (!readOnly) {
                final Long partitionIdLong = Long.valueOf(partitionId);
                if (completedVertexPartitions.contains(partitionIdLong)) {
                    LOGGER.info("Vertices PartitionId " + partitionId + " is already written, skipping.");
                    return;
                }
            }

            // This is a testing config, used to force failure in specific spots to allow us to test the recovery modes.
            final String recoveryFailure = config.getOrDefault(RECOVERY_FAILURE);
            if (recoveryFailure != null && recoveryFailure.startsWith("VERTEX_WRITE")) {
                // The failure position is supplied as "VERTEX_WRITE:<partition_id>".
                final int partitionToFailOn = recoveryFailure.split(":", 2).length > 1 ? Integer.parseInt(recoveryFailure.split(":", 2)[1]) : -1;
                if (partitionToFailOn == -1) {
                    // If a specific partition was not supplied, fail instantly.
                    throw new RuntimeException("Failed to get partition to fail on from recovery failure property, please contact support.");
                } else {
                    // Otherwise fail on the specific partition a few mins later to allow other partitions to complete.
                    if (partitionId == partitionToFailOn) {
                        // Wait so other partitions can complete before we fail this partition.
                        try {
                            Thread.sleep(180000);
                        } catch (final InterruptedException ignored) {
                        }
                        throw new RuntimeException("Testing recovery failure, please contact support.");
                    }
                }
            }

            LOGGER.info("Starting to write VertexDataset in PartitionId: " + partitionId);
            try (final FireflyGraph graph = FireflyGraph.open(config.getFireflyConfig())) {
                graph.fireflySummaryUpdater.startVertexPartition(partitionId);
                graph.fireflySummaryUpdater.startMergeVertexPartition(partitionId);
                graph.fireflySummaryUpdater.startSupernodePartition(partitionId);
                final String nullValue = this.config.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE);
                final long allowBadEntryCount = this.config.getOrDefaultInt(ALLOWED_BAD_ENTRY_COUNT);
                final ScheduledExecutorService executor = DatasetOperations.getScheduledThreadPoolService();
                final ExponentialBackoffRetry retry = new ExponentialBackoffRetry("vertex-write-partitionid-" + partitionId);
                final int bufferSize = getVertexWriteBufferSize();
                LOGGER.info(String.format("Vertex write buffer size %d", bufferSize));

                final Instant totalStart = Instant.now();
                Instant start = Instant.now();
                int batch = 1;
                final List<CompletionStage<Void>> futures = new ArrayList<>();
                while (rowIterator.hasNext()) {
                    if (futures.size() >= bufferSize) {
                        final CompletableFuture megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                        megaTask.join();
                        LOGGER.info(String.format("Vertex write, partitionId: %d, batch: %d, time taken(in milli-seconds): %d", partitionId,
                                batch, Duration.between(start, Instant.now()).toMillis()));
                        start = Instant.now();
                        batch = batch + 1;
                        if (megaTask.isCompletedExceptionally()) {
                            throw new RuntimeException("Error occurred while writing vertices - see logs for more details");
                        }
                        futures.clear();
                    }
                    final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next().copy();
                    final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNS_TO_REMOVE);
                    try {
                        final VertexWriteTask vwt = new VertexWriteTask(retry, nullValue, graph, fireflyRow, metadataRow, supernodes, partitionId);
                        if (config.hasAction(BulkLoaderConfigHelper.INCREMENTAL_LOAD)) {
                            futures.add(vwt.writeIncremental(executor));
                        } else {
                            futures.add(vwt.write(executor));
                        }
                    } catch (final BadCsvEntryException e) {
                        if (allowBadEntryCount == 0) {
                            throw e;
                        }
                    }
                }

                LOGGER.info(String.format("Done submitting vertex write task; waiting for their completion in partitionId %d", partitionId));
                final CompletableFuture megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                megaTask.join();
                LOGGER.info(String.format("Completed vertex write task in partitionId %d", partitionId));
                futures.clear();
                if (megaTask.isCompletedExceptionally()) {
                    throw new RuntimeException("Error occurred while writing vertices - see logs for more details");
                }

                final String taskName = String.format("Vertex write in partition:{}", partitionId);
                if (!readOnly) {
                    LOGGER.info("Writing vertex partition complete for partitionId: {}", partitionId);
                    RecoveryUtil.writeVertexPartitionComplete(graph.getBaseGraph(), partitionId);
                }
                graph.fireflySummaryUpdater.completeVertexPartition(partitionId);
                graph.fireflySummaryUpdater.completeSupernodePartition(partitionId);
                LOGGER.info("Task:{}; Total time taken(in milliseconds):{}", taskName, Duration.between(totalStart, Instant.now()).toMillis());
            }
        });
    }

    private void verifyVertices(final Dataset<Row> sampledVertexDatasets) {
        final String errMessage = "Error occurred while verifying vertices; see logs for more details.";
        sampledVertexDatasets.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
            final String nullValue = this.config.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE);

            try (final FireflyGraph graph = FireflyGraph.open(this.config.getFireflyConfig())) {
                final GraphTraversalSource g = graph.traversal();
                int bufferSize = getVertexWriteBufferSize();

                int batch = 1;
                final List<Future<?>> futures = new ArrayList<>();
                final ScheduledExecutorService executor = DatasetOperations.getScheduledThreadPoolService();
                while (rowIterator.hasNext()) {
                    batch = processBatch(bufferSize, batch, futures, errMessage);
                    final GenericRowWithSchema row = (GenericRowWithSchema) rowIterator.next();
                    futures.add(CompletableFuture.supplyAsync(() -> {
                        verifyVertexRow(nullValue, g, row);
                        return null;
                    }, executor).exceptionally(e -> {
                        LOGGER.error("Exception occurred in verifying Vertex row", e);  // Log the error when final failure happens
                        throw new RuntimeException(e);
                    }));
                }
                final CompletableFuture megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                megaTask.join();
                if (megaTask.isCompletedExceptionally()) {
                    throw new RuntimeException("Error occurred while verifying Vertices; see logs for more details.");
                }
                futures.clear();
            }
            return Collections.singletonList(1).iterator();
        }, Encoders.INT()).write().format("noop").mode(SaveMode.Append).save();
    }

    private static void verifyVertexRow(final String nullValue, final GraphTraversalSource g,
                                        final GenericRowWithSchema row) {
        final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(row, COLUMNS_TO_REMOVE);
        final SparkFireflyVertex sparkVertex = SparkFireflyVertex.createVertex(fireflyRow, nullValue);

        final Object id = sparkVertex.getId();
        Vertex v = null;
        int tryCount = 0;
        boolean successful = false;
        while (!successful) {
            try {
                final GraphTraversal<Vertex, Vertex> vertexById = g.V(id);
                v = vertexById.next();
                if (vertexById.hasNext()) {
                    throw new AssertionError("Validation failed: More than one vertex with ID " + id
                            + " exists");
                }
                successful = true;
            } catch (final AerospikeGraphException e) {
                if (!isRetryable(e)) {
                    LOGGER.error("Failed to verify loaded Vertex due to non-retryable error: " + row, e);
                    throw e;
                } else if (++tryCount > RETRY_LIMIT) {
                    LOGGER.error("Failed to verify loaded Vertex after " + tryCount + " attempts: " + row, e);
                    throw e;
                } else {
                    LOGGER.warn("Failed to verify loaded Edge: " + row + ". Attempt count: " + tryCount, e);
                    exponentialBackoff(tryCount);
                }
            }
        }
        if (!v.label().equals(sparkVertex.getLabel())) {
            throw new AssertionError("Validation failed: Label did not match for vertex with ID " + id);
        }
        final List<Map.Entry<String, Object>> sparkVertexProperties = sparkVertex.getProperties();
        for (final Map.Entry<String, Object> property : sparkVertexProperties) {
            try {
                // TODO: Handle null (when supported in Firefly) and cardinality.
                boolean isList = property.getValue() instanceof List<?>;
                if (isList) {
                    final List<Object> propertyValues = new LinkedList<>((List<Object>) property.getValue());
                    for (final Object vertexPropertyValue : (List<Object>) v.value(property.getKey())) {
                        propertyValues.remove(vertexPropertyValue);
                    }
                    if (!propertyValues.isEmpty()) {
                        throw new AssertionError("Validation failed: Property key "
                                + property.getKey() + " on vertex with ID " + id
                                + " did not match value " + property.getValue());
                    }
                } else {
                    if (property.getValue() != null) {
                        final Object vertexPropertyValue = v.value(property.getKey());
                        if (!property.getValue().equals(vertexPropertyValue)) {
                            throw new AssertionError("Validation failed: Property key "
                                    + property.getKey() + " on vertex with ID " + id
                                    + " did not match value " + property.getValue());
                        }
                    }
                }
            } catch (final AssertionError ae) {
                throw ae;
            } catch (final Exception e) {
                throw new AssertionError("Validation failed: Property key " + property.getKey() +
                        " on vertex with ID " + id + " did not match value " + property.getValue(), e);
            }
        }
    }

    public void verifySampleVerticesAfterWrite(final Dataset<Row> sampledVertexDataset) {
        if (this.config.hasAction(VERIFY_OUTPUT_DATA) && !this.config.hasAction(DISABLE_VERTEX_WRITE)) {
            final long allowDuplicateVertexIds = this.config.getOrDefaultInt(ALLOWED_DUPLICATE_VERTEX_ID_COUNT);
            if (allowDuplicateVertexIds > 0) {
                LOGGER.warn(ALLOWED_DUPLICATE_VERTEX_ID_COUNT + " is set to a value greater than 0. Vertex verification cannot be performed and will be skipped.");
                return;
            }
            final long allowBadEntries = this.config.getOrDefaultInt(ALLOWED_BAD_ENTRY_COUNT);
            if (allowBadEntries > 0) {
                LOGGER.warn(ALLOWED_BAD_ENTRY_COUNT + " is set to a value greater than 0. Vertex verification cannot be performed and will be skipped.");
                return;
            }
            String taskName = "Verify Vertex";
            sampledVertexDataset.sparkSession().sparkContext().setJobGroup(taskName, "Verify Vertex task", true);
            verifyVertices(sampledVertexDataset);
            sampledVertexDataset.sparkSession().sparkContext().cancelJobGroup(taskName);
        }
    }

    public void writeVerticesToDB(final Dataset<Row> vertexDataSet,
                                  final Set<Object> supernodes,
                                  final Set<Long> completedVertexPartitions,
                                  final boolean readOnly) {
        if (!this.config.hasAction(DISABLE_VERTEX_WRITE)) {
            final Instant startOfVertexWrite = Instant.now();
            String taskName = "Vertex write";
            vertexDataSet.sparkSession().sparkContext().setJobGroup(taskName, "Vertex write task", true);
            writeVertices(vertexDataSet, supernodes, completedVertexPartitions, readOnly);
            vertexDataSet.sparkSession().sparkContext().cancelJobGroup(taskName);
            final Instant endOfVertexWrite = Instant.now();
            Duration vertexInterval = Duration.between(startOfVertexWrite, endOfVertexWrite);
            LOGGER.info("Execution time in seconds for vertex write task: " + vertexInterval.getSeconds());
        }
    }

    private int getVertexWriteBufferSize() {
        return this.config.getOrDefaultInt(BulkLoaderConfigHelper.VERTEX_WRITE_BUFFER);
    }
}
