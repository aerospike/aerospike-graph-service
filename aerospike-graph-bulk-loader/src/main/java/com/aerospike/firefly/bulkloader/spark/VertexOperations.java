package com.aerospike.firefly.bulkloader.spark;

import com.aerospike.firefly.bulkloader.spark.executorservice.VertexWriteTask;
import com.aerospike.firefly.bulkloader.spark.resilience.ExponentialBackoffRetry;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyVertex;
import com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper;
import com.aerospike.firefly.structure.FireflyGraph;
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

import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.COLUMNS_TO_REMOVE;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.processBatch;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyElement.ID_HEADER;
import static com.aerospike.firefly.process.call.bulkload.utils.CommandLineParser.VERIFY_VERTEX;
import static com.aerospike.firefly.process.call.bulkload.utils.CommandLineParser.WRITE_VERTEX;

public class VertexOperations implements Serializable {
    public static final List<String> REQUIRED_VERTEX_HEADERS = List.of(ID_HEADER);
    private static final Logger LOGGER = LoggerFactory.getLogger(VertexOperations.class);
    private final BulkLoaderConfigHelper config;
    public List<String> vertexPaths;

    public VertexOperations(final BulkLoaderConfigHelper config, final List<String> vertexCSVFiles) {
        this.config = Objects.requireNonNull(config);
        this.vertexPaths = Objects.requireNonNull(vertexCSVFiles);
    }

    private void writeVertices(final Dataset<Row> unionVertexDS, final Set<Object> supernodes) {
        unionVertexDS.foreachPartition(rowIterator -> {
            LOGGER.info("PartitionId in VertexDataset = " + TaskContext.getPartitionId());
            final String nullValue = this.config.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE);
            try (final FireflyGraph graph = FireflyGraph.open(config.getFireflyConfig())) {
                ExponentialBackoffRetry retry = new ExponentialBackoffRetry("vertex-write-partitionid-"+ TaskContext.getPartitionId());
                final ScheduledExecutorService executor = DatasetOperations.getScheduledThreadPoolService();
                int bufferSize = getVertexWriteBufferSize();
                LOGGER.info(String.format("vertex write buffer size %d", bufferSize));

                Instant start = Instant.now();
                int batch = 1;
                int partitionId = TaskContext.getPartitionId();
                final List<CompletionStage<Void>> futures = new ArrayList<>();
                while (rowIterator.hasNext()) {
                    if (futures.size() >= bufferSize) {
                        CompletableFuture<Void> megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                        megaTask.join();
                        final Instant end = Instant.now();
                        LOGGER.info(String.format("Vertex write, partitionId=%d, batch=%d, time taken (in milliseconds)=%d", partitionId,
                                batch, Duration.between(start, end).toMillis()));
                        start = Instant.now();
                        batch = batch + 1;
                        if (megaTask.isCompletedExceptionally()) {
                            throw new RuntimeException("Error occurred while writing Vertices; see logs for more details.");
                        }
                        futures.clear();
                    }
                    final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                    final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNS_TO_REMOVE);
                    final VertexWriteTask vwt = new VertexWriteTask(retry, nullValue, graph, fireflyRow, TaskContext.getPartitionId(), metadataRow, supernodes);
                    futures.add(vwt.write(executor));
                }

                LOGGER.info(String.format("Done submitting Vertex write task; waiting for their completion in partition %d", TaskContext.getPartitionId()));
                final CompletableFuture megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                megaTask.join();
                futures.clear();
            }
        });
    }

    private void verifyVertices(final Dataset<Row> sampledVertexDatasets) {
        final String errMessage= "Error occurred while verifying vertices; see logs for more details.";
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

    private static void verifyVertexRow(String nullValue, GraphTraversalSource g, GenericRowWithSchema row) {
        final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(row, COLUMNS_TO_REMOVE);
        final SparkFireflyVertex sparkVertex = SparkFireflyVertex.createVertex(fireflyRow, nullValue);

        final Object id = sparkVertex.getId();
        final GraphTraversal<Vertex, Vertex> vertexById = g.V(id);
        final Vertex v = vertexById.next();
        if (vertexById.hasNext()) {
            throw new AssertionError("Validation failed: More than one vertex with ID " + id
                    + " exists");
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
        if (this.config.hasAction(VERIFY_VERTEX)) {
            String taskName = "Verify Vertex";
            sampledVertexDataset.sparkSession().sparkContext().setJobGroup(taskName, "Verify Vertex task", true);
            verifyVertices(sampledVertexDataset);
            sampledVertexDataset.sparkSession().sparkContext().cancelJobGroup(taskName);
        }
    }

    public void writeVerticesToDB(final Dataset<Row> vertexDataSet, final Set<Object> supernodes) {
        if (this.config.hasAction(WRITE_VERTEX)) {
            final Instant startOfVertexWrite = Instant.now();
            String taskName = "Vertex write";
            vertexDataSet.sparkSession().sparkContext().setJobGroup(taskName, "Vertex write task", true);
            writeVertices(vertexDataSet, supernodes);
            vertexDataSet.sparkSession().sparkContext().cancelJobGroup(taskName);
            final Instant endOfVertexWrite = Instant.now();
            Duration vertexInterval = Duration.between(startOfVertexWrite, endOfVertexWrite);
            LOGGER.info("Execution time in seconds for vertex write task: " + vertexInterval.getSeconds());
        }
    }

    private int getVertexWriteBufferSize() {
        return Integer.parseInt(this.config.getOrDefault(BulkLoaderConfigHelper.VERTEX_WRITE_BUFFER).trim());
    }
}
