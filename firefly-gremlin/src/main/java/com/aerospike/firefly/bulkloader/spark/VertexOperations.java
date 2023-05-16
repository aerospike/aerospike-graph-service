package com.aerospike.firefly.bulkloader.spark;


import com.aerospike.firefly.bulkloader.exception.FireflyBulkLoaderException;
import com.aerospike.firefly.bulkloader.spark.executorservice.VertexWriteTask;
import com.aerospike.firefly.bulkloader.spark.resilience.ExponentialBackoffRetry;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyVertex;
import com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper;
import com.aerospike.firefly.structure.FireflyGraph;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration2.MapConfiguration;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.COLUMNSET_TO_REMOVE;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.DIRECTORY_COLUMN;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.THREAD_POOL_BUFFER_SIZE;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyElement.ID_HEADER;

public class VertexOperations implements Serializable {
    public static final List<String> REQUIRED_VERTEX_HEADERS = List.of(ID_HEADER);
    private static final Logger LOGGER = LoggerFactory.getLogger(VertexOperations.class);
    private final CommandLine cmd;
    private final Map<String, Object> config;
    public List<String> vertexPaths;

    public VertexOperations(CommandLine cmd, Map<String, Object> config, List<String> vertexCSVFiles) {
        this.cmd = Objects.requireNonNull(cmd);
        this.config = Objects.requireNonNull(config);
        this.vertexPaths = Objects.requireNonNull(vertexCSVFiles);
    }

    public static String getVertexDirectory(Map<String, Object> configMap) {
        return BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.VERTEX_DIRECTORY_KEY, configMap);
    }

    public void dryRunVertices(Dataset<Row> vertexDataSet) {
        if (cmd.hasOption("dryrunvertex")) {
            if (!dryRunVertexRows(vertexDataSet)) {
                final String preflightFailed = "Detected invalid CSV data in VERTICES pre-flight check. See logs for detail on which line number and file caused the failure.";
                throw new FireflyBulkLoaderException(preflightFailed);
            }
        }
    }

    private boolean dryRunVertexRows(final Dataset<Row> vertices) {
        final String nullValue = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE, config);
        final List<Integer> failures = vertices.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
            final AtomicInteger failureCount = new AtomicInteger(0);
            while (rowIterator.hasNext()) {
                final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNSET_TO_REMOVE);
                try {
                    SparkFireflyVertex.createVertex(fireflyRow, nullValue);
                } catch (FireflyBulkLoaderException e) {
                    LOGGER.error("Format validation of CSV data failed on line '{}' of file {}.",
                            metadataRow.get(metadataRow.fieldIndex(DatasetOperations.LINENUMBER_COLUMN)), metadataRow.get(metadataRow.fieldIndex(DIRECTORY_COLUMN)));
                    failureCount.incrementAndGet();
                }
            }
            return Collections.singletonList(failureCount.get()).iterator();
        }, Encoders.INT()).collectAsList();
        for (final int failure : failures) {
            if (failure != 0) {
                return false;
            }
        }
        return true;
    }

    private void writeVertices(final Dataset<Row> unionVertexDS) {
        unionVertexDS.foreachPartition( rowIterator -> {
            LOGGER.info("PartitionId in VertexDataset = " + TaskContext.getPartitionId());
//            final boolean ignoreElementCreationFailed =
//                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.IGNORE_ELEMENT_CREATION_FAILED, config));
            final String nullValue = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE, config);
            try (final FireflyGraph graph = FireflyGraph.open(new MapConfiguration(config))) {
                ExponentialBackoffRetry retry = new ExponentialBackoffRetry(Optional.of("vertex-write-partitionid-"+ TaskContext.getPartitionId()));
                ThreadFactory edgeThreadFactory =
                        new ThreadFactoryBuilder().setNameFormat("vertex-write-thread-for-partition-id-" + TaskContext.getPartitionId()).setDaemon(true).build();
                final ScheduledExecutorService ses = new ScheduledThreadPoolExecutor(THREAD_POOL_BUFFER_SIZE, edgeThreadFactory);

                int bufferSize = getVertexWriteBufferSize(config);
                LOGGER.info(String.format("vertex write buffer size %d", bufferSize));

                Instant start = Instant.now();
                int batch = 1;
                int partitionId = TaskContext.getPartitionId();
                final List<Future<?>> futures = new ArrayList<>();
                while (rowIterator.hasNext()) {
                    if (futures.size() >= bufferSize) {
                        CompletableFuture megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                        megaTask.join();
                        final Instant end = Instant.now();
                        LOGGER.info(String.format("vertex write, partitionId=%d, batch= %d, time taken(in milli-seconds)= %d", partitionId,
                                batch, Duration.between(start, end).toMillis()));
                        start = Instant.now();
                        batch = batch + 1;
                        if (megaTask.isCompletedExceptionally()) {
                            throw new RuntimeException("Error occurred while writing vertices, see logs for more details");
                        }
                        futures.clear();
                    }
                    final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                    final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNSET_TO_REMOVE);
                    final VertexWriteTask vwt = new VertexWriteTask(retry, nullValue, graph, fireflyRow, TaskContext.getPartitionId(), metadataRow);
                    futures.add(vwt.write(ses));
                }

                LOGGER.info(String.format("done submitting vertex write task, waiting for their completion in partition %d", TaskContext.getPartitionId()));
                CompletableFuture megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                megaTask.join();
                futures.clear();
                ses.shutdown();
            }
        });
    }

    private void verifyVertices(final Dataset<Row> sampledVertexDatasets) {
        sampledVertexDatasets.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
            final String nullValue = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE, config);

            try (final FireflyGraph graph = FireflyGraph.open(new MapConfiguration(config))) {
                final GraphTraversalSource g = graph.traversal();
                while (rowIterator.hasNext()) {
                    final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                    final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNSET_TO_REMOVE);
                    final SparkFireflyVertex sparkVertex =
                            SparkFireflyVertex.createVertex(fireflyRow, nullValue);

                    final Object id = sparkVertex.getId();
                    final GraphTraversal<Vertex, Vertex> vertexById = g.V(id);
                    Vertex v = vertexById.next();
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
            }
            return Collections.singletonList(1).iterator();
        }, Encoders.INT()).write().format("noop").mode(SaveMode.Append).save();
    }

    public void verifySampleVerticesAfterWrite(Dataset<Row> sampledVertexDataset) {
        if (cmd.hasOption("verifyvertex")) {
            sampledVertexDataset.sparkSession().sparkContext().setJobGroup("Verify Vertex", "Verify vertex task", true);
            verifyVertices(sampledVertexDataset);
        }
    }

    public void writeVerticesToDB(Dataset<Row> vertexDataSet) {
        if (cmd.hasOption("writevertex")) {
            final Instant startOfVertexWrite = Instant.now();
            vertexDataSet.sparkSession().sparkContext().setJobGroup("Vertex write", "Vertex write task", true);
            writeVertices(vertexDataSet);
            final Instant endOfVertexWrite = Instant.now();
            Duration vertexInterval = Duration.between(startOfVertexWrite, endOfVertexWrite);
            LOGGER.info("Execution time in seconds for vertex write task: " + vertexInterval.getSeconds());
        }

    }

    private int getVertexWriteBufferSize(Map<String, Object> conf) {
        return  Integer.parseInt(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.VERTEX_WRITE_BUFFER, conf));
    }

}
