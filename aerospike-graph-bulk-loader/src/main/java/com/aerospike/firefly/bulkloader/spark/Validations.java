package com.aerospike.firefly.bulkloader.spark;

import com.aerospike.client.AerospikeException;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyElement;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyVertex;
import com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper;
import com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyBulkLoaderException;
import com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyBulkLoaderPreflightException;
import com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyLoadingException;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.aerospike.firefly.bulkloader.SparkBulkLoaderMain.exponentialBackoff;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.COLUMNS_TO_REMOVE;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.FILENAME_COLUMN;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.RETRY_LIMIT;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.BAD_ENTRY_COUNT_EXCEEDED;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.DUPLICATE_VERTEX_ID_COUNT_EXCEEDED;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ALLOWED_BAD_ENTRY_COUNT;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ALLOWED_DUPLICATE_VERTEX_ID_COUNT;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.EDGE_WRITE_BUFFER;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.INCREMENTAL_LOAD;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.NULL_VALUE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.VERTEX_WRITE_BUFFER;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.count;

public class Validations {
    private static final Logger LOGGER = LoggerFactory.getLogger(Validations.class);

    private static long dryRunEdgeCreation(final Dataset<Row> edgeDataset, final BulkLoaderConfigHelper config) {
        final boolean allowBadEntries = config.getOrDefaultInt(ALLOWED_BAD_ENTRY_COUNT) > 0;

        final List<Long> failures = edgeDataset.mapPartitions((MapPartitionsFunction<Row, Long>) rowIterator -> {
            final boolean keepProvidedId =
                    config.getOrDefaultBool(KEEP_PROVIDED_EDGE_ID_AS_PROPERTY);
            final String providedIdPropertyName = config.getOrDefault(PROVIDED_EDGE_ID_PROPERTY_NAME);
            final String nullValue = config.getOrDefault(NULL_VALUE);
            long failureCount = 0L;
            final int bufferSize = config.getOrDefaultInt(EDGE_WRITE_BUFFER);
            final List<CompletableFuture<Integer>> futures = new ArrayList<>();

            FireflyGraph graph = null;
            if (allowBadEntries) {
                graph = FireflyGraph.open(config.getFireflyConfig());
            }
            try {
                while (rowIterator.hasNext()) {
                    if (futures.size() >= bufferSize) {
                        final CompletableFuture<Void> megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                        megaTask.join();
                        failureCount += countFailures(futures);
                        futures.clear();
                    }
                    futures.add(asyncDryRunEdgeCreation(rowIterator.next(), keepProvidedId, providedIdPropertyName, nullValue, graph));
                }

                final CompletableFuture megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                megaTask.join();
                failureCount += countFailures(futures);
                futures.clear();
                return Collections.singletonList(failureCount).iterator();
            } finally {
                if (graph != null) {
                    graph.close();
                }
            }
        }, Encoders.LONG()).collectAsList();

        long badElementCount = 0;
        for (final Long l : failures) {
            badElementCount += l;
        }
        return badElementCount;
    }

    private static int countFailures(final List<CompletableFuture<Integer>> futures) {
        try {
            return futures.stream().reduce(CompletableFuture.completedFuture(0),
                    (future1, future2) -> future1.thenCompose(
                            v1 -> future2.thenApply(v2 -> v1 + v2)
                    )).get();
        } catch (final InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static CompletableFuture<Integer> asyncDryRunEdgeCreation(final Row row, final boolean keepProvidedId,
                                                                      final String providedIdPropertyName,
                                                                      final String nullValue,
                                                                      final FireflyGraph graph) {
        return CompletableFuture.supplyAsync(() -> {
            final GenericRowWithSchema metadataRow = (GenericRowWithSchema) row;
            final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNS_TO_REMOVE);
            try {
                SparkFireflyEdge.createEdge(fireflyRow, keepProvidedId, providedIdPropertyName, nullValue, null, true, null);
                return 0;
            } catch (final FireflyBulkLoaderException e) {
                final Set<String> columnToRemove = Set.of(FILENAME_COLUMN);
                final String filteredRow = DatasetOperations.removeColumns(metadataRow, columnToRemove).toString();
                final String fileName = metadataRow.get(metadataRow.fieldIndex(FILENAME_COLUMN)).toString();
                LOGGER.error("Edge Format validation of CSV edge data failed on row {} of file {}.",
                        filteredRow, fileName);
                if (graph != null) {
                    graph.writeBadEntry(filteredRow, fileName);
                }
                return 1;
            }
        });

    }

    private static CompletableFuture<Integer> asyncDryRunVertexCreation(final Row row, final String nullValue,
                                                                        final FireflyGraph graph) {
        return CompletableFuture.supplyAsync(() -> {
            final GenericRowWithSchema metadataRow = (GenericRowWithSchema) row;
            final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNS_TO_REMOVE);
            try {
                SparkFireflyVertex.createVertex(fireflyRow, nullValue);
                return 0;
            } catch (final FireflyBulkLoaderException e) {
                final Set<String> columnToRemove = Set.of(FILENAME_COLUMN);
                final String filteredRow = DatasetOperations.removeColumns(metadataRow, columnToRemove).toString();
                final String fileName = metadataRow.get(metadataRow.fieldIndex(FILENAME_COLUMN)).toString();
                LOGGER.error("Format validation of CSV vertex data failed on row {} of file {}.",
                        filteredRow, fileName);
                if (graph != null) {
                    graph.writeBadEntry(filteredRow, fileName);
                }
                return 1;
            }
        });
    }

    public static void dryRunEdgeRows(final Dataset<Row> edgeDataset, final BulkLoaderConfigHelper config) {
        long badEntryCount = dryRunEdgeCreation(edgeDataset,config);
        final long allowedBadEntryCount = config.getOrDefaultInt(ALLOWED_BAD_ENTRY_COUNT);
        if (badEntryCount > 0) {
            try (final FireflyGraph graph = FireflyGraph.open(config.getFireflyConfig())) {
                int tryCount = 0;
                while (true) {
                    try {
                        badEntryCount = graph.getBaseGraph().incrementAndGetBadEntryCount(badEntryCount);
                        if (badEntryCount > allowedBadEntryCount) {
                            LOGGER.error("Found too many bad entries in Edge dataset, please check Spark worker logs for more details.");
                            throw new FireflyBulkLoaderPreflightException(BAD_ENTRY_COUNT_EXCEEDED);
                        }
                        break;
                    } catch (final AerospikeException e) {
                        final FireflyLoadingException fle = new FireflyLoadingException(e);
                        if (!fle.isRetryable()) {
                            LOGGER.error("Unexpected exception when attempting to get bad entry count: ", e);
                            throw e;
                        } else if (++tryCount > RETRY_LIMIT) {
                            LOGGER.error("Failed to get bad entry count after " + tryCount + " attempts.", e);
                            throw e;
                        } else {
                            LOGGER.warn("Failed to get bad entry count. Attempt count: " + tryCount, e);
                            exponentialBackoff(tryCount);
                        }
                    }
                }
            }
        }
    }

    public static void dryRunVertices(final Dataset<Row> vertices, final BulkLoaderConfigHelper config) {
        final Instant start = Instant.now();
        long badEntryCount = dryRunVertexCreation(vertices, config);

        long duplicateIdCount = 0;
        if (!config.hasAction(INCREMENTAL_LOAD)) {
            duplicateIdCount = validateNoDuplicateVertexIds(vertices, config);
        }
        LOGGER.info("Completed dryRunVertices; time taken (in seconds): {}", Duration.between(start,Instant.now()).getSeconds());

        if (duplicateIdCount > 0 || badEntryCount > 0) {
            try (final FireflyGraph graph = FireflyGraph.open(config.getFireflyConfig())) {
                final long allowedDuplicateIdCount = config.getOrDefaultInt(ALLOWED_DUPLICATE_VERTEX_ID_COUNT);
                if (duplicateIdCount > 0) {
                    int tryCount = 0;
                    while (true) {
                        try {
                            duplicateIdCount = graph.getBaseGraph().incrementAndGetDuplicateVertexIdCount(duplicateIdCount);
                            if (duplicateIdCount > allowedDuplicateIdCount) {
                                LOGGER.error("Found too many duplicate Vertex IDs in Vertex dataset, please check Spark worker logs for more details.");
                                throw new FireflyBulkLoaderPreflightException(DUPLICATE_VERTEX_ID_COUNT_EXCEEDED);
                            }
                            break;
                        } catch (final AerospikeException e) {
                            final FireflyLoadingException fle = new FireflyLoadingException(e);
                            if (!fle.isRetryable()) {
                                LOGGER.error("Unexpected exception when attempting to get duplicate Vertex ID count: ", e);
                                throw e;
                            } else if (++tryCount > RETRY_LIMIT) {
                                LOGGER.error("Failed to get duplicate Vertex ID count after " + tryCount + " attempts.", e);
                                throw e;
                            } else {
                                LOGGER.warn("Failed to get duplicate Vertex ID count. Attempt count: " + tryCount, e);
                                exponentialBackoff(tryCount);
                            }
                        }
                    }
                }

                final long allowedBadEntryCount = config.getOrDefaultInt(ALLOWED_BAD_ENTRY_COUNT);
                if (badEntryCount > 0) {
                    int tryCount = 0;
                    while (true) {
                        try {
                            badEntryCount = graph.getBaseGraph().incrementAndGetBadEntryCount(badEntryCount);
                            if (badEntryCount > allowedBadEntryCount) {
                                LOGGER.error("Found too many bad entries in Vertex dataset, please check Spark worker logs for more details.");
                                throw new FireflyBulkLoaderPreflightException(BAD_ENTRY_COUNT_EXCEEDED);
                            }
                            break;
                        } catch (final AerospikeException e) {
                            final FireflyLoadingException fle = new FireflyLoadingException(e);
                            if (!fle.isRetryable()) {
                                LOGGER.error("Unexpected exception when attempting to get bad entry count: ", e);
                                throw e;
                            } else if (++tryCount > RETRY_LIMIT) {
                                LOGGER.error("Failed to get bad entry count after " + tryCount + " attempts.", e);
                                throw e;
                            } else {
                                LOGGER.warn("Failed to get bad entry count. Attempt count: " + tryCount, e);
                                exponentialBackoff(tryCount);
                            }
                        }
                    }
                }
            }
        }
    }

    private static long dryRunVertexCreation(final Dataset<Row> vertices, final BulkLoaderConfigHelper config) {
        final String nullValue = config.getOrDefault(NULL_VALUE);
        final boolean allowBadEntries = config.getOrDefaultInt(ALLOWED_BAD_ENTRY_COUNT) > 0;
        final List<Long> failures = vertices.mapPartitions((MapPartitionsFunction<Row, Long>) rowIterator -> {
            long count = 0L;
            long failureCount = 0L;
            final int bufferSize = config.getOrDefaultInt(VERTEX_WRITE_BUFFER);
            final List<CompletableFuture<Integer>> futures = new ArrayList<>();
            final Instant start = Instant.now();
            LOGGER.info("Starting dryRunVertexCreation; partition-id: {}", TaskContext.getPartitionId());

            FireflyGraph graph = null;
            if (allowBadEntries) {
                graph = FireflyGraph.open(config.getFireflyConfig());
            }
            try {
                while (rowIterator.hasNext()) {
                    count += 1;
                    if (futures.size() > bufferSize) {
                        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
                        failureCount += countFailures(futures);
                        futures.clear();
                    }
                    futures.add(asyncDryRunVertexCreation(rowIterator.next(), nullValue, graph));
                }
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
                failureCount += countFailures(futures);
                futures.clear();

                LOGGER.info("Completed dryRunVertexCreation; partition-id: {}; time taken (in seconds): {}, records processed: {}",
                        TaskContext.getPartitionId(), Duration.between(start, Instant.now()).getSeconds(), count);
                return Collections.singletonList(failureCount).iterator();
            } finally {
                if (graph != null) {
                    graph.close();
                }
            }
        }, Encoders.LONG()).collectAsList();

        long badElementCount = 0;
        for (final Long l : failures) {
            badElementCount += l;
        }
        return badElementCount;
    }

    public static long validateNoDuplicateVertexIds(final Dataset<Row> vertices, final BulkLoaderConfigHelper config) {
        final String countColumn = "~count";
        final String idColumn = SparkFireflyElement.ID_HEADER;
        final long allowedDuplicateIdCount = config.getOrDefaultInt(ALLOWED_DUPLICATE_VERTEX_ID_COUNT);

        final Instant start = Instant.now();
        final Dataset<Row> dsWithCount = vertices.select(idColumn,FILENAME_COLUMN)
                .groupBy(idColumn)
                .agg(collect_set(FILENAME_COLUMN).as(FILENAME_COLUMN), count(idColumn).alias(countColumn));
        final Dataset<Row> duplicateID = dsWithCount.filter(col(countColumn).gt(1));

        final String[] columns = duplicateID.columns();
        final int idIdx = ArrayUtils.indexOf(columns, idColumn);
        final int fileColumnIdx = ArrayUtils.indexOf(columns, FILENAME_COLUMN);
        final int countColumnIdx = ArrayUtils.indexOf(columns, countColumn);

        final List<Long> failures = duplicateID.mapPartitions((MapPartitionsFunction<Row, Long>) rowIterator -> {
            FireflyGraph graph = null;
            if (allowedDuplicateIdCount > 0) {
                graph = FireflyGraph.open(config.getFireflyConfig());
            }
            try {
                final AtomicLong failureCount = new AtomicLong(0);
                while (rowIterator.hasNext()) {
                    final Row row = rowIterator.next();
                    final Object id = row.get(idIdx);
                    final long count = (long) row.get(countColumnIdx);
                    LOGGER.error("Vertex id: {}, found total {} occurrences in files {}", id, count, row.getList(fileColumnIdx));
                    // Subtract 1 since the failure count is the number of duplicates, not the total count.
                    failureCount.addAndGet(count - 1);
                    if (graph != null) {
                        graph.writeDuplicateVertexId(id, count);
                    }
                }
                return Collections.singletonList(failureCount.get()).iterator();
            } finally {
                if (graph != null) {
                    graph.close();
                }
            }
        }, Encoders.LONG()).collectAsList();

        long duplicateCount = 0;
        for (final Long l : failures) {
            duplicateCount += l;
        }
        LOGGER.info("Completed validateNoDuplicateVertexIds; time taken (in seconds): {}", Duration.between(start, Instant.now()).getSeconds());
        return duplicateCount;
    }
}
