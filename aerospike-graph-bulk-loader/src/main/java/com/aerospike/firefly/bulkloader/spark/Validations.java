package com.aerospike.firefly.bulkloader.spark;

import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyElement;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyVertex;
import com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper;
import com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyBulkLoaderException;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.COLUMNS_TO_REMOVE;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.FILENAME_COLUMN;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.LINENUMBER_COLUMN;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.EDGE_WRITE_BUFFER;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.NULL_VALUE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.count;

public class Validations {
    private static final Logger LOGGER = LoggerFactory.getLogger(Validations.class);

    private static boolean dryRunEdgeCreation(final Dataset<Row> edgeDataset, final BulkLoaderConfigHelper config) {
        final List<Long> failures = edgeDataset.mapPartitions((MapPartitionsFunction<Row, Long>) rowIterator -> {
            final boolean keepProvidedId =
                    Boolean.parseBoolean(config.getOrDefault(KEEP_PROVIDED_EDGE_ID_AS_PROPERTY));
            final String providedIdPropertyName = config.getOrDefault(PROVIDED_EDGE_ID_PROPERTY_NAME);
            final String nullValue = config.getOrDefault(NULL_VALUE);
            long failureCount = 0L;
            final int bufferSize = Integer.parseInt(config.getOrDefault(EDGE_WRITE_BUFFER).trim());
            final List<CompletableFuture<Integer>> futures = new ArrayList<>();
            while (rowIterator.hasNext()) {
                if (futures.size() >= bufferSize) {
                    final CompletableFuture<Void> megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                    megaTask.join();
                    failureCount += countFailures(futures);
                    futures.clear();
                }
                futures.add(asyncDryRunEdgeCreation(rowIterator.next(), keepProvidedId, providedIdPropertyName, nullValue));
            }

            final CompletableFuture megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
            megaTask.join();
            failureCount += countFailures(futures);
            futures.clear();
            return Collections.singletonList(failureCount).iterator();
        }, Encoders.LONG()).collectAsList();
        boolean valid = failures.stream().filter(item -> item > 0).findAny().isEmpty();
        if (!valid) {
            LOGGER.error("Format validation of Edge CSV data failed.");
        }
        return valid;
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
                                                                      final String nullValue) {
        return CompletableFuture.supplyAsync(() -> {
            final GenericRowWithSchema metadataRow = (GenericRowWithSchema) row;
            final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNS_TO_REMOVE);
            try {
                SparkFireflyEdge.createEdge(fireflyRow, keepProvidedId, providedIdPropertyName, nullValue, null, true);
                return 0;
            } catch (final FireflyBulkLoaderException e) {
                LOGGER.error("Edge Format validation of CSV data failed on row {} of file {} at line {}.",
                        metadataRow, metadataRow.get(metadataRow.fieldIndex(FILENAME_COLUMN)), metadataRow.get(metadataRow.fieldIndex(LINENUMBER_COLUMN)));
                return 1;
            }
        });

    }

    private static CompletableFuture<Integer> asyncDryRunVertexCreation(final Row row, final String nullValue) {
        return CompletableFuture.supplyAsync(() -> {
            final GenericRowWithSchema metadataRow = (GenericRowWithSchema) row;
            final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNS_TO_REMOVE);
            try {
                SparkFireflyVertex.createVertex(fireflyRow, nullValue);
                return 0;
            } catch (final FireflyBulkLoaderException e) {
                LOGGER.error("Format validation of CSV vertex data failed on row {} of file {}.",
                        metadataRow, metadataRow.get(metadataRow.fieldIndex(FILENAME_COLUMN)));
                return 1;
            }
        });
    }

    public static boolean dryRunEdgeRows(final Dataset<Row> edgeDataset, final BulkLoaderConfigHelper config) {
        return dryRunEdgeCreation(edgeDataset,config);
    }

    public static boolean dryRunVertices(final Dataset<Row> vertices, final BulkLoaderConfigHelper config) {
        final Instant start = Instant.now();
        final boolean creationSuccess = dryRunVertexCreation(vertices,config);
        final boolean noDuplicateIdDetected = validateNoDuplicateVertexIds(vertices);
        LOGGER.info("Completed dryRunVertices; time taken (in seconds): {}", Duration.between(start,Instant.now()).getSeconds());
        return creationSuccess && noDuplicateIdDetected;
    }

    private static boolean dryRunVertexCreation(final Dataset<Row> vertices, final BulkLoaderConfigHelper config) {
        final String nullValue = config.getOrDefault(NULL_VALUE);
        final List<Long> failures = vertices.mapPartitions((MapPartitionsFunction<Row, Long>) rowIterator -> {
            long count = 0L;
            long failureCount = 0L;
            final int bufferSize = Integer.parseInt(config.getOrDefault(EDGE_WRITE_BUFFER).trim());
            final List<CompletableFuture<Integer>> futures = new ArrayList<>();
            final Instant start = Instant.now();
            LOGGER.info("Starting dryRunVertexCreation; partition-id: {}", TaskContext.getPartitionId());
            while (rowIterator.hasNext()) {
                count += 1;
                if (futures.size() > bufferSize) {
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
                    failureCount += countFailures(futures);
                    futures.clear();
                }
                futures.add(asyncDryRunVertexCreation(rowIterator.next(), nullValue));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
            failureCount += countFailures(futures);
            futures.clear();

            LOGGER.info("Completed dryRunVertexCreation; partition-id: {}; time taken (in seconds): {}, records processed: {}",
                    TaskContext.getPartitionId(), Duration.between(start, Instant.now()).getSeconds(), count);
            return Collections.singletonList(failureCount).iterator();
        }, Encoders.LONG()).collectAsList();

        boolean valid = failures.stream().filter(item -> item > 0).findAny().isEmpty();
        if (!valid) {
            LOGGER.error("Format validation of Vertex CSV data failed.");
        }
        return valid;
    }

    public static boolean validateNoDuplicateVertexIds(final Dataset<Row> vertices) {
        final String countColumn = "~count";
        final String idColumn = SparkFireflyElement.ID_HEADER;

        final Instant start = Instant.now();
        final Dataset<Row> dsWithCount = vertices.select(idColumn,FILENAME_COLUMN)
                .groupBy(idColumn)
                .agg(collect_set(FILENAME_COLUMN).as(FILENAME_COLUMN), count(idColumn).alias(countColumn));
        final Dataset<Row> duplicateID = dsWithCount.filter(col(countColumn).gt(1));

        final String[] columns = duplicateID.columns();
        final int idIdx = ArrayUtils.indexOf(columns, idColumn);
        final int fileColumnIdx = ArrayUtils.indexOf(columns,FILENAME_COLUMN);
        final int countColumnIdx = ArrayUtils.indexOf(columns,countColumn);

        final List<Integer> failures = duplicateID.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
            final AtomicInteger failureCount = new AtomicInteger(0);
            while (rowIterator.hasNext()) {
                final Row row = rowIterator.next();
                LOGGER.error("Vertex id: {}, found total {} occurrences in files {}", row.get(idIdx), row.get(countColumnIdx), row.getList(fileColumnIdx));
                failureCount.incrementAndGet();
            }
            return Collections.singletonList(failureCount.get()).iterator();
        }, Encoders.INT()).collectAsList();

        boolean valid = failures.stream().filter(item -> item > 0).findAny().isEmpty();
        if (!valid) {
            LOGGER.error("Found duplicate Vertex IDs in Vertex dataset, please check Spark worker logs for more details.");
        }
        LOGGER.info("Completed validateNoDuplicateVertexIds; time taken (in seconds): {}", Duration.between(start, Instant.now()).getSeconds());
        return valid;
    }
}
