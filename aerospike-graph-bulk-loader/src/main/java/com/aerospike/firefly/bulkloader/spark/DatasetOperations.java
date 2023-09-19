package com.aerospike.firefly.bulkloader.spark;

import com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper;
import com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyBulkLoaderPreflightException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.DATAFRAME_STORAGE_TYPE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ENABLE_DATAFRAME_CACHING;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.SAMPLING_PERCENTAGE;
import static com.aerospike.firefly.process.call.bulkload.utils.CommandLineParser.DRY_RUN;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.input_file_name;

public class DatasetOperations implements Serializable {
    public static int RETRY_LIMIT = 20; // Not provided through config
    public static final String FILENAME_COLUMN = "~fileName";
    public static final String EDGE_ID_COLUMN = "~edgeid";
    public static final Set<String> COLUMNS_TO_REMOVE = Set.of(FILENAME_COLUMN, EDGE_ID_COLUMN);
    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetOperations.class);

    public DatasetOperations() {}

    /**
     * Function to merge Datasets.
     *
     * @param spark    Spark session
     * @param datasets List of Datasets to merge
     * @return Merged Dataset<Row> from all the given Datasets
     */
    private static Dataset<Row> mergeDatasets(final SparkSession spark,
                                             final List<Dataset<Row>> datasets) {
        Objects.nonNull(datasets);
        if (datasets.isEmpty()) {
            return spark.emptyDataFrame();
        } else {
            Dataset<Row> unionDs = datasets.get(0);
            for (int i = 1; i < datasets.size(); i++) {
                unionDs = unionDs.unionByName(datasets.get(i), true);
            }
            return unionDs;
        }
    }

    /**
     * Convert csv files to a list of Datasets.
     *
     * @param spark           Spark session
     * @param csvPaths        List of paths to csv files to convert to Datasets
     * @param requiredHeaders List of required headers in the csv files
     * @return List of Dataset<Row> from the list of csv files
     */
    public static List<Dataset<Row>> createDatasets(final SparkSession spark, final List<String> csvPaths,
                                                    final List<String> requiredHeaders) {

        final List<Dataset<Row>> datasets = new ArrayList<>();
        for (final String csv : csvPaths) {
            final Dataset<Row> dataset = spark.read()
                    .option("header", "true")
                    .option("recursiveFileLookup", "true").csv(csv)
                    .select(input_file_name().as(FILENAME_COLUMN), col("*"));
            testHeaders(requiredHeaders, csv, dataset);
            datasets.add(dataset);
        }
        return datasets;
    }

    public static void testHeaders(List<String> requiredHeaders, String csv, Dataset<Row> dataset) {
        final Set<String> headers = Set.of(dataset.columns());
        for (final String requiredHeader : requiredHeaders) {
            if (!headers.contains(requiredHeader)) {
                throw new RuntimeException(String.format("Unable to find the required column header values %s in directory %s files", requiredHeader, csv));
            }
        }
    }

    public static GenericRowWithSchema removeColumns(final GenericRowWithSchema row, final Set<String> columnsToRemove) {
        ArrayList<Object> values = new ArrayList<>();
        StructType oldSchema = row.schema();
        StructType newSchema = new StructType(Arrays.stream(oldSchema.fields())
                .filter(field -> !columnsToRemove.contains(field.name()))
                .toArray(StructField[]::new));
        for (int i = 0; i < row.size(); i++) {
            if (!columnsToRemove.contains(row.schema().fields()[i].name())) {
                values.add(row.get(i));
            }
        }
        String[] array = new String[values.size()];
        array = values.toArray(array);
        return new GenericRowWithSchema(array, newSchema);
    }

    /**
     * Extract dataframe storage level from configuration
     */
    public static StorageLevel getDfStorageLevel(final BulkLoaderConfigHelper config) {
        StorageLevel storageLevel = StorageLevel.NONE();
        if (Boolean.parseBoolean(config.getOrDefault(ENABLE_DATAFRAME_CACHING))) {
            switch (config.getOrDefault(DATAFRAME_STORAGE_TYPE).toLowerCase()) {
                case "memory_only":
                    storageLevel = StorageLevel.MEMORY_ONLY();
                    break;
                case "memory_and_disk":
                    storageLevel = StorageLevel.MEMORY_AND_DISK();
                    break;
                default:
                    LOGGER.info("Default Storage Level set for persist operation = {}", StorageLevel.DISK_ONLY());
                    storageLevel = StorageLevel.DISK_ONLY();
            }
        }
        return storageLevel;
    }

    public static Dataset<Row> loadDataset(final SparkSession session, final List<String> paths, final List<String> requiredHeaders, StorageLevel level) {
        Dataset<Row> data = mergeDatasets(session, createDatasets(session, paths, requiredHeaders));
        return persistIfPossible(level, data);
    }

    public static Dataset<Row> persistIfPossible(StorageLevel level, Dataset<Row> data) {
        return level.isValid() ? data.persist(level) : data;
    }

    public static double getSamplingPercent(final BulkLoaderConfigHelper config) {
        return Double.parseDouble(config.getOrDefault(SAMPLING_PERCENTAGE)) / 100;
    }

    public static void preflightCheck(final Dataset<Row> edgeDataset, final Dataset<Row> vertexDataset,
                                      final BulkLoaderConfigHelper config) {
        if (config.hasAction(DRY_RUN)) {
            final String taskName = "Preflight check";
            edgeDataset.sparkSession().sparkContext().setJobGroup(taskName, taskName + " task", true);
            final Instant start = Instant.now();
            LOGGER.info("Starting preflightCheck, partition-id:{}", TaskContext.getPartitionId());
            LOGGER.info("Number of partitions in vertexDataset: {}, number of partitions in edgeDataset {}", Arrays.stream(vertexDataset.rdd().getPartitions()).count(), Arrays.stream(edgeDataset.rdd().getPartitions()).count());
            final boolean preflightVertexSuccess = Validations.dryRunVertices(vertexDataset, config);
            final boolean preflightEdgeSuccess = Validations.dryRunEdgeRows(edgeDataset, config);
            final String preflightEdgeFailed = "Detected invalid CSV data in Edge pre-flight check.";
            final String preflightVertexFailed = "Detected invalid CSV data in Vertex pre-flight check.";

            if (!preflightVertexSuccess) {
                LOGGER.error(preflightVertexFailed);
            }
            if (!preflightEdgeSuccess) {
                LOGGER.error(preflightEdgeFailed);
            }
            if (!preflightEdgeSuccess || !preflightVertexSuccess) {
                throw new FireflyBulkLoaderPreflightException("Pre-flight checks failed. Check logs for details on which line number and files caused the failure.");
            }

            edgeDataset.sparkSession().sparkContext().cancelJobGroup(taskName);
            final Instant end = Instant.now();
            LOGGER.info("Completed preflightCheck. Time taken (in seconds):{}", Duration.between(start, end).getSeconds());
        }
    }

    protected static int processBatch(int bufferSize, int batch, List<Future<?>> futures, final String errorMessage) {
        if (futures.size() >= bufferSize) {
            CompletableFuture<?> megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
            megaTask.join();
            futures.clear();
            batch = batch + 1;
            if (megaTask.isCompletedExceptionally()) {
                throw new RuntimeException(errorMessage);
            }
        }
        return batch;
    }

    private static final Supplier<ScheduledExecutorService> THREADPOOL_SUPPLIER = new Supplier<>() {
        private ScheduledThreadPoolExecutor instance = null;
        @Override
        public ScheduledThreadPoolExecutor get() {
            if (instance == null) {
                synchronized (this) {
                    if (instance == null || instance.isShutdown()) {
                        final ThreadFactory threadFactory =
                                new ThreadFactoryBuilder().setNameFormat("common-dataset-operation-pool").setDaemon(true).build();
                        instance = new ScheduledThreadPoolExecutor( Runtime.getRuntime().availableProcessors() * 2, threadFactory);
                    }
                }
            }
            return instance;
        }
    };

    public static ScheduledExecutorService getScheduledThreadPoolService() {
        return THREADPOOL_SUPPLIER.get();
    }
}
