package com.aerospike.firefly.bulkloader.spark;

import com.aerospike.firefly.bulkloader.exception.FireflyBulkLoaderPreflightException;
import com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper;
import joptsimple.internal.Strings;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.DATAFRAME_STORAGE_TYPE;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.ENABLE_DATAFRAME_CACHING;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.SAMPLING_PERCENTAGE;
import static com.aerospike.firefly.bulkloader.util.CommandLineParser.DRY_RUN;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

public class DatasetOperations implements Serializable {
    public static final int RETRY_LIMIT = 20; // not provided through config
    public static final String FILENAME_COLUMN = "~fileName";
    public static final String LINENUMBER_COLUMN = "~line";
    public static final String DIRECTORY_COLUMN = "~directory";
    public static final Set<String> COLUMNSET_TO_REMOVE = new HashSet<>((Arrays.asList(DIRECTORY_COLUMN, FILENAME_COLUMN, LINENUMBER_COLUMN)));
    public static final int THREAD_POOL_BUFFER_SIZE = 4;
    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetOperations.class);

    public DatasetOperations() {
    }

    /**
     * Function to merge Datasets.
     *
     * @param spark    Spark session
     * @param datasets List of Datasets to merge
     * @return Merged Dataset<Row> from all the given Datasets
     */
    public static Dataset<Row> mergeDatasets(final SparkSession spark,
                                             final List<Dataset<Row>> datasets) {
        Objects.requireNonNull(datasets);
        Dataset<Row> unionDs = spark.emptyDataFrame();
        for (final Dataset<Row> dataset : datasets) {
            if (unionDs.isEmpty()) {
                unionDs = dataset.select(input_file_name().as(FILENAME_COLUMN), col("*"))
                        .withColumn(LINENUMBER_COLUMN, monotonically_increasing_id());
            } else {
                unionDs = unionDs.unionByName(dataset.select(input_file_name().as(FILENAME_COLUMN), col("*"))
                        .withColumn(LINENUMBER_COLUMN, monotonically_increasing_id()), true);
            }
        }
        return unionDs;
//        return unionDs.persist(level);
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

        LOGGER.info("csvPaths: {}", Strings.join(csvPaths, ", "));
        final List<Dataset<Row>> datasets = new ArrayList<>();
        for (final String csv : csvPaths) {
            final Dataset<Row> dataset = spark.read().option("header", "true").csv(csv)
                    .withColumn(DIRECTORY_COLUMN, lit(csv));
            final Set<String> headers = Set.of(dataset.columns());
            for (final String requiredHeader : requiredHeaders) {
                if (!headers.contains(requiredHeader)) {
                    throw new RuntimeException("Unable to find the required column header values '" +
                            requiredHeaders + "' in source file '" + csv + "'.");
                }
            }
            datasets.add(dataset);
        }
        return datasets;
    }

    public static GenericRowWithSchema removeColumns(final GenericRowWithSchema row, final Set<String> columnsToRemove) {
        Object[] values = new Object[row.size() - columnsToRemove.size()];
        StructType oldSchema = row.schema();
        StructType newSchema = new StructType(Arrays.stream(oldSchema.fields())
                .filter(field -> !columnsToRemove.contains(field.name()))
                .toArray(StructField[]::new));
        int index = 0;
        for (int i = 0; i < row.size(); i++) {
            if (!columnsToRemove.contains(row.schema().fields()[i].name())) {
                values[index] = row.get(i);
                index++;
            }
        }
        return new GenericRowWithSchema(values, newSchema);
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
        return level.isValid() ? data.persist(level) : data;
    }

    public static double getSamplingPercent(final BulkLoaderConfigHelper config) {
        return Double.parseDouble(config.getOrDefault(SAMPLING_PERCENTAGE)) / 100;
    }

    public static void preflightCheck(final Dataset<Row> edgeDataSet, final Dataset<Row> vertexDataset,
                                      final BulkLoaderConfigHelper config) {
            if (config.hasAction(DRY_RUN)) {
                final boolean preflightVertexSuccess = VertexOperations.dryRunVertices(vertexDataset, config);
                final boolean preflightEdgeSuccess = EdgeOperations.dryRunEdgeRows(edgeDataSet, config);
                final String preflightEdgeFailed = "Detected invalid CSV data in EDGE pre-flight check.";
                final String preflightVertexFailed = "Detected invalid CSV data in VERTEX pre-flight check.";

                if (!preflightVertexSuccess) {
                    LOGGER.error(preflightVertexFailed);
                }
                if (!preflightEdgeSuccess) {
                    LOGGER.error(preflightEdgeFailed);
                }
                if (!(preflightEdgeSuccess && preflightVertexSuccess)) {
                    throw new FireflyBulkLoaderPreflightException("Preflight checks failed, check logs for detail on which line number and file caused the failure.");
                }

                LOGGER.info("Completed dryrun/preflight check.");
            }
        }
}
