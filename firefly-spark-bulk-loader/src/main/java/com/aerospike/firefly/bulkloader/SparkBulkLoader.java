package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.bulkloader.exception.FireflyBulkLoaderCsvException;
import com.aerospike.firefly.bulkloader.exception.FireflyBulkLoaderPreflightException;
import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.storage.FileLoader;
import com.aerospike.firefly.bulkloader.storage.ObjectLoader;
import com.aerospike.firefly.bulkloader.storage.S3ObjectLoader;
import com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper;
import com.aerospike.firefly.bulkloader.util.ProgressBar;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Timer;

import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.createDatasets;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.FROM_VERTEX_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.TO_VERTEX_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyElement.ID_HEADER;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ADJACENCY_INDEX_ENABLED;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.EDGE_CACHE_DISABLED_GLOBALLY;

public class SparkBulkLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoader.class);
    private static final List<String> REQUIRED_VERTEX_HEADERS = List.of(ID_HEADER);
    private static final List<String> REQUIRED_EDGE_HEADERS = List.of(FROM_VERTEX_HEADER, TO_VERTEX_HEADER);
    private static Configuration CONFIG;
    private static String MODE = "cluster";
    private static final ProgressBar progressBar = new ProgressBar();
    private static final Timer progressBarTimer = new Timer();

    public static void main(final String[] args) {
        String s3BucketName = null;
        ObjectLoader loader;
        final CommandLine cmd = com.aerospike.firefly.bulkloader.util.CommandLineParser.parseCmdArgs(args);
        // mode = local/cluster. If running in IDE, set -m local, if spark-submit, set -m cluster
        MODE = cmd.hasOption("m") ? cmd.getOptionValue("m") : MODE;
        final String ENV = cmd.hasOption("e") ? cmd.getOptionValue("e") : "";
        String configPath = cmd.hasOption("c") ? cmd.getOptionValue("c") : null;
        StorageLevel dfStorageLevel = StorageLevel.NONE();
        LOGGER.info("Config path provided = {} & job running in {} mode", configPath, MODE);
        if (configPath == null) {
            throw new RuntimeException("Failed to start bulk loader due to null configPath (" + configPath + ")");
        }
        if (ENV.equalsIgnoreCase("aws")) {
            s3BucketName = cmd.getOptionValue("b");
            if (s3BucketName == null) {
                throw new RuntimeException("Failed to start bulk loader due to null s3BucketName (" + s3BucketName + ")");
            }
            loader = S3ObjectLoader.getInstance();
            ((S3ObjectLoader)loader).setBucketName(s3BucketName);
        } else {
            loader = FileLoader.getInstance();
        }

        CONFIG = loader.loadConfiguration(configPath);
        try {
            // Once graph is set in progress bar, it will be used to update progress bar.
            // If graph fails to open for some reason, it will be null internally and progress bar will not report.
            progressBar.setGraph(FireflyGraph.open(CONFIG));
            progressBarTimer.scheduleAtFixedRate(progressBar, 0, 10000);
        } catch (final Exception e) {
            LOGGER.warn("Failed to start progress bar", e);
        }

        final double sampleFraction = Double.parseDouble(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.SAMPLING_PERCENTAGE, CONFIG)) / 100;
        final boolean enableDFCaching = Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.ENABLE_DATAFRAME_CACHING,CONFIG));
        if (enableDFCaching) {
            switch (BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.DATAFRAME_STORAGE_TYPE, CONFIG)) {
                case "memory_only":
                    dfStorageLevel = StorageLevel.MEMORY_ONLY();
                    break;
                case "memory_and_disk":
                    dfStorageLevel = StorageLevel.MEMORY_AND_DISK();
                    break;
                default:
                    LOGGER.info("Default Storage Level set for persist operation = {}", dfStorageLevel);
                    dfStorageLevel = StorageLevel.DISK_ONLY();
            }
        }

        final String SPARK_LOG_LEVEL = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.SPARK_LOG_LEVEL, CONFIG).toUpperCase();

        // Initialize Spark.
        final SparkConf conf = new SparkConf();
        setSparkConf(conf);

        final SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
        // Set LOG LEVEL for spark logging to disable logging of each step during debugging purposes.
        spark.sparkContext().setLogLevel(SPARK_LOG_LEVEL);

        final String vertexDirectory = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.VERTEX_DIRECTORY_KEY, CONFIG);
        final String edgeDirectory = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.EDGE_DIRECTORY_KEY, CONFIG);
        LOGGER.info("Vertex directory provided: {}", vertexDirectory);
        LOGGER.info("Edge directory provided: {}", edgeDirectory);

        // Get all csv files in the directory.
        List<String> vertexPaths = null;
        List<String> edgePaths = null;
        try {
            vertexPaths = loader.getCsvPaths(vertexDirectory);
            if (vertexPaths.isEmpty()) {
                // Only check empty vertex paths since not loading any edges is a potential valid use case
                throw new RuntimeException("Failed to find files in path '" + vertexDirectory +
                        "'; please review the content of this directory and make sure it contains valid csv files.");
            }
            edgePaths = loader.getCsvPaths(edgeDirectory);
        } catch (IOException e) {
            LOGGER.error("Failed to load input CSV files.", e);
            throw new FireflyBulkLoaderCsvException(e);
        }

        // Convert csv files to Datasets.
        final List<Dataset<Row>> vertexDatasets = createDatasets(spark, vertexPaths, REQUIRED_VERTEX_HEADERS);
        final List<Dataset<Row>> edgeDatasets = createDatasets(spark, edgePaths, REQUIRED_EDGE_HEADERS);

        // Merge Vertex Dataset.
        final Dataset<Row> unionVertexDS = DatasetOperations.mergeDatasets(spark, vertexDatasets);
        final Dataset<Row> persistedVertexDS;
        if (dfStorageLevel.isValid()) {
            persistedVertexDS = unionVertexDS.persist(dfStorageLevel);
            LOGGER.info("Storage Level for vertex dataset = {}", dfStorageLevel);
        } else {
            persistedVertexDS = unionVertexDS;
        }

        // Merge Edge Dataset.
        final Dataset<Row> unionEdgeDS = DatasetOperations.mergeDatasets(spark, edgeDatasets);

        final Dataset<Row> persistedEdgeDS;
        if (dfStorageLevel.isValid()) {
            persistedEdgeDS = unionEdgeDS.persist(dfStorageLevel);
            LOGGER.info("Storage Level for Edge dataset = {}", dfStorageLevel);
        } else {
            persistedEdgeDS = unionEdgeDS;
        }

        // Sample out vertex dataset for verifying the inserts.
        final Dataset<Row> sampledVertexDatasets = persistedVertexDS.sample(sampleFraction);

        final String finalS3BucketName = s3BucketName;

        // Pre-flight verification that row data can be parsed into their respective SparkFireflyELement.
        // Verify Vertex row data can be parsed into SparkFireflyVertex (dry run).
        if (!DatasetOperations.verifyVertexRows(persistedVertexDS, configPath, ENV, finalS3BucketName) ||
                !DatasetOperations.verifyEdgeRows(persistedEdgeDS, configPath, ENV, finalS3BucketName)) {
            final String preflightFailed = "Detected invalid CSV data in pre-flight check. See logs for detail on which line number and file caused the failure.";
            throw new FireflyBulkLoaderPreflightException(preflightFailed);
        }

        // Write Vertices.
        final Instant startOfVertexMapPartitions = Instant.now();
        spark.sparkContext().setJobGroup("Vertex write", "Vertex MapPartition and collectAsList", true);

        progressBar.setVertexLoadStart();
        final List<Long> vertexResult = DatasetOperations.writeVertices(persistedVertexDS, configPath, ENV, finalS3BucketName);
        progressBar.setVertexLoadComplete();

        final Instant endOfVertexMapPartitions = Instant.now();
        Duration vertexInterval = Duration.between(startOfVertexMapPartitions, endOfVertexMapPartitions);
        LOGGER.info("Execution time in seconds for vertexMapPartitions mapPartitions block: " + vertexInterval.getSeconds());

        final int totalVertexDuration = vertexResult.stream().mapToInt(Math::toIntExact).sum();
        int noOfVertexPartitions = vertexResult.size();
        LOGGER.info("Mean time taken per Vertex partition for " + noOfVertexPartitions + " partitions = " + totalVertexDuration);

        // Verify vertices.
        spark.sparkContext().setJobGroup("Verify Vertex", "Verify vertex MapPartition", true);
        DatasetOperations.verifyVertices(sampledVertexDatasets, configPath, ENV, finalS3BucketName);
        progressBar.setVertexValidationComplete();

        // Sample out edge dataset to verify the inserts.
        final Dataset<Row> edgeDatasetsSample = persistedEdgeDS.sample(sampleFraction);

        // If edge caches or adjacency indexes are enabled need to identify supernodes.
        if (!Boolean.parseBoolean(ConfigurationHelper.getOrDefault(EDGE_CACHE_DISABLED_GLOBALLY, CONFIG)) ||
                Boolean.parseBoolean(ConfigurationHelper.getOrDefault(ADJACENCY_INDEX_ENABLED, CONFIG))) {
            spark.sparkContext().setJobGroup("Compute Supernodes", "Compute Supernodes RDD operation", true);
            // Csv format is: ~id, ~from, ~to, ...
            DatasetOperations.extractSupernodes(persistedEdgeDS, CONFIG);
        }
        progressBar.setSuperNodeExtractionComplete();

        // Write edges and to edge cache of non-supernodes.
        final Instant startOfEdgeMapPartitions = Instant.now();
        spark.sparkContext().setJobGroup("Edges write", "Edges MapPartition and collectAsList", true);

        progressBar.setEdgeLoadStart();
        final List<Long> edgeResult = DatasetOperations.writeEdges(configPath, persistedEdgeDS, ENV, finalS3BucketName);
        progressBar.setEdgeLoadComplete();
        final Instant endOfEdgeMapPartitions = Instant.now();
        Duration edgeInterval = Duration.between(startOfEdgeMapPartitions, endOfEdgeMapPartitions);
        LOGGER.info("Execution time in seconds for Edge mapPartitions block: " + edgeInterval.getSeconds());

        final int totalEgdeDuration = edgeResult.stream().mapToInt(Math::toIntExact).sum();
        int noOfEdgePartitions = edgeResult.size();
        LOGGER.info("Mean time taken per Edge partition for " + noOfEdgePartitions + " partitions = " + totalEgdeDuration);

        // Verify edges.
        spark.sparkContext().setJobGroup("Verify Edges", "Verify Edges MapPartition", true);
        DatasetOperations.verifyEdges(configPath, edgeDatasetsSample, ENV, finalS3BucketName);
        progressBar.setEdgeValidationComplete();

        // Stop spark session
        spark.stop();
    }

    private static void setSparkConf(SparkConf conf) {
        if (MODE.equals("local"))
            conf.setMaster("local[*]");

        conf.setAppName("firefly-bulk-loader")
                .set("spark.driver.allowMultipleContexts", "false")
                .set("spark.ui.enabled", "true")
                .set("mapreduce.fileoutputcommitter.algorithm.version","2");
    }

    public static void exponentialBackoff(final int attempt) {
        // This is to prevent overflow since we cap at 10000ms anyways.
        final int cappedAttempt = Math.min(attempt, 14);

        int exponentialTime = Math.min(10000, (int) Math.pow(2, cappedAttempt));
        final int tenPercentSeed = exponentialTime / 10;
        final int jitter = (int) ((Math.random() * tenPercentSeed) - tenPercentSeed);
        exponentialTime += jitter;

        try {
            Thread.sleep(exponentialTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
