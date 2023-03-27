package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyElement;
import com.aerospike.firefly.bulkloader.storage.FileLoader;
import com.aerospike.firefly.bulkloader.storage.ObjectLoader;
import com.aerospike.firefly.bulkloader.storage.S3ObjectLoader;
import com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.FROM_VERTEX_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.TO_VERTEX_HEADER;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.EDGE_CACHE_DISABLED_GLOBALLY;

public class SparkBulkLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoader.class);
    private static final String[] REQUIRED_VERTEX_HEADERS = new String[]{SparkFireflyElement.ID_HEADER};
    private static final String[] REQUIRED_EDGE_HEADERS = new String[]{SparkFireflyElement.ID_HEADER, FROM_VERTEX_HEADER, TO_VERTEX_HEADER};
    private static Configuration CONFIG;
    private static String MODE = "cluster";

    public static void main(final String[] args) {
        String s3BucketName = null;
        ObjectLoader loader;
        final Set<String> vertexDirectories = new HashSet<>();
        final Set<String> edgeDirectories = new HashSet<>();
        final CommandLine cmd = com.aerospike.firefly.bulkloader.util.CommandLineParser.parseCmdArgs(args);
        // mode = local/cluster. If running in IDE, set -m local, if spark-submit, set -m cluster
        MODE = cmd.hasOption("m") ? cmd.getOptionValue("m") : MODE;
        final String ENV = cmd.hasOption("e") ? cmd.getOptionValue("e") : "";
        String configPath = cmd.hasOption("c") ? cmd.getOptionValue("c") : null;
        StorageLevel dfStorageLevel = StorageLevel.NONE();
        LOGGER.info("Config path provided = {} & job running in {} mode", configPath, MODE);
        try {
            if (configPath == null)
                throw new RuntimeException("Failed to start bulk loader due to null configPath (" + configPath + ")");

            if (ENV.equalsIgnoreCase("aws")) {
                s3BucketName = cmd.getOptionValue("b");
                if (s3BucketName == null)
                    throw new RuntimeException("Failed to start bulk loader due to null s3BucketName (" + s3BucketName + ")");
                loader = S3ObjectLoader.getInstance();
                ((S3ObjectLoader)loader).setBucketName(s3BucketName);
            } else loader = FileLoader.getInstance();

            CONFIG = loader.loadConfiguration(configPath);
            vertexDirectories.addAll(loader.getObjectList(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.VERTEX_DIRECTORY_KEY, CONFIG)));
            edgeDirectories.addAll(loader.getObjectList(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.EDGE_DIRECTORY_KEY, CONFIG)));
        }
        catch (final IOException ie) {
            LOGGER.error("Unable to load config.", ie);
            ie.printStackTrace();
            System.exit(1);
        }
        catch (final RuntimeException runtimeException) {
            LOGGER.error("Amazon SDK client error", runtimeException);
            System.exit(1);
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

        Dataset<Row> unionVertexDS = DatasetOperations.loadAndMergeDatasets(spark, vertexDirectories, REQUIRED_VERTEX_HEADERS);
        // Sample out vertex dataset for verifying the inserts.
        Dataset<Row> sampledVertexDatasets = unionVertexDS.sample(sampleFraction);
        Dataset<Row> persistedVertexDS;
        if (dfStorageLevel.isValid()) {
            persistedVertexDS = unionVertexDS.persist(dfStorageLevel);
            LOGGER.info("Storage Level for vertex dataset = {}", dfStorageLevel);
        } else {
            persistedVertexDS = unionVertexDS;
		}
        final String finalS3BucketName = s3BucketName;

        // Write Vertices.
        final Instant startOfVertexMapPartitions = Instant.now();
        spark.sparkContext().setJobGroup("Vertex write", "Vertex MapPartition and collectAsList", true);
        final List<Long> vertexResult = DatasetOperations.vertexWrite(persistedVertexDS, configPath, ENV, finalS3BucketName);
        final Instant endOfVertexMapPartitions = Instant.now();
        Duration vertexInterval = Duration.between(startOfVertexMapPartitions, endOfVertexMapPartitions);
        LOGGER.info("Execution time in seconds for vertexMapPartitions mapPartitions block: " + vertexInterval.getSeconds());

        final int totalVertexDuration = vertexResult.stream().mapToInt(Math::toIntExact).sum();
        int noOfVertexPartitions = vertexResult.size();
        LOGGER.info("Mean time taken per Vertex partition for " + noOfVertexPartitions + " partitions = " + totalVertexDuration);

        // Verify vertices.
        spark.sparkContext().setJobGroup("Verify Vertex", "Verify vertex MapPartition", true);
        DatasetOperations.verifyVertices(sampledVertexDatasets, configPath, ENV, finalS3BucketName);

        // Load and Merge Edges.
        final Dataset<Row> unionEdgeDS = DatasetOperations.loadAndMergeDatasets(spark, edgeDirectories, REQUIRED_EDGE_HEADERS);

        Dataset<Row> persistedEdgeDS;
        if (dfStorageLevel.isValid()) {
            persistedEdgeDS = unionEdgeDS.persist(dfStorageLevel);
            LOGGER.info("Storage Level for Edge dataset = {}", dfStorageLevel);
        } else {
            persistedEdgeDS = unionEdgeDS;
		}	

        // Sample out edge dataset to verify the inserts.
        final Dataset<Row> edgeDatasetsSample = persistedEdgeDS.sample(sampleFraction);

        // If the edge cache is disabled globally we do not need to search for supernodes.
        if (!Boolean.parseBoolean(ConfigurationHelper.getOrDefault(EDGE_CACHE_DISABLED_GLOBALLY, CONFIG))) {
            spark.sparkContext().setJobGroup("Compute Supernodes", "Compute Supernodes RDD operation", true);
            // Csv format is: ~id, ~from, ~to, ...
            DatasetOperations.extractSupernodes(persistedEdgeDS, CONFIG);
        }

        // Write to edge caches for non-supernodes.
        final Instant startOfEdgeMapPartitions = Instant.now();
        spark.sparkContext().setJobGroup("Edges write", "Edges MapPartition and collectAsList", true);
        final List<Long> edgeResult = DatasetOperations.writeEdges(configPath, persistedEdgeDS, ENV, finalS3BucketName);
        final Instant endOfEdgeMapPartitions = Instant.now();
        Duration edgeInterval = Duration.between(startOfEdgeMapPartitions, endOfEdgeMapPartitions);
        LOGGER.info("Execution time in seconds for Edge mapPartitions block: " + edgeInterval.getSeconds());

        final int totalEgdeDuration = edgeResult.stream().mapToInt(Math::toIntExact).sum();
        int noOfEdgePartitions = edgeResult.size();
        LOGGER.info("Mean time taken per Edge partition for " + noOfEdgePartitions + " partitions = " + totalEgdeDuration);

        // Verify edges.
        spark.sparkContext().setJobGroup("Verify Edges", "Verify Edges MapPartition", true);
        DatasetOperations.verifyEdges(configPath, edgeDatasetsSample, ENV, finalS3BucketName);

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
