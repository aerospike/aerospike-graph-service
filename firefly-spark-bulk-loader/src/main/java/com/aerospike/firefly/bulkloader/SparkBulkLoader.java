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
        try {
            if (ENV.equalsIgnoreCase("aws")) {
                s3BucketName = cmd.getOptionValue("b");
                assert s3BucketName != null;
                assert configPath != null;
                loader = S3ObjectLoader.getInstance();
                ((S3ObjectLoader)loader).setBucketName(s3BucketName);
            } else {
                final String defaultConfigPath = "conf/spark-bulk-loader-conf/config.properties";
                configPath = configPath == null ? defaultConfigPath : configPath;
                loader = FileLoader.getInstance();
            }
            CONFIG = loader.loadConfiguration(configPath);
            vertexDirectories.addAll(loader.getObjectList(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.VERTEX_DIRECTORY_KEY, CONFIG)));
            edgeDirectories.addAll(loader.getObjectList(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.EDGE_DIRECTORY_KEY, CONFIG)));
        }
        catch (final IOException ie) {
            LOGGER.error("Unable to load config." + ie.getMessage());
            ie.printStackTrace();
            System.exit(1);
        }
        catch (final RuntimeException runtimeException) {
            LOGGER.error("Amazon SDK client error" + runtimeException.getMessage());
            System.exit(1);
        }

        final double sampleFraction = Double.parseDouble(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.SAMPLING_PERCENTAGE, CONFIG)) / 100;

        // Initialize Spark
        final SparkConf conf = new SparkConf();
        setSparkConf(conf);

        final SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        Dataset<Row> unionVertexDS = DatasetOperations.loadAndMergeDatasets(spark, vertexDirectories, REQUIRED_VERTEX_HEADERS);
        // sample out vertex dataset for verifying the inserts
        Dataset<Row> sampledVertexDatasets = unionVertexDS.sample(sampleFraction);

        final String finalConfigPath = configPath;
        final String finalS3BucketName = s3BucketName;

        // Write Vertices
        final Instant startOfVertexMapPartitions = Instant.now();
        spark.sparkContext().setJobGroup("Vertex write", "Vertex MapPartition and collectAsList", true);

        List<Long> result = DatasetOperations.vertexWrite(unionVertexDS, finalConfigPath, ENV, finalS3BucketName);
        final Instant endOfVertexMapPartitions = Instant.now();
        Duration vertexInterval = Duration.between(startOfVertexMapPartitions, endOfVertexMapPartitions);
        LOGGER.info("Execution time in seconds for vertexMapPartitions mapPartitions block: " + vertexInterval.getSeconds());

        final int totalVertexDuration = result.stream().mapToInt(Math::toIntExact).sum();
        int noOfVertexPartitions = result.size();
        LOGGER.info("Mean time taken per Vertex partition for " + noOfVertexPartitions + " partitions = " + totalVertexDuration);

        // Verify vertices.
        spark.sparkContext().setJobGroup("Verify Vertex", "Verify vertex MapPartition", true);
        DatasetOperations.verifyVertices(sampledVertexDatasets, finalConfigPath, ENV, finalS3BucketName);

        // Load and Merge Edges
        Dataset<Row> unionEdgeDS = DatasetOperations.loadAndMergeDatasets(spark, edgeDirectories, REQUIRED_EDGE_HEADERS);
        Dataset<Row> persistedEdgeDS = unionEdgeDS.persist(StorageLevel.DISK_ONLY());
        //sample out edge dataset to verify the inserts
        Dataset<Row> edgeDatasetsSample = persistedEdgeDS.sample(sampleFraction);

        // If the edge cache is disabled globally we do not need to search for supernodes.
        if (!Boolean.parseBoolean(ConfigurationHelper.getOrDefault(EDGE_CACHE_DISABLED_GLOBALLY, CONFIG))) {
            spark.sparkContext().setJobGroup("Compute Supernodes", "Compute Supernodes RDD operation", true);
            // Csv format is: ~id, ~from, ~to, ...
            DatasetOperations.extractSupernodes(persistedEdgeDS, CONFIG);
        }

        // Write to edge caches for non-supernodes.
        final Instant startOfEdgeMapPartitions = Instant.now();
        spark.sparkContext().setJobGroup("Edges write", "Edges MapPartition and collectAsList", true);
        result = DatasetOperations.writeEdges(finalConfigPath, persistedEdgeDS, ENV, finalS3BucketName);
        final Instant endOfEdgeMapPartitions = Instant.now();
        Duration edgeInterval = Duration.between(startOfEdgeMapPartitions, endOfEdgeMapPartitions);
        LOGGER.info("Execution time in seconds for Edge mapPartitions block: " + edgeInterval.getSeconds());

        final int totalEgdeDuration = result.stream().mapToInt(Math::toIntExact).sum();
        int noOfEdgePartitions = result.size();
        LOGGER.info("Mean time taken per Edge partition for " + noOfEdgePartitions + " partitions = " + totalEgdeDuration);

        // Verify edges.
        spark.sparkContext().setJobGroup("Verify Edges", "Verify Edges MapPartition", true);
        DatasetOperations.verifyEdges(finalConfigPath, edgeDatasetsSample, ENV, finalS3BucketName);

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
