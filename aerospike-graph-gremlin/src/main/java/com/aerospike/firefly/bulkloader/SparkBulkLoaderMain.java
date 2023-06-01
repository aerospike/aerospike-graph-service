package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.spark.EdgeOperations;
import com.aerospike.firefly.bulkloader.spark.VertexOperations;
import com.aerospike.firefly.bulkloader.storage.FileLoader;
import com.aerospike.firefly.bulkloader.storage.HybridLoader;
import com.aerospike.firefly.bulkloader.storage.ObjectLoader;
import com.aerospike.firefly.bulkloader.storage.S3ObjectLoader;
import com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper;
import com.aerospike.firefly.bulkloader.util.ProgressBar;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;

import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.FILE_SYSTEM;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.SPARK_LOG_LEVEL;
import static com.aerospike.firefly.bulkloader.util.CommandLineParser.LOCAL_MODE;

public class SparkBulkLoaderMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderMain.class);
    private static final ProgressBar progressBar = new ProgressBar();
    private static final Timer progressBarTimer = new Timer();

    public static void main(final String[] args) {
        try {
            final CommandLine cmd = com.aerospike.firefly.bulkloader.util.CommandLineParser.parseCmdArgs(args);
            LOGGER.info("Command line input: {}", String.join(",", args));

            // Initialize Spark.
            String configPath = cmd.hasOption("c") ? cmd.getOptionValue("c") : null;
            Objects.requireNonNull(configPath);
            ObjectLoader loader = buildConfiguration(cmd);
        	Map<String, Object> fileConfig = loader.loadConfiguration(configPath);
        	LOGGER.info("aerospike.graphloader.config: " + fileConfig.toString());
        	final BulkLoaderConfigHelper config = new BulkLoaderConfigHelper(fileConfig, cmd);
            final SparkSession spark = buildSparkSession(config, cmd);

        	initializeProgressBar(fileConfig);

            // Vertex processing
            VertexOperations vo = null;
            try {
            	vo = new VertexOperations(config, loader.getCsvPaths(VertexOperations.getVertexDirectory(config)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Dataset<Row> vertexDataset = DatasetOperations.loadDataset(spark, vo.vertexPaths, VertexOperations.REQUIRED_VERTEX_HEADERS,
                    DatasetOperations.getDfStorageLevel(config));
            EdgeOperations edges = null;
            try {
            	edges = new EdgeOperations(config, loader.getCsvPaths(EdgeOperations.getEdgeDirectory(config)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            Dataset<Row> edgeDataset = DatasetOperations.
                    loadDataset(spark, edges.edgePaths, EdgeOperations.REQUIRED_EDGE_HEADERS, DatasetOperations.getDfStorageLevel(config));

            // Preflight check
        	DatasetOperations.preflightCheck(edgeDataset, vertexDataset, config);

            // Vertex processing
            progressBar.setVertexLoadStart();
            vo.writeVerticesToDB(vertexDataset);
            progressBar.setVertexLoadComplete();

            vo.verifySampleVerticesAfterWrite(vertexDataset.sample(DatasetOperations.getSamplingPercent(config)));
            progressBar.setVertexValidationComplete();
            vertexDataset.unpersist();

            // Edge processing
            edges.extractSupernodes(edgeDataset);
            progressBar.setSuperNodeExtractionComplete();

            progressBar.setEdgeLoadStart();
            edges.writeEdgeToDB(edgeDataset);
            progressBar.setEdgeLoadComplete();

            edges.verifySampleEdgeAfterWrite(edgeDataset.sample(DatasetOperations.getSamplingPercent(config)));
            progressBar.setEdgeValidationComplete();
            edgeDataset.unpersist();

            // Stop spark session
            spark.stop();
        } finally {
            progressBarTimer.cancel();
            progressBar.close();
        }
    }

    private static void initializeProgressBar(Map<String, Object> config) {
        try {
            // Once graph is set in progress bar, it will be used to update progress bar.
            // If graph fails to open for some reason, it will be null internally and progress bar will not report.
            progressBar.setGraph(FireflyGraph.open(new MapConfiguration(config)));
            progressBarTimer.scheduleAtFixedRate(progressBar, 0, 10000);
        } catch (final Exception e) {
            LOGGER.warn("Failed to start progress bar", e);
        }
    }

    public static ObjectLoader buildConfiguration(final CommandLine cmd) {
        final String env = cmd.hasOption("e") ? cmd.getOptionValue("e") : "";
        final String fileSystem = cmd.hasOption("fs") ? cmd.getOptionValue("fs") : "";
        final ObjectLoader loader;
        if (env.equalsIgnoreCase("aws")) {
            final String s3BucketName = cmd.getOptionValue("md");
            if (s3BucketName == null) {
                throw new RuntimeException("Failed to start bulk loader due to no specified S3 Bucket Name");
            }
            final S3ObjectLoader s3Loader = S3ObjectLoader.getInstance();
            s3Loader.setBucketName(s3BucketName);
            loader = s3Loader;
        } else {
            final ObjectLoader configLoader = FileLoader.getInstance();
            if (fileSystem.equalsIgnoreCase("s3")) {
                final String s3BucketName = cmd.getOptionValue("md");
                if (s3BucketName == null) {
                    throw new RuntimeException("Failed to start bulk loader due to no specified S3 Bucket Name");
                }
                final S3ObjectLoader csvLoader = S3ObjectLoader.getInstance();
                csvLoader.setBucketName(s3BucketName);
                HybridLoader.setInstance(configLoader, csvLoader);
                loader = HybridLoader.getInstance();
            } else {
                loader = configLoader;
            }
        }
        return loader;
    }

    private static SparkSession buildSparkSession(final BulkLoaderConfigHelper config, final CommandLine cmd) {
        Objects.requireNonNull(config);
        SparkConf conf = new SparkConf();
        final String fileSystem = config.getOrDefault(FILE_SYSTEM);
        if (cmd.hasOption(LOCAL_MODE)) {
            conf.setMaster("local[*]");
        }

        conf.setAppName("aerospike-graph-bulk-loader")
                .set("spark.driver.allowMultipleContexts", "false")
                .set("spark.ui.enabled", "true")
                .set("mapreduce.fileoutputcommitter.algorithm.version", "2");

        final SparkSession.Builder builder = SparkSession.builder().config(conf);
        if (fileSystem.equalsIgnoreCase("s3")) {
            builder.config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            if (cmd.hasOption("u")) {
                builder.config("spark.hadoop.fs.s3a.access.key", cmd.getOptionValue("u"));
            }
            if (cmd.hasOption("p")) {
                builder.config("spark.hadoop.fs.s3a.secret.key", cmd.getOptionValue("p"));
            }
        }
        final SparkSession spark = builder.getOrCreate();
        final String logLevel = config.getOrDefault(SPARK_LOG_LEVEL).toUpperCase();
        // Set LOG LEVEL for spark logging to disable logging of each step during debugging purposes.
        spark.sparkContext().setLogLevel(logLevel);
        return spark;
    }

    public static void exponentialBackoff(final int attempt) {
        // This is to prevent overflow since we cap at 10000ms anyway.
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
