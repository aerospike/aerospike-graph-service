package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.spark.EdgeOperations;
import com.aerospike.firefly.bulkloader.spark.VertexOperations;
import com.aerospike.firefly.bulkloader.storage.FileLoader;
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

public class SparkBulkLoaderMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderMain.class);
    private static final ProgressBar progressBar = new ProgressBar();
    private static final Timer progressBarTimer = new Timer();

    public static void main(final String[] args) {

        final CommandLine cmd = com.aerospike.firefly.bulkloader.util.CommandLineParser.parseCmdArgs(args);
        LOGGER.info("Command line input: {}", String.join(",", args));

        // Initialize Spark.
        String configPath = cmd.hasOption("c") ? cmd.getOptionValue("c") : null;
        Objects.requireNonNull(configPath);
        ObjectLoader loader = buildConfiguration(cmd);
        Map<String, Object> config = loader.loadConfiguration(configPath);
        LOGGER.info("config: " + config.toString());
        final SparkSession spark = buildSparkSession(config, cmd);

        initializeProgressBar(config);

        //vertex processing
        VertexOperations vo = null;
        try {
            vo = new VertexOperations(cmd, config, loader.getCsvPaths(VertexOperations.getVertexDirectory(config)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Dataset<Row> vertexDataset = DatasetOperations.loadDataset(spark, vo.vertexPaths, VertexOperations.REQUIRED_VERTEX_HEADERS,
                DatasetOperations.getDfStorageLevel(config));
        vo.dryRunVertices(vertexDataset);


        vo.writeVerticesToDB(vertexDataset);
        progressBar.setVertexLoadComplete();

        vo.verifySampleVerticesAfterWrite(vertexDataset.sample(DatasetOperations.getSamplingPercent(config)));
        progressBar.setVertexValidationComplete();

        vertexDataset.unpersist();

        //edge processing
        EdgeOperations edges = null;
        try {
            edges = new EdgeOperations(cmd, config, loader.getCsvPaths(EdgeOperations.getEdgeDirectory(config)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Dataset<Row> edgeDataset = DatasetOperations.
                loadDataset(spark, edges.edgePaths, EdgeOperations.REQUIRED_EDGE_HEADERS, DatasetOperations.getDfStorageLevel(config));

        edges.dryRunEdges(edgeDataset);
        progressBar.setVertexValidationComplete();

        edges.extractSupernodes(edgeDataset);
        progressBar.setSuperNodeExtractionComplete();

        edges.writeEdgeToDB(edgeDataset);
        progressBar.setEdgeLoadComplete();

        edges.verifySampleEdgeAfterWrite(edgeDataset.sample(DatasetOperations.getSamplingPercent(config)));
        progressBar.setEdgeValidationComplete();
        edgeDataset.unpersist();

        // Stop spark session
        spark.stop();
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
        final String ENV = cmd.hasOption("e") ? cmd.getOptionValue("e") : "";
        ObjectLoader loader;
        if (ENV.equalsIgnoreCase("aws")) {
            String s3BucketName = cmd.getOptionValue("b");
            if (s3BucketName == null) {
                throw new RuntimeException("Failed to start bulk loader due to null s3BucketName (" + s3BucketName + ")");
            }
            loader = S3ObjectLoader.getInstance();
            ((S3ObjectLoader) loader).setBucketName(s3BucketName);
        } else {
            loader = FileLoader.getInstance();
        }
        return loader;
    }

    private static SparkSession buildSparkSession(final Map<String, Object> CONFIG, final CommandLine cmd) {
        Objects.requireNonNull(CONFIG);
        SparkConf conf = new SparkConf();
        String MODE = "cluster";
        MODE = cmd.hasOption("m") ? cmd.getOptionValue("m") : MODE;
        if (MODE.equals("local"))
            conf.setMaster("local[*]");

        conf.setAppName("firefly-bulk-loader")
                .set("spark.driver.allowMultipleContexts", "false")
                .set("spark.ui.enabled", "true")
                .set("mapreduce.fileoutputcommitter.algorithm.version", "2");

        final SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
        final String SPARK_LOG_LEVEL = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.SPARK_LOG_LEVEL, CONFIG).toUpperCase();
        // Set LOG LEVEL for spark logging to disable logging of each step during debugging purposes.
        spark.sparkContext().setLogLevel(SPARK_LOG_LEVEL);
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
