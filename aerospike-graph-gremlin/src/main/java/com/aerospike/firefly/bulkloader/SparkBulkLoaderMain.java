package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.bulkloader.exception.FireflyBulkLoaderException;
import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.spark.EdgeOperations;
import com.aerospike.firefly.bulkloader.spark.VertexOperations;
import com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper;
import com.aerospike.firefly.bulkloader.util.ProgressBar;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.stream.Collectors;

import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.EDGE_DIRECTORY_KEY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.SPARK_LOG_LEVEL;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.VERTEX_DIRECTORY_KEY;
import static com.aerospike.firefly.bulkloader.util.CommandLineParser.LOCAL_MODE;

public class SparkBulkLoaderMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderMain.class);
    private static final String LOCAL = "local";
    private static final String S3 = "s3";
    private static final String GCS = "gcs";
    private static String FILE_SYSTEM;
    private static ProgressBar PROGRESS_BAR;
    private static Timer PROGRESS_BAR_TIMER;
    private static final int DRYRUN_STACKTRACE_LIMIT = 5;

    public static void main(final String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down DatasetOperations executor service");
            DatasetOperations.getScheduledThreadPoolService().shutdown();
        }));
        try {
            final CommandLine cmd = com.aerospike.firefly.bulkloader.util.CommandLineParser.parseCmdArgs(args);
            final List<String> printableArgs = new ArrayList();
            String previous = "";
            for (final String current : args) {
                if (previous.equals("-p") || previous.equals("-aerospike.graphloader.remote-passkey")) {
                    char[] censoredPass = new char[current.length()];
                    Arrays.fill(censoredPass, '*');
                    printableArgs.add(new String(censoredPass));
                } else {
                    printableArgs.add(current);
                }
                previous = current;
            }
            LOGGER.info("Command line input: {}", String.join(", ", printableArgs));

            // new Timer(true) creates the timer as a daemon, which means that it will not prevent the JVM from exiting.
            PROGRESS_BAR = new ProgressBar();
            PROGRESS_BAR_TIMER = new Timer(true);

            // Initialize Spark.
            FILE_SYSTEM = LOCAL;
            final SparkSession spark = buildSparkSession(cmd);
            final String configPath = cmd.hasOption("c") ? cmd.getOptionValue("c") : null;
            Objects.requireNonNull(configPath);
            final Map<String, Object> fileConfig = loadConfiguration(spark, cmd, configPath);
            LOGGER.info("Config: " + fileConfig.toString());
            final BulkLoaderConfigHelper config = new BulkLoaderConfigHelper(fileConfig, cmd);

            final String logLevel = config.getOrDefault(SPARK_LOG_LEVEL).toUpperCase();
            // Set LOG LEVEL for spark logging to disable logging of each step during debugging purposes.
            spark.sparkContext().setLogLevel(logLevel);

            // Pre-processing
            final List<String> vertexDirectories = getDirectories(spark, cmd, config.getOrDefault(VERTEX_DIRECTORY_KEY));
            final VertexOperations vertexOperations = new VertexOperations(config, vertexDirectories);
            final Dataset<Row> vertexDataset = DatasetOperations.loadDataset(spark, vertexDirectories,
                    VertexOperations.REQUIRED_VERTEX_HEADERS, DatasetOperations.getDfStorageLevel(config));

            final List<String> edgeDirectories = getDirectories(spark, cmd, config.getOrDefault(EDGE_DIRECTORY_KEY));
            final EdgeOperations edgeOperations = new EdgeOperations(config, edgeDirectories);
            final Dataset<Row> edgeDataset = DatasetOperations.loadDataset(spark, edgeDirectories,
                    EdgeOperations.REQUIRED_EDGE_HEADERS, DatasetOperations.getDfStorageLevel(config));

            initializeProgressBar(fileConfig);

            // Preflight check
            try {
                DatasetOperations.preflightCheck(edgeDataset, vertexDataset, config);
            } catch (final Exception e) {
                // We are limiting stacktrace size by DRYRUN_STACKTRACE_LIMIT
                StackTraceElement[] originalStackTrace = e.getStackTrace();
                StackTraceElement[] limitedStackTrace =
                    Arrays.copyOf(originalStackTrace, Math.min(originalStackTrace.length, DRYRUN_STACKTRACE_LIMIT));
                e.setStackTrace(limitedStackTrace);
                throw e;
            }
			PROGRESS_BAR.setPreflightCheckComplete();

            // Supernode processing
            final Set<Object> supernodes = edgeOperations.extractSupernodes(edgeDataset);
            PROGRESS_BAR.setSuperNodeExtractionComplete();

            // Vertex processing
            PROGRESS_BAR.setVertexLoadStart();
            vertexOperations.writeVerticesToDB(vertexDataset, supernodes);
            PROGRESS_BAR.setVertexLoadComplete();

            vertexOperations.verifySampleVerticesAfterWrite(vertexDataset.sample(DatasetOperations.getSamplingPercent(config)));
            PROGRESS_BAR.setVertexValidationComplete();
            vertexDataset.unpersist();

            // Edge processing
            PROGRESS_BAR.setEdgeLoadStart();
            edgeOperations.writeEdgeToDB(edgeDataset);
            PROGRESS_BAR.setEdgeLoadComplete();

            edgeOperations.verifySampleEdgeAfterWrite(edgeDataset.sample(DatasetOperations.getSamplingPercent(config)));
            PROGRESS_BAR.setEdgeValidationComplete();
            edgeDataset.unpersist();

            // Stop spark session
            spark.stop();
        } finally {
            PROGRESS_BAR_TIMER.cancel();
            PROGRESS_BAR.close();
        }
    }

    private static void initializeProgressBar(Map<String, Object> config) {
        try {
            // Once graph is set in progress bar, it will be used to update progress bar.
            // If graph fails to open for some reason, it will be null internally and progress bar will not report.
            PROGRESS_BAR.setGraph(FireflyGraph.open(new MapConfiguration(config)));
            PROGRESS_BAR_TIMER.scheduleAtFixedRate(PROGRESS_BAR, 0, 10000);
        } catch (final Exception e) {
            LOGGER.warn("Failed to start progress bar", e);
        }
    }

    private static SparkSession buildSparkSession(final CommandLine cmd) {
        SparkConf conf = new SparkConf();
        if (cmd.hasOption(LOCAL_MODE)) {
            conf.setMaster("local[*]");
        }

        conf.setAppName("aerospike-graph-bulk-loader")
                .set("spark.driver.allowMultipleContexts", "false")
                .set("spark.ui.enabled", "true")
                .set("mapreduce.fileoutputcommitter.algorithm.version", "2");

        final SparkSession.Builder builder = SparkSession.builder().config(conf);
        builder.config("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .config("google.cloud.auth.service.account.enable", true);
        return builder.getOrCreate();
    }

    private static Map<String, Object> loadConfiguration(final SparkSession spark, final CommandLine cmd,
                                                         final String configPath) {
        configureFileSystem(spark, cmd, configPath);
        LOGGER.debug("Configuration uri:" + configPath);

        final String fileContext = spark.read().option("wholetext", true).text(configPath).collectAsList().get(0)
                .getString(0);
        LOGGER.debug("Configuration content: " + fileContext);

        final Properties prop = new Properties();
        try (final StringReader reader = new StringReader(fileContext)) {
            prop.load(reader);
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        }
        return new MapConfiguration(prop).getMap();
    }

    private static List<String> getDirectories(final SparkSession spark, final CommandLine cmd, final String directory) {
        configureFileSystem(spark, cmd, directory);

        final Dataset<Row> directories = spark.read().format("csv").option("recursiveFileLookup", "true")
                .load(directory)
                .withColumn( "~temp", org.apache.spark.sql.functions.input_file_name())
                .select("~temp")
                .withColumnRenamed("~temp", "fname")
                .withColumn("fname", functions.expr("substring(fname, 1, length(fname) - length(substring_index(fname, '/', -1)))")) // Extract directory
                .distinct();

        final List<String> directoryPaths = directories.collectAsList().stream().map( row -> row.get(0).toString()).collect(Collectors.toList());
        LOGGER.info("CSV directories: {}", String.join(", ", directoryPaths));
        return directoryPaths;
    }

    /**
     * Configures the Spark Session to have the right configurations for interacting with different file systems.
     * @param spark Spark session
     * @param cmd   Command line args
     * @param uri   URI to check before reading
     */
    private static void configureFileSystem(final SparkSession spark, final CommandLine cmd, final String uri) {
        // File system always starts off as local since local config and cloud storage for CSV is an allowed combination.
        //
        // This also means the only time changing the file system is allowed is from local to something else, which also
        // means that it should only be configured one time.
        final String uriFileSystem = getFileSystem(uri);
        if (FILE_SYSTEM.equals(uriFileSystem)) {
            // Don't need to do anything if file system did not change.
            return;
        } else if (FILE_SYSTEM.equals(LOCAL)) {
            LOGGER.info("Remote file system detected. Changing to '" + uriFileSystem + "' mode.");
            FILE_SYSTEM = uriFileSystem;
            if (FILE_SYSTEM.equals(S3)) {
                if (cmd.hasOption("u")) {
                    spark.conf().set("fs.s3a.access.key", cmd.getOptionValue("u").trim());
                }
                if (cmd.hasOption("p")) {
                    spark.conf().set("fs.s3a.secret.key", cmd.getOptionValue("p").trim());
                }
            } else if (uriFileSystem.equals(GCS)) {
                if (cmd.hasOption("gck")) {
                    final String keyFilePath = cmd.getOptionValue("gck");
                    LOGGER.info("Google Cloud Service Account key file specified: " + keyFilePath);
                    spark.conf().set("google.cloud.auth.service.account.json.keyfile", keyFilePath);
                } else if (cmd.hasOption("u") && cmd.hasOption("p") && cmd.hasOption("gem")) {
                    LOGGER.info("Google Cloud Service credentials passed in directly.");
                    spark.conf().set("fs.gs.auth.service.account.private.key.id", cmd.getOptionValue("u").trim());
                    spark.conf().set("fs.gs.auth.service.account.private.key", cmd.getOptionValue("p").trim());
                    spark.conf().set("fs.gs.auth.service.account.email", cmd.getOptionValue("gem").trim());
                } else {
                    // Credentials are only necessary in JVM/Local mode.
                    if (cmd.hasOption(LOCAL_MODE)) {
                        final String gcsCredentialError = "Either 'aerospike.graphloader.gcs-keyfile' or all of 'aerospike.graphloader.gcs-email', 'aerospike.graphloader.remote-user', and 'aerospike.graphloader.remote-passkey' must be specified to read from GCS.";
                        LOGGER.error(gcsCredentialError);
                        throw new FireflyBulkLoaderException(gcsCredentialError);
                    }
                }
            }
        } else {
        }
    }

    private static String getFileSystem(final String uri) {
        if (uri.toLowerCase().startsWith("s3://")) {
            return S3;
        } else if (uri.toLowerCase().startsWith("gs://")) {
            return GCS;
        } else {
            return LOCAL;
        }
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
