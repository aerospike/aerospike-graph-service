package com.aerospike.firefly.bulkloader.statemachine.machine;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.spark.EdgeOperations;
import com.aerospike.firefly.bulkloader.spark.VertexOperations;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderState;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateDone;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateStart;
import com.aerospike.firefly.bulkloader.util.ProgressBar;
import com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper;
import com.aerospike.firefly.process.call.bulkload.utils.CommandLineParser;
import com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyBulkLoaderException;
import com.aerospike.firefly.runtime.HttpServer;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.DATABASE_NOT_EMPTY;
import static com.aerospike.firefly.process.call.bulkload.BulkLoaderServiceLoad.BULK_LOAD_SUCCESS;
import static com.aerospike.firefly.process.call.bulkload.BulkLoaderServiceLoad.formatErrorCount;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.CONFIG_DIRECTORY_KEY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.DISABLE_EDGE_WRITE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.DISABLE_VERTEX_WRITE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.EDGE_DIRECTORY_KEY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.GCS_EMAIL;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.GCS_KEYFILE_DIRECTORY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.INCREMENTAL_LOAD;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.LOCAL_MODE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.REMOTE_PASSKEY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.REMOTE_USERNAME;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.SPARK_LOG_LEVEL;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.TEMP_DIRECTORY_KEY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.VERTEX_DIRECTORY_KEY;

public class SparkBulkLoaderStateMachine {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderStateMachine.class);
    public static final String LOCAL = "local";
    public static final String S3 = "s3";
    public static final String GCS = "gcs";
    public String fileSystem;
    public boolean fileSystemMutable;
    public ProgressBar progressBar;
    public Timer progressBarTimer;
    public final int progressBarIntervalMs = 10000;
    public boolean isL2Mode;
    public List<String> vertexDirectories;
    public List<String> edgeDirectories;
    public boolean incrementalLoad;
    public BulkLoaderConfigHelper config;
    public SparkSession spark;
    public VertexOperations vertexOperations;
    public Dataset<Row> vertexDataset;
    public EdgeOperations edgeOperations;
    public Dataset<Row> edgeDataset;
    public CommandLine cmd;
    public Map<String, Object> fileConfig;
    public FireflyGraph initializerGraph;
    public Set<Object> supernodes;
    public Dataset<Row> persistedEdgeIdDataset;
    public Integer edgePartitionCount;
    public Integer vertexPartitionCount;

    public SparkBulkLoaderStateMachine(final String[] args) {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Shutting down DatasetOperations executor service");
                DatasetOperations.getScheduledThreadPoolService().shutdown();
            }));

            isL2Mode = false;

            cmd = CommandLineParser.parseCmdArgs(args);
            final List<String> printableArgs = new ArrayList();
            String previous = "";
            for (final String current : args) {
                if (previous.equals("-p") || previous.equals("-" + REMOTE_PASSKEY)) {
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
            progressBar = new ProgressBar(progressBarIntervalMs);
            progressBar.setIsL2Mode(cmd.hasOption(LOCAL_MODE));
            progressBarTimer = new Timer(true);

            // Initialize Spark.
            fileSystem = LOCAL;
            fileSystemMutable = true;
            spark = buildSparkSession(cmd);
            final String configPath = cmd.hasOption("c") ? cmd.getOptionValue("c") : null;
            isL2Mode = cmd.hasOption(LOCAL_MODE);
            Objects.requireNonNull(configPath);
            fileConfig = loadConfiguration(spark, cmd, configPath);
            LOGGER.info("Config: " + fileConfig);
            config = new BulkLoaderConfigHelper(fileConfig, cmd);

            final String logLevel = config.getOrDefault(SPARK_LOG_LEVEL).toUpperCase();
            // Set LOG LEVEL for spark logging to disable logging of each step during debugging purposes.
            spark.sparkContext().setLogLevel(logLevel);

            initializerGraph = FireflyGraph.open(config.getFireflyConfig());
            if (!initializerGraph.isEmpty() && !config.hasAction(DISABLE_EDGE_WRITE) &&
                    !config.hasAction(DISABLE_VERTEX_WRITE) && !config.hasAction(INCREMENTAL_LOAD)) {
                // If we're doing partial writing checking the emptiness of the database isn't valid.
                LOGGER.error(DATABASE_NOT_EMPTY);
                throw new RuntimeException(DATABASE_NOT_EMPTY);
            }
            initializerGraph.getBaseGraph().initialzeBulkLoadMetadata();

            // Pre-processing
            vertexDirectories = getDirectories(spark, cmd, config.getOrDefault(VERTEX_DIRECTORY_KEY));

            // FILE_SYSTEM cannot be mutated after vertex directory filesystem is checked
            fileSystemMutable = false;

            edgeDirectories = getDirectories(spark, cmd, config.getOrDefault(EDGE_DIRECTORY_KEY));
            incrementalLoad = false;
            if (config.hasAction(INCREMENTAL_LOAD)) {
                LOGGER.info("Incremental load mode detected.");
                incrementalLoad = true;
            }
            progressBar.initialize(initializerGraph, incrementalLoad);
        } catch (final Exception e) {
            LOGGER.error("Failed to initialize SparkBulkLoaderStateMachine", e);
            cleanup();
            throw e;
        }
    }

    public void cleanup() {
        synchronized (SparkBulkLoaderStateMachine.class) {
            // Only close the HTTP server in L3. Not L2.
            if (spark != null) {
                spark.sparkContext().stop();
                spark = null;
            }
            if (progressBarTimer != null) {
                progressBarTimer.cancel();
                progressBarTimer = null;
            }
            if (initializerGraph != null) {
                initializerGraph.close();
                initializerGraph = null;
            }
            if (!isL2Mode) {
                HttpServer.close();
            }
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

        // Internal use configurations
        if (cmd.hasOption("s3e")) {
            builder.config("fs.s3a.endpoint", cmd.getOptionValue("s3e")).config("fs.s3a.connection.ssl.enabled", "false");
        }

        return builder.getOrCreate();
    }

    private Map<String, Object> loadConfiguration(final SparkSession spark, final CommandLine cmd,
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
        final Map<String, Object> config = new MapConfiguration(prop).getMap();
        config.put(ConfigurationHelper.Keys.BULK_LOADER_FLAG.toLowerCase(), "true");
        return config;
    }

    private List<String> getDirectories(final SparkSession spark, final CommandLine cmd, final String directory) {
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
    public void configureFileSystem(final SparkSession spark, final CommandLine cmd, final String uri) {
        // File system always starts off as local since local config and cloud storage for CSV is an allowed combination.
        //
        // This also means the only time changing the file system is allowed is from local to something else, which also
        // means that it should only be configured one time.
        // FILE_SYSTEM_MUTABLE is set to false once the vertex directory is checked for its file system.
        final String uriFileSystem = getFileSystem(uri);
        if (fileSystem.equals(uriFileSystem)) {
            // Don't need to do anything if file system did not change.
            return;
        } else if (fileSystem.equals(LOCAL) && fileSystemMutable) {
            LOGGER.info("Remote file system detected. Changing to '" + uriFileSystem + "' mode.");
            fileSystem = uriFileSystem;
            if (fileSystem.equals(S3)) {
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
                        final String gcsCredentialError = "Either '" + GCS_KEYFILE_DIRECTORY + "' or all of '" +
                                GCS_EMAIL+ "', '" + REMOTE_USERNAME + "', and '" + REMOTE_PASSKEY +
                                "' must be specified to read from GCS.";
                        LOGGER.error(gcsCredentialError);
                        throw new FireflyBulkLoaderException(gcsCredentialError);
                    }
                }
            }
        } else {
            throw new IllegalArgumentException("Multiple file systems detected for one or more parameters: '" +
                    CONFIG_DIRECTORY_KEY + "', '" + VERTEX_DIRECTORY_KEY + "', '" +
                    EDGE_DIRECTORY_KEY + "', '" + TEMP_DIRECTORY_KEY +
                    "'. Cross-platform is not supported in a single bulk load.");
        }
    }

    private String getFileSystem(final String uri) {
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
