package com.aerospike.firefly.olap;

import com.aerospike.firefly.olap.helper.CommandLineParser;
import com.aerospike.firefly.runtime.FireflyServer;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.aerospike.firefly.olap.helper.CommandLineParser.CONFIG_DIRECTORY_KEY;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DistributedGraphComputerMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedGraphComputerMain.class);
    public static final String LOCAL = "local";
    public static final String S3 = "s3";
    public static final String GCS = "gcs";
    static String fileSystem = LOCAL;

    // Public static variable to be used by test to shut down the server.
    public static FireflyServer fireflyServerForTesting = null;


    public static final String LOCAL_MODE = "local";
    public static final String GCS_KEYFILE_DIRECTORY = "aerospike.graphloader.gcs-keyfile";
    public static final String GCS_EMAIL = "aerospike.graphloader.gcs-email";
    public static final String REMOTE_USERNAME = "aerospike.graphloader.remote-user";
    public static final String REMOTE_PASSKEY = "aerospike.graphloader.remote-passkey";


    public static void main(final String[] args) {

        // Create new Object so we can invoke non-static method load()
        LOGGER.info("Starting Aerospike Graph OLAP Server");

        System.out.println();
        System.out.println("              \\,,,/    \\,,,/    \\,,,/    \\,,,/    \\,,,/");
        System.out.println("              (o o)    (o o)    (o o)    (o o)    (o o)");
        System.out.println("          o00o-(3)-oOOo-(3)-oOOo-(3)-oOOo-(3)-oOOo-(3)-o00o");
        System.out.println("              \\,,,/            /\\   /\\           \\,,,/");
        System.out.println("              (o o)           {  o o  }           (o o)  ");
        System.out.println("          o00o-(3)-oO0o       (   ^   )       o00o-(3)-o00o");
        System.out.println("              \\,,,/           |  ===  |           \\,,,/");
        System.out.println("              (o o)          /|  \\_/ |\\           (o o)  ");
        System.out.println("          o00o-(3)-oO0o     (_|  |_|  |_)     o00o-(3)-o00o");
        System.out.println("              \\,,,/           |  | |  |           \\,,,/");
        System.out.println("              (o o)           |  | |  |           (o o)  ");
        System.out.println("          o00o-(3)-oO0o      (___| |___)      o00o-(3)-o00o");
        System.out.println("              \\,,,/    \\,,,/    \\,,,/    \\,,,/    \\,,,/");
        System.out.println("              (o o)    (o o)    (o o)    (o o)    (o o)");
        System.out.println("          o00o-(3)-oOOo-(3)-oOOo-(3)-oOOo-(3)-oOOo-(3)-o00o");
        System.out.println("                                                                    ");
        System.out.println("       +-----------------------------------------------------+      ");
        System.out.println("       |              AEROSPIKE GRAPH OLAP                   |      ");
        System.out.println("       +-----------------------------------------------------+      ");
        System.out.println("                   |                             |                  ");
        System.out.println("        +----------------------+        +-------------------+       ");
        System.out.println("        |     APACHE SPARK     |<------>|     TINKERPOP     |       ");
        System.out.println("        +----------------------+        +-------------------+       ");
        System.out.println();

        final CommandLine commandLine = CommandLineParser.parseCmdArgs(args);
        if (commandLine.getOptionValue("c") == null) {
            LOGGER.error("Configuration file is required. Please provide a configuration file using the -c option.");
            System.exit(1);
        }

        final String pythonScriptPath = readJarFileWriteToTemp("scripts", "configure_aerospike_graph.py");
        final String serverYamlPath = readJarFileWriteToTemp("scripts", "flattened-default-gremlin-server.yaml");
        final String tempDirectory = System.getProperty("java.io.tmpdir") + File.separator + "aerospike-graph-olap" + File.separator;
        final String outputServerYaml = tempDirectory + "gremlin-server.yaml";

        final SparkSession spark = buildSparkSession(commandLine);
        final String configPath = commandLine.hasOption("c") ? commandLine.getOptionValue("c") : null;
        configureFileSystem(spark, commandLine, commandLine.getOptionValue(CONFIG_DIRECTORY_KEY));
        final Map<String, Object> fileConfig = loadConfiguration(spark, commandLine, configPath);

        // Write config file to a file so python can configure it.
        final String configFilePath = tempDirectory + "aerospike-graph.properties";
        final StringBuilder configMapToFile = new StringBuilder();
        for (final Map.Entry<String, Object> entry : fileConfig.entrySet()) {
            configMapToFile.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
        }
        configMapToFile.append(ConfigurationHelper.Keys.OLAP_ENABLED).append("=").append("true\n");
        configMapToFile.append(ConfigurationHelper.Keys.AUTO_PRE_HEAT).append("=").append("false\n");
        try {
            FileUtils.writeStringToFile(new File(configFilePath), configMapToFile.toString(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            LOGGER.error("Failed to write config file to file for configuration: " + configFilePath, e);
            System.exit(1);
        }

        // Invoke python
        try {
            final ProcessBuilder processBuilder = new ProcessBuilder("python3",
                    pythonScriptPath,
                    configFilePath,
                    serverYamlPath,
                    outputServerYaml,
                    tempDirectory,
                    tempDirectory + "/java_options.txt");
            final Process process = processBuilder.start();
            final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            LOGGER.error("Failed to invoke python script for configuration.", e);
            System.exit(1);
        }
        int workers = spark.sparkContext().getExecutorMemoryStatus().size();
        System.out.println("Workers: " + workers);
        System.out.println("Memory: " + spark.sparkContext().getExecutorMemoryStatus());
        System.out.println("Configuration: " + Arrays.toString(spark.sparkContext().getConf().getAll()));

        FireflyServer.setSpark(spark);
        fireflyServerForTesting = FireflyServer.start(List.of(outputServerYaml).toArray(new String[]{}));
    }

    private static String readJarFileWriteToTemp(final String directory, final String fileName) {
        final InputStream in = DistributedGraphComputerMain.class.getClassLoader().getResourceAsStream(directory + "/" + fileName);
        if (in == null) {
            LOGGER.error("Failed to find file '" + directory + "/" + fileName + "' script in the OLAP jar. Please contact support.");
            System.exit(1);
        }

        File tempScript = null;
        try {
            tempScript = File.createTempFile(fileName.split("\\.")[0], "." + fileName.split("\\.")[1]);
            if (!tempScript.setExecutable(true)) {
                throw new IllegalStateException("Unable to set executable permission for temporary script file: " + tempScript.getAbsolutePath());
            }
            tempScript.deleteOnExit();
        } catch (final IOException e) {
            LOGGER.error("Failed to create temporary file for configuration script.", e);
            System.exit(1);
        }

        try (final OutputStream out = new FileOutputStream(tempScript)) {
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        } catch (final IOException e) {
            LOGGER.error("Failed to read temporary configuration script file.", e);
            System.exit(1);
        }

        final String pythonScriptPath = tempScript.getAbsolutePath();
        final File file = new File(pythonScriptPath);
        if (!file.exists()) {
            // Note in intellij debugger, these wont exist. Need to run through maven.
            LOGGER.error("Failed to find temp file after writing for '" + directory + "/" + fileName + "'. Please contact support.");
            System.exit(1);
        }
        return pythonScriptPath;
    }

    /**
     * Configures the Spark Session to have the right configurations for interacting with different file systems.
     *
     * @param spark Spark session
     * @param cmd   Command line args
     * @param uri   URI to check before reading
     */
    public static void configureFileSystem(final SparkSession spark, final CommandLine cmd, final String uri) {
        final String uriFileSystem = getFileSystem(uri);
        if (fileSystem.equals(uriFileSystem)) {
            // Don't need to do anything if file system did not change.
            return;
        } else if (fileSystem.equals(LOCAL)) {
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
                                GCS_EMAIL + "', '" + REMOTE_USERNAME + "', and '" + REMOTE_PASSKEY +
                                "' must be specified to read from GCS.";
                        LOGGER.error(gcsCredentialError);
                        throw new RuntimeException(gcsCredentialError);
                    }
                }
            }
        } else {
            LOGGER.error("Cannot change file system from '" + fileSystem + "' to '" + uriFileSystem + "'.");
            throw new RuntimeException("Cannot change file system from '" + fileSystem + "' to '" + uriFileSystem + "'.");
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

    private static SparkSession buildSparkSession(final CommandLine cmd) {
        SparkConf conf = new SparkConf();
        if (cmd.hasOption(LOCAL_MODE)) {
            conf.setMaster("local[*]");
        }

        conf.setAppName("aerospike-graph-olap")
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

    private static Map<String, Object> loadConfiguration(final SparkSession spark, final CommandLine cmd,
                                                         final String configPath) {
        configureFileSystem(spark, cmd, configPath);
        LOGGER.debug("Configuration uri:" + configPath);

        final String fileContext = spark.read().option("wholetext", true).text(configPath).collectAsList().get(0)
                .getString(0);

        final Properties prop = new Properties();
        try (final StringReader reader = new StringReader(fileContext)) {
            prop.load(reader);
        } catch (final IOException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        }
        final Map<String, Object> config = new MapConfiguration(prop).getMap();
        config.put(ConfigurationHelper.Keys.OLAP_ENABLED.toLowerCase(), "true");
        return config;
    }
}
