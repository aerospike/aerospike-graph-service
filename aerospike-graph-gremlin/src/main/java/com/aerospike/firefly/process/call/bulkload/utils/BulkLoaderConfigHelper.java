package com.aerospike.firefly.process.call.bulkload.utils;

import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class BulkLoaderConfigHelper implements Serializable {
    // ==CommandLine Configurations==
    // Flag indicating that the job is running from IDE/JVM.
    public static final String LOCAL_MODE = "local";
    // Directory containing config file.
    public static final String CONFIG_DIRECTORY_KEY = "aerospike.graphloader.config";
    // Username/ID credential for cloud storage. Optional if local.
    public static final String REMOTE_USERNAME = "aerospike.graphloader.remote-user";
    // Password/Key/Secret credential for cloud storage. Optional if local.
    public static final String REMOTE_PASSKEY = "aerospike.graphloader.remote-passkey";

    // ==Google Cloud specific configurations (CommandLine ONLY)==
    // Local-only path to Google Cloud key file for the Google Service Account.
    public static final String GCS_KEYFILE_DIRECTORY = "aerospike.graphloader.gcs-keyfile";
    // Email of the Google Service Account.
    public static final String GCS_EMAIL = "aerospike.graphloader.gcs-email";

    // ==Configurations shared with config file==
    // Directory containing the Vertex CSV files.
    public static final String VERTEX_DIRECTORY_KEY = "aerospike.graphloader.vertices";
    // Directory containing the Edge CSV files.
    public static final String EDGE_DIRECTORY_KEY = "aerospike.graphloader.edges";
    // Directory where temporary data is written to for bulk loading.
    public static final String TEMP_DIRECTORY_KEY = "aerospike.graphloader.temp-directory";
    // Boolean value to determine whether to keep provided ID values for Edges as a Property on the Edge.
    public static final String KEEP_PROVIDED_EDGE_ID_AS_PROPERTY = "aerospike.graphloader.keep-provided-edge-id-as-property";
    // The key of the property to store the provided IDs for Edges in.
    public static final String PROVIDED_EDGE_ID_PROPERTY_NAME = "aerospike.graphloader.provided-edge-id-property-name";
    // Percentage of the provided Vertex and Edge data to sample to verify integrity of the bulk load after completion.
    public static final String SAMPLING_PERCENTAGE = "aerospike.graphloader.sampling-percentage";
    public static final String SPARK_LOG_LEVEL = "aerospike.graphloader.spark-log-level";
    // String value of what should be parsed as a literal null value for properties. The null character \0 is a good alternative choice for this.
    public static final String NULL_VALUE = "aerospike.graphloader.null-value";
    public static final String VERTEX_WRITE_BUFFER = "aerospike.graphloader.vertex-write-buffer";
    public static final String EDGE_WRITE_BUFFER = "aerospike.graphloader.edge-write-buffer";
    // Flag to enable/disable to caching of dataframe
    public static final String ENABLE_DATAFRAME_CACHING = "aerospike.graphloader.dataframe-caching";
    // Storage type for Dataframe persist operation
    public static final String DATAFRAME_STORAGE_TYPE = "aerospike.graphloader.dataframe-storage-type";

    // ==Internal-only use configurations==
    public static final String S3_ENDPOINT = "aerospike.graphloader.s3-endpoint";

    // ==Actions==
    public static final String VERIFY_OUTPUT_DATA = "verify-output-data";
    public static final String VALIDATE_INPUT_DATA = "validate-input-data";
    public static final String DISABLE_EDGE_WRITE = "disable-edges";
    public static final String DISABLE_VERTEX_WRITE = "disable-vertices";
    public static final String READ_ONLY = "read-only";


    public static final Map<String, String> KEY_TO_CMD = Map.ofEntries(
            Map.entry(CONFIG_DIRECTORY_KEY, "c"),
            Map.entry(REMOTE_USERNAME, "u"),
            Map.entry(REMOTE_PASSKEY, "p"),

            Map.entry(GCS_KEYFILE_DIRECTORY, "gck"),
            Map.entry(GCS_EMAIL, "gem"),

            Map.entry(VERTEX_DIRECTORY_KEY, "vd"),
            Map.entry(EDGE_DIRECTORY_KEY, "ed"),
            Map.entry(TEMP_DIRECTORY_KEY, "td"),
            Map.entry(KEEP_PROVIDED_EDGE_ID_AS_PROPERTY, "ki"),
            Map.entry(PROVIDED_EDGE_ID_PROPERTY_NAME, "ep"),
            Map.entry(NULL_VALUE, "nv"),
            Map.entry(SAMPLING_PERCENTAGE, "sp"),
            Map.entry(SPARK_LOG_LEVEL, "lv"),
            Map.entry(VERTEX_WRITE_BUFFER, "vb"),
            Map.entry(EDGE_WRITE_BUFFER, "eb"),
            Map.entry(ENABLE_DATAFRAME_CACHING, "dc"),
            Map.entry(DATAFRAME_STORAGE_TYPE, "dt"),

            Map.entry(S3_ENDPOINT, "s3e")
    );

    private final Map<String, Object> fileConfig;
    private final CommandLine cmdConfig;

    private static final Map<String, String> DEFAULT_VALUES = new HashMap<>() {{
        put(KEEP_PROVIDED_EDGE_ID_AS_PROPERTY, "false");
        put(PROVIDED_EDGE_ID_PROPERTY_NAME, "~providedId");
        put(SAMPLING_PERCENTAGE, "0");
        put(ENABLE_DATAFRAME_CACHING, "false");
        put(DATAFRAME_STORAGE_TYPE, "disk_only");
        put(SPARK_LOG_LEVEL, "INFO");
        put(NULL_VALUE, "null");
        put(VERTEX_WRITE_BUFFER, "10000");
        put(EDGE_WRITE_BUFFER, "10000");
    }};

    public BulkLoaderConfigHelper(final Map<String, Object> fileConfig, final CommandLine cmdConfig) {
        this.fileConfig = fileConfig;
        this.cmdConfig = cmdConfig;
    }

    public String getOrDefault(final String key) {
        final String loweredKey = key.toLowerCase();
        final String cmdKey = KEY_TO_CMD.get(key);
        if (this.cmdConfig.hasOption(cmdKey)) {
            return this.cmdConfig.getOptionValue(cmdKey);
        } else if (this.fileConfig.containsKey(loweredKey)) {
            return this.fileConfig.get(loweredKey).toString();
        } else if (DEFAULT_VALUES.containsKey(loweredKey)) {
            return DEFAULT_VALUES.get(loweredKey);
        } else {
            throw new ConfigurationRuntimeException("No default value available for key: " + loweredKey);
        }
    }

    public boolean hasAction(final String action) {
        return this.cmdConfig.hasOption(action);
    }

    public Configuration getFireflyConfig() {
        return new MapConfiguration(this.fileConfig);
    }

    static public Configuration getConfig(Path path) {
        return ConfigurationHelper.loadFromFile(path);
    }
}
