package com.aerospike.firefly.process.call.bulkload.utils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ALLOWED_BAD_EDGES_COUNT;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ALLOWED_BAD_ENTRY_COUNT;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ALLOWED_DUPLICATE_VERTEX_ID_COUNT;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.CLEAR_EXISTING_DATA;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.CONFIG_DIRECTORY_KEY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.DATAFRAME_STORAGE_TYPE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.DISABLE_EDGE_WRITE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.DISABLE_VERTEX_WRITE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.EDGE_DIRECTORY_KEY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.EDGE_WRITE_BUFFER;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ENABLE_DATAFRAME_CACHING;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.FORCE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.GCS_EMAIL;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.GCS_KEYFILE_DIRECTORY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.INCREMENTAL_LOAD;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.KEY_TO_CMD;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.LOCAL_MODE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.NULL_VALUE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.READ_ONLY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.REMOTE_PASSKEY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.REMOTE_USERNAME;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.RESUME;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.S3_ENDPOINT;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.SAMPLING_PERCENTAGE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.SPARK_LOG_LEVEL;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.TEMP_DIRECTORY_KEY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.VALIDATE_INPUT_DATA;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.VERIFY_OUTPUT_DATA;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.VERTEX_DIRECTORY_KEY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.VERTEX_WRITE_BUFFER;

public class CommandLineParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandLineParser.class);

    static public CommandLine parseCmdArgs(final String[] args) {
        final Options options = new Options();

        // CommandLine ONLY configurations
        final Option modeOption = new Option(LOCAL_MODE, "Flag to indicate job is running from IDE/JVM");
        options.addOption(modeOption);
        final Option pathOption = new Option(KEY_TO_CMD.get(CONFIG_DIRECTORY_KEY), CONFIG_DIRECTORY_KEY, true, "Path to config. Local: Absolute path. AWS S3: Full path after bucket name.");
        options.addOption(pathOption);
        final Option usernameOption = new Option(KEY_TO_CMD.get(REMOTE_USERNAME), REMOTE_USERNAME, true, "Username/ID credential for cloud storage. Optional if local.");
        options.addOption(usernameOption);
        final Option passKeyOption = new Option(KEY_TO_CMD.get(REMOTE_PASSKEY), REMOTE_PASSKEY, true, "Password/Key/Secret credential for cloud storage. Optional if local.");
        options.addOption(passKeyOption);

        // Google Cloud specific configurations (CommandLine ONLY)
        final Option keyFileOption = new Option(KEY_TO_CMD.get(GCS_KEYFILE_DIRECTORY), GCS_KEYFILE_DIRECTORY, true, "Local-only path to Google Cloud key file for the Google Service Account.");
        options.addOption(keyFileOption);
        final Option gmailOption = new Option(KEY_TO_CMD.get(GCS_EMAIL), GCS_EMAIL, true, "Email of the Google Service Account.");
        options.addOption(gmailOption);

        // Configurations shared with config file
        final Option vertexDirOption = new Option(KEY_TO_CMD.get(VERTEX_DIRECTORY_KEY), VERTEX_DIRECTORY_KEY, true, "Path to directory containing vertex CSVs. Local: Absolute path. AWS S3: Directory after bucket name.");
        options.addOption(vertexDirOption);
        final Option edgeDirOption = new Option(KEY_TO_CMD.get(EDGE_DIRECTORY_KEY), EDGE_DIRECTORY_KEY, true, "Path to directory containing edge CSVs. Local: Absolute path. AWS S3: Directory after bucket name.");
        options.addOption(edgeDirOption);
        final Option edgeIdDirOption = new Option(KEY_TO_CMD.get(TEMP_DIRECTORY_KEY), TEMP_DIRECTORY_KEY, true, "Path to EdgeID directory. Local: Absolute path. AWS S3: Directory after bucket name.");
        options.addOption(edgeIdDirOption);
        final Option keepEdgeIdOption = new Option(KEY_TO_CMD.get(KEEP_PROVIDED_EDGE_ID_AS_PROPERTY), KEEP_PROVIDED_EDGE_ID_AS_PROPERTY, true, "Boolean to keep provided Edge IDs as a property. Optional argument - Default: 'false'.");
        options.addOption(keepEdgeIdOption);
        final Option idPropNameOption = new Option(KEY_TO_CMD.get(PROVIDED_EDGE_ID_PROPERTY_NAME), PROVIDED_EDGE_ID_PROPERTY_NAME, true, "Property key of provided Edge ID if stored as a property. Optional argument - Default: '~providedId'.");
        options.addOption(idPropNameOption);
        final Option nullValueOption = new Option(KEY_TO_CMD.get(NULL_VALUE), NULL_VALUE, true, "String value in CSV that is parsed as a literal null for property values. Optional argument - Default: 'null'.");
        options.addOption(nullValueOption);
        final Option samplePercentageOption = new Option(KEY_TO_CMD.get(SAMPLING_PERCENTAGE), SAMPLING_PERCENTAGE, true, "Percentage of dataset to sample as validation after bulk loading. Optional argument - Default: '0'.");
        options.addOption(samplePercentageOption);
        final Option logLevelOption = new Option(KEY_TO_CMD.get(SPARK_LOG_LEVEL), SPARK_LOG_LEVEL, true, "Logger verbosity level. Optional argument - Default: 'INFO'.");
        options.addOption(logLevelOption);
        final Option vertexWriteBufferOption = new Option(KEY_TO_CMD.get(VERTEX_WRITE_BUFFER), VERTEX_WRITE_BUFFER, true, "Vertex write buffer size. Optional argument - Default: '10000'.");
        options.addOption(vertexWriteBufferOption);
        final Option edgeWriteBufferOption = new Option(KEY_TO_CMD.get(EDGE_WRITE_BUFFER), EDGE_WRITE_BUFFER, true, "Edge write buffer size. Optional argument - Default: '10000'.");
        options.addOption(edgeWriteBufferOption);
        final Option dataframeCacheOption = new Option(KEY_TO_CMD.get(ENABLE_DATAFRAME_CACHING), ENABLE_DATAFRAME_CACHING, true, "Boolean for enabling dataframe caching. Optional argument - Default: 'false'.");
        options.addOption(dataframeCacheOption);
        final Option dataframeStorageOption = new Option(KEY_TO_CMD.get(DATAFRAME_STORAGE_TYPE), DATAFRAME_STORAGE_TYPE, true, "Dataframe storage type. Optional argument - Default: 'disk_only'.");
        options.addOption(dataframeStorageOption);
        final Option duplicateVertexIdCountOption = new Option(KEY_TO_CMD.get(ALLOWED_DUPLICATE_VERTEX_ID_COUNT), ALLOWED_DUPLICATE_VERTEX_ID_COUNT, true, "Amount of duplicate Vertex IDs allowed in CSV data set. Optional argument - Default: 'No limit'.");
        options.addOption(duplicateVertexIdCountOption);
        final Option badEdgeCountOption = new Option(KEY_TO_CMD.get(ALLOWED_BAD_EDGES_COUNT), ALLOWED_BAD_EDGES_COUNT, true, "Amount of Edges with invalid IN or OUT Vertex ID allowed in CSV data set. Optional argument - Default: 'No limit'.");
        options.addOption(badEdgeCountOption);
        final Option badEntryCountOption = new Option(KEY_TO_CMD.get(ALLOWED_BAD_ENTRY_COUNT), ALLOWED_BAD_ENTRY_COUNT, true, "Amount of entries with values that cannot be parsed according to type specified by the header allowed in CSV data set. Optional argument - Default: 'No limit'.");
        options.addOption(badEntryCountOption);

        // Internal use configurations
        final Option s3EndPointOption = new Option(KEY_TO_CMD.get(S3_ENDPOINT), S3_ENDPOINT, true, "Custom S3 endpoint.");
        options.addOption(s3EndPointOption);

        // Actions
        options.addOption(new Option(RESUME, "Resume previous load."));
        options.addOption(new Option(CLEAR_EXISTING_DATA, "Clear existing load data and run fresh (removes resume data and existing data in database)."));
        options.addOption(new Option(INCREMENTAL_LOAD, "Enable incremental load."));
        options.addOption(new Option(VERIFY_OUTPUT_DATA, "Read elements back after bulk load completion to validate loading."));
        options.addOption(new Option(VALIDATE_INPUT_DATA, "Validate entire content of vertex and edge CSVs before bulk loading."));
        options.addOption(new Option(READ_ONLY, "Disables intermediate writing to a temporary file to prevent potential duplicate edges."));
        options.addOption(new Option(DISABLE_EDGE_WRITE, "Disable Edge writing."));
        options.addOption(new Option(DISABLE_VERTEX_WRITE, "Disable Vertex writing."));
        options.addOption(new Option(FORCE, "Force load to run with supplied inputs (removes any existing recovery data)."));

        final org.apache.commons.cli.CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (final ParseException e) {
            LOGGER.error("Error parsing configuration arguments: ", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("SparkBulkLoader", options);
            throw new IllegalArgumentException(e);
        }
    }
}
