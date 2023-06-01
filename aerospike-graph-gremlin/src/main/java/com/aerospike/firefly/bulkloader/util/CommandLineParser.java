package com.aerospike.firefly.bulkloader.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.DATAFRAME_STORAGE_TYPE;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.EDGE_DIRECTORY_KEY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.EDGE_WRITE_BUFFER;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.ENABLE_DATAFRAME_CACHING;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.FILE_SYSTEM;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.MASTER_DIRECTORY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.NULL_VALUE;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.SAMPLING_PERCENTAGE;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.SPARK_LOG_LEVEL;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.VERTEX_DIRECTORY_KEY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.VERTEX_WRITE_BUFFER;

public class CommandLineParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommandLineParser.class);
    // Mode
    public static final String LOCAL_MODE = "local";
    // Actions
    public static final String VERIFY_EDGE = "verifyedge";
    public static final String VERIFY_VERTEX = "verifyvertex";
    public static final String DRY_RUN = "dryrun";
    public static final String WRITE_EDGE = "writeedge";
    public static final String WRITE_VERTEX = "writevertex";
    public static final String SUPERNODE = "supernode";

    static public CommandLine parseCmdArgs(final String[] args) {
        final Options options = new Options();

        // CommandLine ONLY configurations
        final Option modeOption = new Option(LOCAL_MODE, "Flag to indicate job is running from IDE/JVM");
        options.addOption(modeOption);
        final Option pathOption = new Option("c", "aerospike.graphloader.config", true, "Path to config. Local: Absolute path. AWS S3: Full path after bucket name.");
        options.addOption(pathOption);
        final Option envOption = new Option("e", "aerospike.graphloader.env", true, "Job environment. Optional argument - Default: 'local'. 'aws' when running job in cluster mode in AWS.");
        options.addOption(envOption);
        final Option usernameOption = new Option("u", "aerospike.graphloader.remote-user", true, "Username/ID credential for cloud storage. Optional if local.");
        options.addOption(usernameOption);
        final Option passKeyOption = new Option("p", "aerospike.graphloader.remote-passkey", true, "Password/Key/Secret credential for cloud storage. Optional if local");        options.addOption(passKeyOption);

        // Configurations shared with config file
        final Option bucketOption = new Option("md", MASTER_DIRECTORY, true, "Top level container name when using cloud storage. Optional if local. AWS S3: Bucket name.");
        options.addOption(bucketOption);
        final Option vertexDirOption = new Option("vd", VERTEX_DIRECTORY_KEY, true, "Path to directory containing vertex CSVs. Local: Absolute path. AWS S3: Directory after bucket name.");
        options.addOption(vertexDirOption);
        final Option edgeDirOption = new Option("ed", EDGE_DIRECTORY_KEY, true, "Path to directory containing edge CSVs. Local: Absolute path. AWS S3: Directory after bucket name.");
        options.addOption(edgeDirOption);
        final Option fsOption = new Option("fs", FILE_SYSTEM, true, "FileSystem storage type. Optional argument - Default: 'local'. AWS: 's3'.");
        options.addOption(fsOption);
        final Option keepEdgeIdOption = new Option("ki", KEEP_PROVIDED_EDGE_ID_AS_PROPERTY, true, "Boolean to keep provided Edge IDs as a property. Optional argument - Default: 'false'.");
        options.addOption(keepEdgeIdOption);
        final Option idPropNameOption = new Option("ep", PROVIDED_EDGE_ID_PROPERTY_NAME, true, "Property key of provided Edge ID if stored as a property. Optional argument - Default: '~providedId'.");
        options.addOption(idPropNameOption);
        final Option nullValueOption = new Option("nv", NULL_VALUE, true, "String value in CSV that is parsed as a literal null for property values. Optional argument - Default: 'null'.");
        options.addOption(nullValueOption);
        final Option samplePercentageOption = new Option("sp", SAMPLING_PERCENTAGE, true, "Percentage of dataset to sample as validation after bulk loading. Optional argument - Default: '1'.");
        options.addOption(samplePercentageOption);
        final Option logLevelOption = new Option("lv", SPARK_LOG_LEVEL, true, "Logger verbosity level. Optional argument - Default: 'INFO'.");
        options.addOption(logLevelOption);
        final Option vertexWriteBufferOption = new Option("vb", VERTEX_WRITE_BUFFER, true, "Vertex write buffer size. Optional argument - Default: '10000'.");
        options.addOption(vertexWriteBufferOption);
        final Option edgeWriteBufferOption = new Option("eb", EDGE_WRITE_BUFFER, true, "Edge write buffer size. Optional argument - Default: '10000'.");
        options.addOption(edgeWriteBufferOption);
        final Option dataframeCacheOption = new Option("dc", ENABLE_DATAFRAME_CACHING, true, "Boolean for enabling dataframe caching. Optional argument - Default: 'false'.");
        options.addOption(dataframeCacheOption);
        final Option dataframeStorageOption = new Option("dt", DATAFRAME_STORAGE_TYPE, true, "Dataframe storage type. Optional argument - Default: 'disk_only'.");
        options.addOption(dataframeStorageOption);

        // Actions
        final Option ve = new Option(VERIFY_EDGE, "Verify edges.");
        options.addOption(ve);
        final Option vv = new Option(VERIFY_VERTEX, "Verify vertices.");
        options.addOption(vv);
        final Option dr = new Option(DRY_RUN, "Dry run of edges and vertices.");
        options.addOption(dr);
        final Option we = new Option(WRITE_EDGE, "Write edges.");
        options.addOption(we);
        final Option wv = new Option(WRITE_VERTEX, "Write vertices.");
        options.addOption(wv);
        final Option se = new Option(SUPERNODE, "Handle supernodes.");
        options.addOption(se);

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
