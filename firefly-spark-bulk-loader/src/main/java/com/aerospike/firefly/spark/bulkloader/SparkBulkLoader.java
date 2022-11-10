package com.aerospike.firefly.spark.bulkloader;

import com.aerospike.firefly.spark.bulkloader.structure.SparkFireflyEdge;
import com.aerospike.firefly.spark.bulkloader.structure.SparkFireflyVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.cli.*;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static com.aerospike.firefly.spark.bulkloader.structure.SparkFireflyEdge.FROM_VERTEX_HEADER;
import static com.aerospike.firefly.spark.bulkloader.structure.SparkFireflyEdge.TO_VERTEX_HEADER;
import static com.aerospike.firefly.spark.bulkloader.structure.SparkFireflyElement.ID_HEADER;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.*;

public class SparkBulkLoader {
    private static final Logger logger = LoggerFactory.getLogger(SparkBulkLoader.class);
    private static final String[] REQUIRED_VERTEX_HEADERS = new String[]{ID_HEADER};
    private static final String[] REQUIRED_EDGE_HEADERS = new String[]{ID_HEADER, FROM_VERTEX_HEADER, TO_VERTEX_HEADER};

    private static Configuration config;

    private static String env = "prod";

    private static AmazonS3 s3Client;

    public static void main(final String[] args) {
        String bucket = "";
        String config_path = "";
        final Path path;
        final Set<String> vertexFiles = new HashSet<>();
        final Set<String> edgeFiles = new HashSet<>();
        try {
            CommandLine cmd = validInputs(args);
            env = cmd.hasOption("e") ? cmd.getOptionValue("e") : env;
            if ( env.equals("local") ) {
                String DEFAULT_CONFIG_PATH = "conf/spark-bulk-loader-conf/config.properties";
                config_path = cmd.hasOption("c") ? cmd.getOptionValue("c") : DEFAULT_CONFIG_PATH;
                path = Path.of(config_path);
                config = getConfig(path);
                vertexFiles.addAll(validateAndGetFiles(getOrDefault(VERTEX_DIRECTORY_KEY, config)));
                edgeFiles.addAll(validateAndGetFiles(getOrDefault(EDGE_DIRECTORY_KEY, config)));
            }
            else {
                s3Client = AmazonS3ClientBuilder.standard().build();
                bucket = cmd.getOptionValue("b");
                config_path = cmd.getOptionValue("c");
                assert bucket != null;
                assert config_path != null;
                config = loadConfigFromS3(bucket, config_path);
                vertexFiles.addAll(getObjectsListFromS3(bucket, getOrDefault(VERTEX_DIRECTORY_KEY, config)));
                edgeFiles.addAll(getObjectsListFromS3(bucket, getOrDefault(EDGE_DIRECTORY_KEY, config)));
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }


        // Initialize Spark
        final SparkConf conf = new SparkConf();
        if ( env.equals("local"))
            conf.setMaster("local[2]");

        conf.setAppName("firefly-bulk-loader")
            .set("spark.driver.allowMultipleContexts", "false")
            .set("spark.ui.enabled", "true");
        final SparkSession spark = SparkSession
                .builder().config(conf).getOrCreate();

        final List<Dataset<Row>> vertexDatasets = new ArrayList<>();
        final List<Dataset<Row>> edgeDatasets = new ArrayList<>();

        for (final String vertexFile : vertexFiles) {
            Map<String, String> options = new HashMap<>();
            options.put("header", "true");
            final Dataset<Row> vertexData = spark.read().options(options).csv(vertexFile);
            final Set<String> headers = new HashSet<>();
            for (final String header : vertexData.columns()) {
                headers.add(header.toLowerCase());
            }
            for (final String requiredHeader : REQUIRED_VERTEX_HEADERS) {
                if (!headers.contains(requiredHeader)) {
                    throw new IllegalArgumentException("Unable to find all required column header values in source: " +
                            vertexFile);
                }
            }
            vertexDatasets.add(vertexData);
        }

        for (final String edgeFile : edgeFiles) {
            Map<String, String> options = new HashMap<>();
            options.put("header", "true");
            final Dataset<Row> edgeData = spark.read().options(options).csv(edgeFile);
            final Set<String> headers = new HashSet<>();
            for (final String header : edgeData.columns()) {
                headers.add(header.toLowerCase());
            }
            for (final String requiredHeader : REQUIRED_EDGE_HEADERS) {
                if (!headers.contains(requiredHeader)) {
                    throw new IllegalArgumentException("Unable to find all required column header values in source: " +
                            edgeFile);
                }
            }
            edgeDatasets.add(edgeData);
        }

        // Vertices
        for (final Dataset<Row> vertexData : vertexDatasets) {
            final String finalBucket = bucket;
            final String finalConfig_path = config_path;
            vertexData.mapPartitions((MapPartitionsFunction<Row, Long>) rowIterator -> {
                logger.warn("PartitionId in VertexDataSet = " + TaskContext.getPartitionId()); //numerical value
                ArrayList<Long> list = new ArrayList<>(0);
                var new_config = config;
                if ( !env.equals("local") ) {
                    s3Client = AmazonS3ClientBuilder.standard().build();
                    new_config = loadConfigFromS3(finalBucket, finalConfig_path);
                }
                try (final FireflyGraph graph = FireflyGraph.open(new_config)) {
                    final boolean ignoreFailedProperties =
                            Boolean.parseBoolean(getOrDefault(IGNORE_PARSE_FAILED_PROPERTIES, new_config));
                    final boolean ignoreElementCreationFailed =
                            Boolean.parseBoolean(getOrDefault(IGNORE_ELEMENT_CREATION_FAILED, new_config));

                    while (rowIterator.hasNext()) {
                        final GenericRowWithSchema row = (GenericRowWithSchema) rowIterator.next();
                        try {
                            final SparkFireflyVertex sparkVertex = SparkFireflyVertex.createVertex(row,
                                    ignoreFailedProperties);
                            FireflyVertex vertex = graph.writeVertex(sparkVertex.getFireflyId(), sparkVertex.getLabel(), sparkVertex.getProperties());
                            list.add((long) vertex.id());
                        } catch (final RuntimeException e) {
                            logger.warn("Failed to load vertex for row: " + Arrays.toString(row.values()), e);
                            if (!ignoreElementCreationFailed) {
                                throw e;
                            }
                        }
                    }
                }
                return list.iterator();
            }, Encoders.LONG()).write().format("noop").mode(SaveMode.Append).save();
        }

        // Edges
        for (final Dataset<Row> edgeData : edgeDatasets) {
            final String finalBucket = bucket;
            String finalConfig_path = config_path;
            edgeData.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
                logger.info("PartitionId in EdgeDataSet = " + TaskContext.getPartitionId()); //numerical value
                var new_config = config;
                if ( !env.equals("local") ) {
                    s3Client = AmazonS3ClientBuilder.standard().build();
                    new_config = loadConfigFromS3(finalBucket, finalConfig_path);
                }
                try (final FireflyGraph graph = FireflyGraph.open(new_config)) {
                    final boolean ignoreFailedProperties =
                            Boolean.parseBoolean(getOrDefault(IGNORE_PARSE_FAILED_PROPERTIES, new_config));
                    final boolean useProvidedId = Boolean.parseBoolean(getOrDefault(USE_PROVIDED_EDGE_ID,
                            new_config));
                    final boolean keepProvidedId = Boolean.parseBoolean(getOrDefault(KEEP_PROVIDED_EDGE_ID_AS_PROPERTY,
                            new_config));
                    final String providedIdPropertyName = getOrDefault(PROVIDED_EDGE_ID_PROPERTY_NAME, new_config);
                    final boolean ignoreElementCreationFailed =
                            Boolean.parseBoolean(getOrDefault(IGNORE_ELEMENT_CREATION_FAILED, new_config));
                    while (rowIterator.hasNext()) {
                        final GenericRowWithSchema row = (GenericRowWithSchema) rowIterator.next();
                        try {
                            final SparkFireflyEdge sparkEdge = SparkFireflyEdge.createEdge(row, ignoreFailedProperties,
                                    useProvidedId, keepProvidedId, providedIdPropertyName, graph);
                            graph.bulkWriteEdgeToVertices(sparkEdge.getInVertexId(), sparkEdge.getOutVertexId(),
                                    sparkEdge.getId(), sparkEdge.getLabel());
                            graph.bulkWriteEdge(sparkEdge.getId(), sparkEdge.getLabel(), sparkEdge.getProperties(),
                                    sparkEdge.getInVertexId(), sparkEdge.getOutVertexId()); //change this to bulkWrite for much faster execution
                        } catch (final RuntimeException e) {
                            logger.warn("Failed to load edge for row: " + Arrays.toString(row.values()), e);
                            if (!ignoreElementCreationFailed) {
                                throw e;
                            }
                        }
                    }
                }
                return Collections.singletonList(1).iterator();
            }, Encoders.INT()).write().format("noop").mode(SaveMode.Append).save();
        }

        spark.stop();
    }

    /**
     * Function to load config files from local path
     * @param directory
     * @return
     */
    static private Set<String> validateAndGetFiles(final String directory) {
        File file = new File(directory);
        File[] directories = file.listFiles(File::isDirectory);

        if (directories.length != 0)
            return Arrays.stream(directories).map(File::getAbsolutePath).collect(Collectors.toSet());
        return Collections.singleton(directory);
    }

    /**
     * Function to load input files from S3.
     * This function returns all the directory paths leading upto the csv files. Does not return the csv's.
     * @param bucketName
     * @param folderKey
     * @return
     */
    public static Set<String> getObjectsListFromS3(final String bucketName, final String folderKey) {
        Set<String> keys = new HashSet<>();
        ObjectListing response = s3Client.listObjects(bucketName, folderKey);
        List<S3ObjectSummary> objects = response.getObjectSummaries();
        for (S3ObjectSummary object : objects) {
            keys.add("s3://" + object.getBucketName() + "/" + object.getKey().substring(0, object.getKey().lastIndexOf("/")));
        }
        while( response.isTruncated() ) {
            response = s3Client.listNextBatchOfObjects(response);
            objects = response.getObjectSummaries();
            for (S3ObjectSummary object : objects) {
                keys.add("s3://" + object.getBucketName() + "/" + object.getKey().substring(0, object.getKey().lastIndexOf("/")));
            }
        }
        return keys;
    }

    /**
     * Function to load config file from S3.
     * @param bucket
     * @param path
     * @return
     */
    public static Configuration loadConfigFromS3(final String bucket, final String path) {
        try {
            Properties props = new Properties();
            S3Object s3Object = s3Client.getObject(bucket, path);
            InputStream is = s3Object.getObjectContent();
            props.load(is);
            HashMap<String, Object> configData = new HashMap<>();
            props.keySet().forEach(it -> {
                final String key = it.toString().toLowerCase();
                final Object value = props.get(it.toString());
                logger.debug("config[{}:{}]", key, value);
                configData.put(key, value);
            });
            return new MapConfiguration(configData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static CommandLine validInputs(String[] args) throws Exception {
        Options options = new Options();
        Option envOption = new Option("e", "env", true, "local or prod");
        options.addOption(envOption);

        Option bucketOption = new Option("b", "bucket", true, "AWS S3 bucket");
        options.addOption(bucketOption);

        Option pathOption = new Option("c", "config", true, "config path [local -> absolute/S3 -> path to config after bucket]");
        options.addOption(pathOption);

        return validInputs(args, options);
    }

    public static CommandLine validInputs(String[] args, Options options) throws Exception {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw new Exception("Invalid Argument Exception " + e.getMessage());
        }
        return cmd;
    }
}

