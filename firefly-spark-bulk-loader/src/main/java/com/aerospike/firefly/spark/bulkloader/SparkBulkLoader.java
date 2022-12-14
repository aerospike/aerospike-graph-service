package com.aerospike.firefly.spark.bulkloader;

import com.aerospike.client.Value;
import com.aerospike.firefly.spark.bulkloader.structure.SparkFireflyEdge;
import com.aerospike.firefly.spark.bulkloader.structure.SparkFireflyVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.aerospike.firefly.spark.bulkloader.structure.SparkFireflyEdge.FROM_VERTEX_HEADER;
import static com.aerospike.firefly.spark.bulkloader.structure.SparkFireflyEdge.TO_VERTEX_HEADER;
import static com.aerospike.firefly.spark.bulkloader.structure.SparkFireflyElement.ID_HEADER;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.EDGE_DIRECTORY_KEY;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.IGNORE_ELEMENT_CREATION_FAILED;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.IGNORE_PARSE_FAILED_PROPERTIES;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.USE_PROVIDED_EDGE_ID;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.VERTEX_DIRECTORY_KEY;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.getConfig;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.getOrDefault;

public class SparkBulkLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoader.class);
    private static final String[] REQUIRED_VERTEX_HEADERS = new String[]{ID_HEADER};
    private static final String[] REQUIRED_EDGE_HEADERS = new String[]{ID_HEADER, FROM_VERTEX_HEADER, TO_VERTEX_HEADER};
    private static Configuration CONFIG;
    private static String ENV = "prod";
    private static AmazonS3 S3_CLIENT;

    public static void main(final String[] args) {
        String s3BucketName = null;
        final String configPath;
        final Set<String> vertexDirectories = new HashSet<>();
        final Set<String> edgeDirectories = new HashSet<>();
        final CommandLine cmd = parseCmdArgs(args);
        ENV = cmd.hasOption("e") ? cmd.getOptionValue("e") : ENV;
        if (ENV.equals("local")) {
            final String defaultConfigPath = "conf/spark-bulk-loader-conf/config.properties";
            configPath = cmd.hasOption("c") ? cmd.getOptionValue("c") : defaultConfigPath;
            final Path path = Path.of(configPath);
            CONFIG = getConfig(path);
            vertexDirectories.addAll(getElementDirectories(getOrDefault(VERTEX_DIRECTORY_KEY, CONFIG)));
            edgeDirectories.addAll(getElementDirectories(getOrDefault(EDGE_DIRECTORY_KEY, CONFIG)));
        } else {
            S3_CLIENT = AmazonS3ClientBuilder.standard().build();
            s3BucketName = cmd.getOptionValue("b");
            configPath = cmd.getOptionValue("c");
            assert s3BucketName != null;
            assert configPath != null;
            CONFIG = loadConfigFromS3(s3BucketName, configPath);
            vertexDirectories.addAll(getObjectsListFromS3(s3BucketName, getOrDefault(VERTEX_DIRECTORY_KEY, CONFIG)));
            edgeDirectories.addAll(getObjectsListFromS3(s3BucketName, getOrDefault(EDGE_DIRECTORY_KEY, CONFIG)));
        }


        // Initialize Spark
        final SparkConf conf = new SparkConf();
        if (ENV.equals("local"))
            conf.setMaster("local[2]");

        conf.setAppName("firefly-bulk-loader")
                .set("spark.driver.allowMultipleContexts", "false")
                .set("spark.ui.enabled", "true");
        final SparkSession spark = SparkSession
                .builder().config(conf).getOrCreate();

        final List<Dataset<Row>> vertexDatasets = new ArrayList<>();
        final List<Dataset<Row>> edgeDatasets = new ArrayList<>();

        for (final String vertexDirectory : vertexDirectories) {
            final Map<String, String> options = new HashMap<>();
            options.put("header", "true");
            final Dataset<Row> vertexData = spark.read().options(options).csv(vertexDirectory);
            final Set<String> headers = new HashSet<>();
            for (final String header : vertexData.columns()) {
                headers.add(header.toLowerCase());
            }
            for (final String requiredHeader : REQUIRED_VERTEX_HEADERS) {
                if (!headers.contains(requiredHeader)) {
                    throw new IllegalArgumentException("Unable to find all required column header values in source: " +
                            vertexDirectory);
                }
            }
            vertexDatasets.add(vertexData);
        }

        for (final String edgeDirectory : edgeDirectories) {
            final Map<String, String> options = new HashMap<>();
            options.put("header", "true");
            final Dataset<Row> edgeData = spark.read().options(options).csv(edgeDirectory);
            final Set<String> headers = new HashSet<>();
            for (final String header : edgeData.columns()) {
                headers.add(header.toLowerCase());
            }
            for (final String requiredHeader : REQUIRED_EDGE_HEADERS) {
                if (!headers.contains(requiredHeader)) {
                    throw new IllegalArgumentException("Unable to find all required column header values in source: " +
                            edgeDirectory);
                }
            }
            edgeDatasets.add(edgeData);
        }

        // Vertices
        for (final Dataset<Row> vertexData : vertexDatasets) {
            final String finalS3BucketName = s3BucketName;
            final String finalConfigPath = configPath;
            vertexData.mapPartitions((MapPartitionsFunction<Row, Long>) rowIterator -> {
                LOGGER.warn("PartitionId in VertexDataset = " + TaskContext.getPartitionId()); // Numerical value
                final ArrayList<Long> list = new ArrayList<>();
                Configuration localConfig = CONFIG;
                if (!ENV.equals("local")) {
                    S3_CLIENT = AmazonS3ClientBuilder.standard().build();
                    localConfig = loadConfigFromS3(finalS3BucketName, finalConfigPath);
                }
                try (final FireflyGraph graph = FireflyGraph.open(localConfig)) {
                    final boolean ignoreFailedProperties =
                            Boolean.parseBoolean(getOrDefault(IGNORE_PARSE_FAILED_PROPERTIES, localConfig));
                    final boolean ignoreElementCreationFailed =
                            Boolean.parseBoolean(getOrDefault(IGNORE_ELEMENT_CREATION_FAILED, localConfig));

                    while (rowIterator.hasNext()) {
                        final GenericRowWithSchema row = (GenericRowWithSchema) rowIterator.next();
                        try {
                            final SparkFireflyVertex sparkVertex = SparkFireflyVertex.createVertex(row,
                                    ignoreFailedProperties);
                            FireflyVertex vertex = graph.writeVertex(sparkVertex.getFireflyId(), sparkVertex.getLabel(),
                                    sparkVertex.getProperties());
                            list.add((long) vertex.id());
                        } catch (final RuntimeException e) {
                            LOGGER.warn("Failed to load vertex for row: " + Arrays.toString(row.values()), e);
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
            final String finalS3BucketName = s3BucketName;
            final String finalConfigPath = configPath;
            edgeData.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
                LOGGER.info("PartitionId in EdgeDataset = " + TaskContext.getPartitionId()); // Bumerical value
                Configuration localConfig = CONFIG;
                if (!ENV.equals("local")) {
                    S3_CLIENT = AmazonS3ClientBuilder.standard().build();
                    localConfig = loadConfigFromS3(finalS3BucketName, finalConfigPath);
                }
                try (final FireflyGraph graph = FireflyGraph.open(localConfig)) {
                    final boolean ignoreFailedProperties =
                            Boolean.parseBoolean(getOrDefault(IGNORE_PARSE_FAILED_PROPERTIES, localConfig));
                    final boolean useProvidedId = Boolean.parseBoolean(getOrDefault(USE_PROVIDED_EDGE_ID,
                            localConfig));
                    final boolean keepProvidedId = Boolean.parseBoolean(getOrDefault(KEEP_PROVIDED_EDGE_ID_AS_PROPERTY,
                            localConfig));
                    final String providedIdPropertyName = getOrDefault(PROVIDED_EDGE_ID_PROPERTY_NAME, localConfig);
                    final boolean ignoreElementCreationFailed =
                            Boolean.parseBoolean(getOrDefault(IGNORE_ELEMENT_CREATION_FAILED, localConfig));

                    // TODO: Finalize these numbers.
                    // Defaults to 1000 since the current default ID_CACHE_SIZE is 100000. Batch write edge IDs to
                    // vertex cache when this amount can be written at once.
                    final long writeEdgeLabelListTriggerSize = Math.max(graph.getBaseGraph().ID_CACHE_SIZE / 100, 100);
                    // Flush the edge ID map when this amount of edges lives in it to prevent running out of memory.
                    final int writeEdgeAmountTriggerSize = 1000000;

                    final AtomicInteger outEdgeCount = new AtomicInteger(0);
                    final AtomicInteger inEdgeCount = new AtomicInteger(0);
                    final Map<Long, Map<String, List<Value>>> vertexOutEdgeMap = new HashMap<>();
                    final Map<Long, Map<String, List<Value>>> vertexInEdgeMap = new HashMap<>();
                    final Set<Long> supernodes = new HashSet<>();

                    while (rowIterator.hasNext()) {
                        final GenericRowWithSchema row = (GenericRowWithSchema) rowIterator.next();
                        try {
                            final SparkFireflyEdge sparkEdge = SparkFireflyEdge.createEdge(row, ignoreFailedProperties,
                                    useProvidedId, keepProvidedId, providedIdPropertyName, graph);
                            final FireflyId edgeId = sparkEdge.getFireflyId();
                            final long inVertexId = sparkEdge.getInVertexId();
                            final long outVertexId = sparkEdge.getOutVertexId();
                            final String edgeLabel = sparkEdge.getLabel();
                                graph.bulkWriteEdge(sparkEdge.getId(), edgeLabel, sparkEdge.getProperties(), inVertexId,
                                        outVertexId);

                            if (!graph.getBaseGraph().EDGE_CACHE_DISABLED_GLOBALLY) {
                                loadEdgeMap(graph, supernodes, outVertexId,
                                        FireflyIdFactory.createEdgeId(edgeId, FireflyIdFactory.createId(inVertexId)),
                                        edgeLabel, Direction.OUT, outEdgeCount, vertexOutEdgeMap,
                                        writeEdgeLabelListTriggerSize, writeEdgeAmountTriggerSize,
                                        ignoreElementCreationFailed);
                                loadEdgeMap(graph, supernodes, inVertexId,
                                        FireflyIdFactory.createEdgeId(edgeId, FireflyIdFactory.createId(outVertexId)),
                                        edgeLabel, Direction.IN, inEdgeCount, vertexInEdgeMap,
                                        writeEdgeLabelListTriggerSize, writeEdgeAmountTriggerSize,
                                        ignoreElementCreationFailed);
                            }
                        } catch (final RuntimeException e) {
                            LOGGER.warn("Failed to load edge for row: " + Arrays.toString(row.values()), e);
                            if (!ignoreElementCreationFailed) {
                                throw e;
                            }
                        }
                    }
                    flushEdgeMap(graph, supernodes, Direction.OUT, vertexOutEdgeMap, ignoreElementCreationFailed);
                    flushEdgeMap(graph, supernodes, Direction.IN, vertexInEdgeMap, ignoreElementCreationFailed);
                }
                return Collections.singletonList(1).iterator();
            }, Encoders.INT()).write().format("noop").mode(SaveMode.Append).save();
        }

        spark.stop();
    }

    static private void loadEdgeMap(final FireflyGraph graph, final Set<Long> supernodes, final long vertexId,
                                    final FireflyId cachedEdgeId, final String edgeLabel, final Direction direction,
                                    final AtomicInteger edgeCount, final Map<Long, Map<String, List<Value>>> edgeMap,
                                    final long writeEdgeLabelListTriggerSize, final long writeEdgeAmountTriggerSize,
                                    final boolean ignoreElementCreationFailed) {
        if (!supernodes.contains(vertexId)) {
            if (!edgeMap.containsKey(vertexId)) {
                edgeMap.put(vertexId, new HashMap<>());
            }
            final Map<String, List<Value>> labelEdgeIds = edgeMap.get(vertexId);
            if (!labelEdgeIds.containsKey(edgeLabel)) {
                labelEdgeIds.put(edgeLabel, new ArrayList<>());
            }
            final List<Value> edgeIds = labelEdgeIds.get(edgeLabel);
            edgeIds.add(Value.get(cachedEdgeId.getCachedId()));
            final int count = edgeCount.incrementAndGet();
            if (edgeIds.size() > writeEdgeLabelListTriggerSize) {
                writeEdgesToFireflyVertex(graph, supernodes, vertexId, direction, edgeLabel, edgeIds,
                        ignoreElementCreationFailed);
                labelEdgeIds.remove(edgeLabel);
                if (labelEdgeIds.isEmpty()) {
                    edgeMap.remove(vertexId);
                }
                edgeCount.addAndGet(-edgeIds.size());
            } else if (count > writeEdgeAmountTriggerSize) {
                flushEdgeMap(graph, supernodes, direction, edgeMap, ignoreElementCreationFailed);
                edgeCount.set(0);
            }
        }
    }

    static private void writeEdgesToFireflyVertex(final FireflyGraph graph, final Set<Long> supernodes,
                                                  final long vertexId, final Direction direction, final String label,
                                                  final List<Value> edgeIds,
                                                  final boolean ignoreElementCreationFailed) {
        try {
            if (!graph.bulkWriteEdgesToVertexCache(FireflyIdFactory.createId(vertexId), direction, edgeIds, label)) {
                supernodes.add(vertexId);
            }
        } catch (final RuntimeException e) {
            LOGGER.warn("Failed to load edges with label " + label + " into " + direction + " edge cache for vertex ID "
                    + vertexId, e);
            if (!ignoreElementCreationFailed) {
                throw e;
            }
        }
    }

    static private void flushEdgeMap(final FireflyGraph graph, final Set<Long> supernodes, final Direction direction,
                                     final Map<Long, Map<String, List<Value>>> edgeMap,
                                     final boolean ignoreElementCreationFailed) {
        for (Map.Entry<Long, Map<String, List<Value>>> vertexIdToLabelMaps : edgeMap.entrySet()) {
            final long vertexId = vertexIdToLabelMaps.getKey();
            final Map<String, List<Value>> labelMaps = vertexIdToLabelMaps.getValue();
            for (Map.Entry<String, List<Value>> labelToEdgeIds : labelMaps.entrySet()) {
                // Writing edge IDs under a different label could have triggered supernode detection.
                if (supernodes.contains(vertexId)) {
                    break;
                }
                writeEdgesToFireflyVertex(graph, supernodes, vertexId, direction, labelToEdgeIds.getKey(),
                        labelToEdgeIds.getValue(), ignoreElementCreationFailed);
            }
        }
        edgeMap.clear();
    }

    /**
     * Function to get a set of valid sub-directory strings of verticies or edges from a master directory.
     *
     * @param directory The master directory.
     *
     * @return  The set of valid sub-directories.
     */
    static private Set<String> getElementDirectories(final String directory) {
        final File file = new File(directory);
        final File[] directories = file.listFiles(File::isDirectory);

        if (directories.length != 0)
            return Arrays.stream(directories).map(File::getAbsolutePath).collect(Collectors.toSet());
        return Collections.singleton(directory);
    }

    /**
     * Function to load input files from S3.
     * This function returns all the directory paths leading upto the csv files. Does not return the csv's.
     *
     * @param bucketName    Name of the S3 bucket.
     * @param folderKey     Folder key string specifying the name of the master directory of verticies or edges.
     *
     * @return  Set of directory path strings containing the csv files in S3.
     */
    public static Set<String> getObjectsListFromS3(final String bucketName, final String folderKey) {
        final Set<String> keys = new HashSet<>();
        ObjectListing response = S3_CLIENT.listObjects(bucketName, folderKey);
        List<S3ObjectSummary> objects = response.getObjectSummaries();
        for (final S3ObjectSummary object : objects) {
            keys.add("s3://" + object.getBucketName() + "/" + object.getKey().substring(0, object.getKey().lastIndexOf("/")));
        }
        // listObjects loads 1000 object keys in one call.
        // If there are multiple directories with more than 1000 files, then need to consume any remaining objects.
        while (response.isTruncated()) {
            response = S3_CLIENT.listNextBatchOfObjects(response);
            objects = response.getObjectSummaries();
            for (S3ObjectSummary object : objects) {
                keys.add("s3://" + object.getBucketName() + "/" + object.getKey().substring(0, object.getKey().lastIndexOf("/")));
            }
        }
        return keys;
    }

    /**
     * Function to load config file from S3.
     *
     * @param bucketName    Name of the S3 bucket.
     * @param path          Path to config in bucket.
     *
     * @return  Configuration object built from config file.
     */
    public static Configuration loadConfigFromS3(final String bucketName, final String path) {
        try (final S3Object s3Object = S3_CLIENT.getObject(bucketName, path);
             final InputStream inputStream = s3Object.getObjectContent()) {
            final Properties props = new Properties();
            props.load(inputStream);
            final HashMap<String, Object> configData = new HashMap<>();
            props.keySet().forEach(it -> {
                final String key = it.toString().toLowerCase();
                final Object value = props.get(it.toString());
                LOGGER.debug("config[{}:{}]", key, value);
                configData.put(key, value);
            });
            return new MapConfiguration(configData);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static CommandLine parseCmdArgs(final String[] args) {
        final Options options = new Options();
        final Option envOption = new Option("e", "env", true, "local or prod");
        options.addOption(envOption);

        final Option bucketOption = new Option("b", "bucket", true, "AWS S3 bucket name");
        options.addOption(bucketOption);

        final Option pathOption = new Option("c", "config", true, "config path [local -> absolute/S3 -> path to config after bucket]");
        options.addOption(pathOption);

        final CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (final ParseException e) {
            LOGGER.error("Error parsing configuration arguments: ", e);
            throw new IllegalArgumentException(e);
        }
    }
}
