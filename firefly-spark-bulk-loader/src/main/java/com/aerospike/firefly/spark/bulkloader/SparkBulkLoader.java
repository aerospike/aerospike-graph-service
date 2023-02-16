package com.aerospike.firefly.spark.bulkloader;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.spark.bulkloader.structure.SparkFireflyEdge;
import com.aerospike.firefly.spark.bulkloader.structure.SparkFireflyVertex;
import com.aerospike.firefly.spark.bulkloader.util.FireflyBulkLoaderException;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.storage.StorageLevel;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.aerospike.firefly.spark.bulkloader.structure.SparkFireflyEdge.FROM_VERTEX_HEADER;
import static com.aerospike.firefly.spark.bulkloader.structure.SparkFireflyEdge.TO_VERTEX_HEADER;
import static com.aerospike.firefly.spark.bulkloader.structure.SparkFireflyElement.ID_HEADER;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.EDGE_DIRECTORY_KEY;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.IGNORE_ELEMENT_CREATION_FAILED;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.IGNORE_PARSE_FAILED_PROPERTIES;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.NULL_VALUE;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.SAMPLING_PERCENTAGE;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.USE_PROVIDED_EDGE_ID;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.VERTEX_DIRECTORY_KEY;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.getConfig;
import static com.aerospike.firefly.spark.bulkloader.util.BulkLoaderConfigHelper.getOrDefault;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.EDGE_CACHE_DISABLED_GLOBALLY;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ID_CACHE_SIZE;

public class SparkBulkLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoader.class);
    private static final String[] REQUIRED_VERTEX_HEADERS = new String[]{ID_HEADER};
    private static final String[] REQUIRED_EDGE_HEADERS = new String[]{ID_HEADER, FROM_VERTEX_HEADER, TO_VERTEX_HEADER};
    private static Configuration CONFIG;
    private static String MODE = "cluster";
    private static AmazonS3 S3_CLIENT;
    private static final int RETRY_LIMIT = 100;

    // TODO: Finalize this number or make it configurable.
    private static final int EDGE_CACHE_FLUSH_THRESHOLD = 100000;

    public static void main(final String[] args) {
        String s3BucketName = null;
        String configPath = "";
        final Set<String> vertexDirectories = new HashSet<>();
        final Set<String> edgeDirectories = new HashSet<>();
        final CommandLine cmd = parseCmdArgs(args);
        final String defaultConfigPath = "conf/spark-bulk-loader-conf/config.properties";
        MODE = cmd.hasOption("m") ? cmd.getOptionValue("m") : MODE;
        final String ENV = cmd.hasOption("e") ? cmd.getOptionValue("e") : "";
        try {
            if (ENV.equalsIgnoreCase("aws")) {
                S3_CLIENT = AmazonS3ClientBuilder.standard().build();
                s3BucketName = cmd.getOptionValue("b");
                configPath = cmd.getOptionValue("c");
                assert s3BucketName != null;
                assert configPath != null;
                CONFIG = loadConfigFromS3(s3BucketName, configPath);
                vertexDirectories.addAll(getObjectsListFromS3(s3BucketName, getOrDefault(VERTEX_DIRECTORY_KEY, CONFIG)));
                edgeDirectories.addAll(getObjectsListFromS3(s3BucketName, getOrDefault(EDGE_DIRECTORY_KEY, CONFIG)));
            } else {
                configPath = cmd.hasOption("c") ? cmd.getOptionValue("c") : defaultConfigPath;
                CONFIG = loadConfiguration(configPath);
                vertexDirectories.addAll(getElementDirectories(getOrDefault(VERTEX_DIRECTORY_KEY, CONFIG)));
                edgeDirectories.addAll(getElementDirectories(getOrDefault(EDGE_DIRECTORY_KEY, CONFIG)));
            }
        }
        catch (IOException ie) {
            LOGGER.error("Unable to load config." + ie.getMessage());
            ie.printStackTrace();
            System.exit(1);
        }
        catch (AmazonClientException awsexception) {
            LOGGER.error("Amazon SDK client error" + awsexception.getMessage());
            System.exit(1);
        }
        final double sampleFraction = Double.parseDouble(getOrDefault(SAMPLING_PERCENTAGE, CONFIG)) / 100;

        // Initialize Spark
        final SparkConf conf = new SparkConf();
        if (MODE.equals("local"))
            conf.setMaster("local[*]");

        conf.setAppName("firefly-bulk-loader")
                .set("spark.driver.allowMultipleContexts", "false")
                .set("spark.ui.enabled", "true");
        final SparkSession spark = SparkSession
                .builder().config(conf).getOrCreate();

        final List<Dataset<Row>> vertexDatasets = new ArrayList<>();
        final List<Dataset<Row>> edgeDatasets = new ArrayList<>();

        final List<Dataset<Row>> sampledVertexDatasets = new ArrayList<>();
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
            sampledVertexDatasets.add(vertexData.sample(true, sampleFraction).distinct());
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

        final String finalS3BucketName = s3BucketName;
        final String finalConfigPath = configPath;

        // Vertices
        final AtomicReference<Configuration> localConfig = new AtomicReference<>();
        for (final Dataset<Row> vertexData : vertexDatasets) {
            vertexData.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
                LOGGER.info("PartitionId in VertexDataset = " + TaskContext.getPartitionId()); // Numerical value
                if (ENV.equalsIgnoreCase("aws")) {
                    S3_CLIENT = AmazonS3ClientBuilder.standard().build();
                    localConfig.set(loadConfigFromS3(finalS3BucketName, finalConfigPath));
                }
                else {
                    localConfig.set(loadConfiguration(finalConfigPath));
                }
                final boolean ignoreFailedProperties =
                        Boolean.parseBoolean(getOrDefault(IGNORE_PARSE_FAILED_PROPERTIES, localConfig.get()));
                final boolean ignoreElementCreationFailed =
                        Boolean.parseBoolean(getOrDefault(IGNORE_ELEMENT_CREATION_FAILED, localConfig.get()));
                final String nullValue = getOrDefault(NULL_VALUE, localConfig.get());

                try (final FireflyGraph graph = FireflyGraph.open(localConfig.get())) {

                    while (rowIterator.hasNext()) {
                        final GenericRowWithSchema row = (GenericRowWithSchema) rowIterator.next();
                        try {
                            final SparkFireflyVertex sparkVertex =
                                    SparkFireflyVertex.createVertex(row, ignoreFailedProperties, nullValue);
                            int tryCount = 0;
                            boolean succeeded = false;
                            while (!succeeded) {
                                try {
                                    graph.writeVertex(sparkVertex.getFireflyId(graph.getBaseGraph().VERTEX_AERO_SET),
                                            sparkVertex.getLabel(), sparkVertex.getProperties());
                                    succeeded = true;
                                } catch (final AerospikeException e) {
                                    if (++tryCount > RETRY_LIMIT) {
                                        LOGGER.error("Failed to write vertex with ID " + sparkVertex.getId() + " after "
                                                + tryCount + " attempts.", e);
                                        if (!ignoreElementCreationFailed) {
                                            throw e;
                                        } else {
                                            break;
                                        }
                                    } else {
                                        LOGGER.warn("Failed to write vertex with ID: " + sparkVertex.getId() +
                                                        ". Attempting to write vertex again. Attempt count: " + tryCount + ".",
                                                e);
                                        exponentialBackoff(tryCount);
                                    }
                                }
                            }

                        } catch (final FireflyBulkLoaderException e) {
                            LOGGER.error("Failed to load vertex for row: " + Arrays.toString(row.values()), e);
                            if (!ignoreElementCreationFailed) {
                                throw e;
                            }
                        }
                    }
                }
                return Collections.singletonList(1).iterator();
            }, Encoders.INT()).write().format("noop").mode(SaveMode.Append).save();
        }

        // Verify vertices.
        for (final Dataset<Row> vertexDataSample : sampledVertexDatasets) {
            vertexDataSample.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
                if (ENV.equalsIgnoreCase("aws")) {
                    S3_CLIENT = AmazonS3ClientBuilder.standard().build();
                    localConfig.set(loadConfigFromS3(finalS3BucketName, finalConfigPath));
                }
                else {
                    localConfig.set(loadConfiguration(finalConfigPath));
                }
                final boolean ignoreFailedProperties =
                        Boolean.parseBoolean(getOrDefault(IGNORE_PARSE_FAILED_PROPERTIES, localConfig.get()));
                final boolean ignoreElementCreationFailed =
                        Boolean.parseBoolean(getOrDefault(IGNORE_ELEMENT_CREATION_FAILED, localConfig.get()));
                final String nullValue = getOrDefault(NULL_VALUE, localConfig.get());

                try (final FireflyGraph graph = FireflyGraph.open(localConfig.get())) {
                    final GraphTraversalSource g = graph.traversal();
                    while (rowIterator.hasNext()) {
                        final GenericRowWithSchema row = (GenericRowWithSchema) rowIterator.next();
                        final SparkFireflyVertex sparkVertex =
                                SparkFireflyVertex.createVertex(row, ignoreFailedProperties, nullValue);

                        final long id = sparkVertex.getId();
                        final GraphTraversal<Vertex, Vertex> vertexById = g.V(id);
                        Vertex v = vertexById.next();
                        if (vertexById.hasNext()) {
                            throw new AssertionError("Validation failed: More than one vertex with ID " + id
                                    + " exists");
                        }
                        if (!v.label().equals(sparkVertex.getLabel())) {
                            throw new AssertionError("Validation failed: Label did not match for vertex with ID " + id);
                        }
                        final List<Map.Entry<String, Object>> sparkVertexProperties = sparkVertex.getProperties();
                        for (final Map.Entry<String, Object> property : sparkVertexProperties) {
                            try {
                                // TODO: Handle null (when supported in Firefly) and cardinality.
                                boolean isList = property.getValue() instanceof List<?>;
                                if (isList) {
                                    final List<Object> propertyValues = new LinkedList<>((List<Object>) property.getValue());
                                    for (final Object vertexPropertyValue : (List<Object>) v.value(property.getKey())) {
                                        propertyValues.remove(vertexPropertyValue);
                                    }
                                    if (!propertyValues.isEmpty()) {
                                        throw new AssertionError("Validation failed: Property key "
                                                + property.getKey() + " on vertex with ID " + id
                                                + " did not match value " + property.getValue());
                                    }
                                } else {
                                    if (property.getValue() != null) {
                                        final Object vertexPropertyValue = v.value(property.getKey());
                                        if (!property.getValue().equals(vertexPropertyValue)) {
                                            throw new AssertionError("Validation failed: Property key "
                                                    + property.getKey() + " on vertex with ID " + id
                                                    + " did not match value " + property.getValue());
                                        }
                                    }
                                }
                            } catch (final AssertionError ae) {
                                throw ae;
                            } catch (final Exception e) {
                                throw new AssertionError("Validation failed: Property key " + property.getKey() +
                                        " on vertex with ID " + id + " did not match value " + property.getValue(), e);
                            }
                        }
                    }
                }
                return Collections.singletonList(1).iterator();
            }, Encoders.INT()).write().format("noop").mode(SaveMode.Append).save();
        }

        // Edges
        // Get the first DS in the list to use it for union in the loop.
        Dataset<Row> unionDS = edgeDatasets.get(0);
        Dataset<Row> edgeDatasetsSample = spark.emptyDataFrame();
        for (final Dataset<Row> edgeData : edgeDatasets) {
            // Invoke union to combine the edge DS.
            unionDS = unionDS.unionByName(edgeData, true).distinct();
            if (edgeDatasetsSample.isEmpty())
                edgeDatasetsSample = edgeData.sample(sampleFraction);
            else
                edgeDatasetsSample = edgeDatasetsSample.unionByName(edgeData.sample(sampleFraction), true).distinct();
        }

        // Persist the union dataframe to allow for subsequent transformations to avoid calling old transformations again.
        final Dataset<Row> persistentEdgeData = unionDS.persist(StorageLevel.DISK_ONLY());

        final Set<Long> supernodes = new HashSet<>();
        // If the edge cache is disabled globally we do not need to search for supernodes.
        if (!Boolean.parseBoolean(ConfigurationHelper.getOrDefault(EDGE_CACHE_DISABLED_GLOBALLY, CONFIG))) {
            // Csv format is: ~id, ~from, ~to, ...
            final JavaRDD<Row> edgeRDD = persistentEdgeData.javaRDD();

            // Values in csv for ~from and ~to will return as strings but are longs.
            final JavaPairRDD<Long, Long> fromPairRDD = edgeRDD.mapToPair((PairFunction<Row, Long, Long>) row ->
                    new Tuple2<>(Long.parseLong(row.getAs("~from")), 1L));
            final JavaPairRDD<Long, Long> toPairRDD = edgeRDD.mapToPair((PairFunction<Row, Long, Long>) row ->
                    new Tuple2<>(Long.parseLong(row.getAs("~to")), 1L));

            // Aggregate together by keys (sum the count of how many times a vertex ID appeared).
            final JavaPairRDD<Long, Long> fromCountPairRDD =
                    fromPairRDD.reduceByKey((Function2<Long, Long, Long>) Long::sum);
            final JavaPairRDD<Long, Long> toCountPairRDD =
                    toPairRDD.reduceByKey((Function2<Long, Long, Long>) Long::sum);

            // Get the supernode threshold from Firefly config.
            final Long supernodeThreshold = Long.parseLong(ConfigurationHelper.getOrDefault(ID_CACHE_SIZE, CONFIG));
            LOGGER.info("supernodeThreshold: " + supernodeThreshold);

            // Filter out the vertex IDs that appeared more than the supernode threshold amount of times.
            final JavaPairRDD<Long, Long> filteredFromCountPairRDD = fromCountPairRDD.filter(
                    (Function<Tuple2<Long, Long>, Boolean>)
                            longLongTuple2 -> longLongTuple2._2 > supernodeThreshold);
            LOGGER.info("filteredFromCountPairRDD sample of 10: ");
            filteredFromCountPairRDD.take(10).forEach(t -> LOGGER.info(t.toString()));
            final JavaPairRDD<Long, Long> filteredToCountPairRDD = toCountPairRDD.filter(
                    (Function<Tuple2<Long, Long>, Boolean>)
                            longLongTuple2 -> longLongTuple2._2 > supernodeThreshold);
            LOGGER.info("filteredToCountPairRDD sample of 10: ");
            filteredToCountPairRDD.take(10).forEach(t -> LOGGER.info(t.toString()));

            final JavaRDD<Long> fromSupernodes = filteredFromCountPairRDD.keys();
            LOGGER.info("fromSupernodes sample of 10: ");
            fromSupernodes.take(10).forEach(t -> LOGGER.info(t.toString()));
            final JavaRDD<Long> toSupernodes = filteredToCountPairRDD.keys();
            LOGGER.info("toSupernodes sample of 10: ");
            toSupernodes.take(10).forEach(t -> LOGGER.info(t.toString()));

            final List<Long> fromSuperNodeList = fromSupernodes.collect();
            LOGGER.info("Identified ~from supernodes: " + fromSuperNodeList);
            final List<Long> toSuperNodeList = toSupernodes.collect();
            LOGGER.info("Identified ~to supernodes: " + toSuperNodeList);

            // Combine into a tracking set.
            supernodes.addAll(fromSuperNodeList);
            supernodes.addAll(toSuperNodeList);
            LOGGER.info("Final supernodes set: " + supernodes);

            // Disable edge caches for supernodes.
            try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
                for (final Long supernodeId : supernodes) {
                    final AerospikeConnection db = graph.getBaseGraph();
                    final Bin cacheDisabledBin = new Bin(db.EDGE_CACHE_DISABLED, true);
                    int tryCount = 0;
                    boolean succeeded = false;
                    while (!succeeded) {
                        try {
                            db.getClient().put(null, new Key(db.getNamespace(), db.VERTEX_AERO_SET, supernodeId),
                                    cacheDisabledBin);
                            succeeded = true;
                        } catch (final AerospikeException e) {
                            if (++tryCount > RETRY_LIMIT) {
                                LOGGER.error("Failed to disable edge cache for vertex with ID " + supernodeId +
                                        " after " + tryCount + " attempts.", e);
                                if (!Boolean.parseBoolean(getOrDefault(IGNORE_ELEMENT_CREATION_FAILED, CONFIG))) {
                                    throw e;
                                } else {
                                    break;
                                }
                            } else {
                                LOGGER.warn("Failed to disable edge cache for vertex with ID: " + supernodeId +
                                        ". Attempting to disable cache again. Attempt count: " + tryCount + ".", e);
                                exponentialBackoff(tryCount);
                            }
                        }
                    }
                }
            }
        }

        // Write to edge caches for non-supernodes.
        persistentEdgeData.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
            LOGGER.info("PartitionId in EdgeDataset = " + TaskContext.getPartitionId()); // Numerical value
            if (ENV.equalsIgnoreCase("aws")) {
                S3_CLIENT = AmazonS3ClientBuilder.standard().build();
                localConfig.set(loadConfigFromS3(finalS3BucketName, finalConfigPath));
            } else {
                localConfig.set(loadConfiguration(finalConfigPath));
            }
            final boolean ignoreFailedProperties =
                    Boolean.parseBoolean(getOrDefault(IGNORE_PARSE_FAILED_PROPERTIES, localConfig.get()));
            final boolean useProvidedId = Boolean.parseBoolean(getOrDefault(USE_PROVIDED_EDGE_ID, localConfig.get()));
            final boolean keepProvidedId =
                    Boolean.parseBoolean(getOrDefault(KEEP_PROVIDED_EDGE_ID_AS_PROPERTY, localConfig.get()));
            final String providedIdPropertyName = getOrDefault(PROVIDED_EDGE_ID_PROPERTY_NAME, localConfig.get());
            final boolean ignoreElementCreationFailed =
                    Boolean.parseBoolean(getOrDefault(IGNORE_ELEMENT_CREATION_FAILED, localConfig.get()));
            final String nullValue = getOrDefault(NULL_VALUE, localConfig.get());

            try (final FireflyGraph graph = FireflyGraph.open(localConfig.get())) {
                final AtomicInteger outEdgeCount = new AtomicInteger(0);
                final AtomicInteger inEdgeCount = new AtomicInteger(0);
                final Map<Long, Map<String, List<Value>>> vertexOutEdgeMap = new HashMap<>();
                final Map<Long, Map<String, List<Value>>> vertexInEdgeMap = new HashMap<>();

                while (rowIterator.hasNext()) {
                    final GenericRowWithSchema row = (GenericRowWithSchema) rowIterator.next();
                    try {
                        final SparkFireflyEdge sparkEdge = SparkFireflyEdge.createEdge(row, ignoreFailedProperties,
                                useProvidedId, keepProvidedId, providedIdPropertyName, nullValue, graph);
                        final FireflyId edgeId = sparkEdge.getFireflyId(graph.getBaseGraph().EDGE_AERO_SET);
                        final long inVertexId = sparkEdge.getInVertexId();
                        final long outVertexId = sparkEdge.getOutVertexId();
                        final String edgeLabel = sparkEdge.getLabel();
                        int tryCount = 0;
                        boolean succeeded = false;
                        while (!succeeded) {
                            try {
                                graph.bulkWriteEdge(sparkEdge.getId(), edgeLabel, sparkEdge.getProperties(),
                                        inVertexId, outVertexId);
                                succeeded = true;
                            } catch (final AerospikeException e) {
                                if (++tryCount > RETRY_LIMIT) {
                                    LOGGER.error("Failed to write edge " + outVertexId + "--" + edgeLabel + "->" +
                                            inVertexId + " after " + tryCount + " attempts.", e);
                                    if (!ignoreElementCreationFailed) {
                                        throw e;
                                    } else {
                                        break;
                                    }
                                } else {
                                    LOGGER.warn("Failed to write edge " + outVertexId + "--" + edgeLabel + "->" +
                                            inVertexId + ". Attempting to write edge again. Attempt count: "
                                            + tryCount + ".", e);
                                    exponentialBackoff(tryCount);
                                    continue;
                                }
                            }

                            // Write edge to vertices' edge caches.
                            if (!graph.getBaseGraph().EDGE_CACHE_DISABLED_GLOBALLY) {
                                loadEdgeMap(graph, supernodes, outVertexId,
                                        graph.getIdFactory().createCompositeEdgeId(edgeId, graph.getIdFactory().createId(inVertexId, FireflyVertex.class)),
                                        edgeLabel, Direction.OUT, outEdgeCount, vertexOutEdgeMap,
                                        ignoreElementCreationFailed);
                                loadEdgeMap(graph, supernodes, inVertexId,
                                        graph.getIdFactory().createCompositeEdgeId(edgeId, graph.getIdFactory().createId(outVertexId, FireflyVertex.class)),
                                        edgeLabel, Direction.IN, inEdgeCount, vertexInEdgeMap,
                                        ignoreElementCreationFailed);
                            }
                        }
                    } catch (final FireflyBulkLoaderException e) {
                        LOGGER.warn("Failed to load edge for row: " + Arrays.toString(row.values()), e);
                        if (!ignoreElementCreationFailed) {
                            throw e;
                        }
                    }
                }
                flushEdgeMap(graph, Direction.OUT, vertexOutEdgeMap, ignoreElementCreationFailed);
                flushEdgeMap(graph, Direction.IN, vertexInEdgeMap, ignoreElementCreationFailed);
            }
            return Collections.singletonList(1).iterator();
        }, Encoders.INT()).write().format("noop").mode(SaveMode.Append).save();

        // Unpersist the dataframe to free up the memory.
        persistentEdgeData.unpersist();

        // Verify edges.
        edgeDatasetsSample.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
            if (ENV.equalsIgnoreCase("aws")) {
                S3_CLIENT = AmazonS3ClientBuilder.standard().build();
                localConfig.set(loadConfigFromS3(finalS3BucketName, finalConfigPath));
            } else {
                localConfig.set(loadConfiguration(finalConfigPath));
            }
            final boolean ignoreFailedProperties =
                    Boolean.parseBoolean(getOrDefault(IGNORE_PARSE_FAILED_PROPERTIES, localConfig.get()));
            final boolean useProvidedId = Boolean.parseBoolean(getOrDefault(USE_PROVIDED_EDGE_ID, localConfig.get()));
            final boolean keepProvidedId =
                    Boolean.parseBoolean(getOrDefault(KEEP_PROVIDED_EDGE_ID_AS_PROPERTY, localConfig.get()));
            final String providedIdPropertyName = getOrDefault(PROVIDED_EDGE_ID_PROPERTY_NAME, localConfig.get());
            final boolean ignoreElementCreationFailed =
                    Boolean.parseBoolean(getOrDefault(IGNORE_ELEMENT_CREATION_FAILED, localConfig.get()));
            final String nullValue = getOrDefault(NULL_VALUE, localConfig.get());
            try (final FireflyGraph graph = FireflyGraph.open(localConfig.get())) {
                final GraphTraversalSource g = graph.traversal();
                while (rowIterator.hasNext()) {
                    final GenericRowWithSchema row = (GenericRowWithSchema) rowIterator.next();
                    final SparkFireflyEdge sparkEdge = SparkFireflyEdge.createEdge(row, ignoreFailedProperties,
                            useProvidedId, keepProvidedId, providedIdPropertyName, nullValue, graph);

                    final GraphTraversal<Vertex, Edge> edgeTraversal = g.V(sparkEdge.getOutVertexId())
                            .outE(sparkEdge.getLabel()).filter(__.inV().has(T.id, sparkEdge.getInVertexId()));
                    final List<Map.Entry<String, Object>> sparkEdgeProperties = sparkEdge.getProperties();

                    // Multiple edges can exist that match the label between the FROM and TO vertices.
                    // Assume if one is found with all the properties we've succeeded.
                    boolean isEdgeFound = false;

                    edgeCheck:
                    while (edgeTraversal.hasNext() && !isEdgeFound) {
                        final Edge edge = edgeTraversal.next();

                        for (final Map.Entry<String, Object> property : sparkEdgeProperties) {
                            try {
                                // TODO: Handle null (when supported in Firefly) and cardinality.
                                boolean isList = property.getValue() instanceof List<?>;
                                if (isList) {
                                    final List<Object> propertyValues = new LinkedList<>((List<Object>) property.getValue());
                                    for (final Object propertyValue : (List<Object>) edge.value(property.getKey())) {
                                        propertyValues.remove(propertyValue);
                                    }
                                    if (!propertyValues.isEmpty()) {
                                        continue edgeCheck;
                                    }
                                } else {
                                    if (property.getValue() != null) {
                                        final Object propertyValue = edge.value(property.getKey());
                                        if (!property.getValue().equals(propertyValue)) {
                                            continue edgeCheck;
                                        }
                                    }
                                }
                            } catch (final Exception e) {
                                continue edgeCheck;
                            }
                        }

                        isEdgeFound = true;
                    }

                    if (!isEdgeFound) {
                        throw new AssertionError("Validation failed: Could not find edge with label "
                                + sparkEdge.getLabel() + " from vertex ID " + sparkEdge.getOutVertexId() + " to vertex ID "
                                + sparkEdge.getInVertexId() + " with properties " + sparkEdge.getProperties());
                    }
                }
            }
            return Collections.singletonList(1).iterator();
        }, Encoders.INT()).write().format("noop").mode(SaveMode.Append).save();

        spark.stop();
    }

    private static Configuration loadConfiguration(String configPath) {
        final Path path = Path.of(configPath);
        return getConfig(path);
    }

    static private void loadEdgeMap(final FireflyGraph graph, final Set<Long> supernodes, final long vertexId,
                                    final FireflyId cachedEdgeId, final String edgeLabel, final Direction direction,
                                    final AtomicInteger edgeCount, final Map<Long, Map<String, List<Value>>> edgeMap,
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
            if (count > EDGE_CACHE_FLUSH_THRESHOLD) {
                flushEdgeMap(graph, direction, edgeMap, ignoreElementCreationFailed);
                edgeCount.set(0);
            }
        }
    }

    static private void writeEdgesToFireflyVertex(final FireflyGraph graph, final long vertexId,
                                                  final Direction direction, final String label,
                                                  final List<Value> edgeIds,
                                                  final boolean ignoreElementCreationFailed) {
        int tryCount = 0;
        boolean successful = false;
        while (!successful) {
            try {
                graph.bulkWriteEdgesToVertexCache(graph.getIdFactory().createId(vertexId, FireflyVertex.class), direction, edgeIds, label);
                successful = true;
            } catch (final AerospikeException e) {
                if (++tryCount > RETRY_LIMIT) {
                    LOGGER.error("Failed to write edges with label " + label + " into " + direction +
                            " edge cache for vertex ID " + vertexId + " after " + tryCount + " attempts.", e);
                    if (!ignoreElementCreationFailed) {
                        throw e;
                    } else {
                        break;
                    }
                }

                if (e.getInDoubt()) {
                    LOGGER.warn("Failed to write edges with label " + label + " into " + direction +
                            " edge cache for vertex ID " + vertexId +
                            ". Write was in doubt; attempting to recover by retrying not written IDs. Attempt count: " +
                            tryCount, e);

                    int doubtTryCount = 0;
                    boolean doubtSuccessful = false;
                    while (!doubtSuccessful) {
                        try {
                            final FireflyVertex fireflyVertex = graph.readVertex(graph.getIdFactory().createId(vertexId, FireflyVertex.class));
                            doubtSuccessful = true;

                            final Iterator<Edge> edges = fireflyVertex.edges(direction, label);
                            final HashSet<FireflyId> writtenEdgeIds = new HashSet<>();
                            edges.forEachRemaining(edge -> writtenEdgeIds.add(((FireflyEdge) edge).id));

                            final List<Value> edgeIdsToRemove = new ArrayList<>();
                            for (final Value edgeId : edgeIds) {
                                final FireflyIdComposite id = (FireflyIdComposite) graph.getIdFactory().createId(edgeId.getObject(), FireflyEdge.class);
                                if (writtenEdgeIds.contains(id.getEdgeId())) {
                                    edgeIdsToRemove.add(edgeId);
                                }
                            }
                            LOGGER.warn("About to retry with {} edge IDs removed from list of size {}.",
                                    edgeIdsToRemove.size(), edgeIds.size());
                            edgeIds.removeAll(edgeIdsToRemove);
                        } catch (final AerospikeException doubtE) {
                            if (++doubtTryCount > RETRY_LIMIT) {
                                LOGGER.error("Failed to read in doubt edge IDs after " + doubtTryCount + " attempts.", e);
                                if (!ignoreElementCreationFailed) {
                                    throw e;
                                } else {
                                    break;
                                }
                            } else {
                                LOGGER.warn("Failed to read in doubt edge IDs. Attempting to read again. Attempt count: " +
                                        doubtTryCount);
                                exponentialBackoff(doubtTryCount);
                            }
                        }
                    }

                    if (edgeIds.isEmpty()) {
                        // All the edge IDs were added to the cache - stop retrying.
                        LOGGER.warn("In doubt write to edge cache was actually successful. Stopping retries.");
                        successful = true;
                    }
                } else {
                    LOGGER.warn("Failed to write edges with label " + label + " into " + direction +
                            " edge cache for vertex ID " + vertexId +
                            ". Write was not in doubt; retrying with all IDs. Attempt count: " + tryCount, e);
                    exponentialBackoff(tryCount);
                }
            }
        }
    }

    static private void flushEdgeMap(final FireflyGraph graph, final Direction direction,
                                     final Map<Long, Map<String, List<Value>>> edgeMap,
                                     final boolean ignoreElementCreationFailed) {
        for (Map.Entry<Long, Map<String, List<Value>>> vertexIdToLabelMaps : edgeMap.entrySet()) {
            final long vertexId = vertexIdToLabelMaps.getKey();
            final Map<String, List<Value>> labelMaps = vertexIdToLabelMaps.getValue();
            for (Map.Entry<String, List<Value>> labelToEdgeIds : labelMaps.entrySet()) {
                writeEdgesToFireflyVertex(graph, vertexId, direction, labelToEdgeIds.getKey(),
                        labelToEdgeIds.getValue(), ignoreElementCreationFailed);
            }
        }
        edgeMap.clear();
    }

    /**
     * Function to get a set of valid sub-directory strings of verticies or edges from a master directory.
     *
     * @param directory The master directory.
     * @return The set of valid sub-directories.
     */
    static private Set<String> getElementDirectories(final String directory) throws IOException {
        final File file = new File(directory);
        checkIfDirectoryEmpty(file);
        final File[] directories = file.listFiles(File::isDirectory);

        if (directories.length != 0) {
            for (File dir : directories)
                checkIfDirectoryEmpty(dir);
            return Arrays.stream(directories).map(File::getAbsolutePath).collect(Collectors.toSet());
        }
        return Collections.singleton(directory);
    }

    static private void checkIfDirectoryEmpty(File directory) throws IOException {
        if (Files.list(Paths.get(directory.getPath())).findAny().isEmpty()) {
            throw new IOException("Empty directory found for path: " + directory);
        }
    }

    /**
     * Function to load input files from S3.
     * This function returns all the directory paths leading upto the csv files. Does not return the csv's.
     *
     * @param bucketName Name of the S3 bucket.
     * @param folderKey  Folder key string specifying the name of the master directory of verticies or edges.
     * @return Set of directory path strings containing the csv files in S3.
     */
    static public Set<String> getObjectsListFromS3(final String bucketName, final String folderKey) {
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
     * @param bucketName Name of the S3 bucket.
     * @param path       Path to config in bucket.
     * @return Configuration object built from config file.
     */
    static public Configuration loadConfigFromS3(final String bucketName, final String path) {
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

    static public CommandLine parseCmdArgs(final String[] args) {
        final Options options = new Options();
        final Option modeOption = new Option("m", "mode", true, "local when running in IDE, cluster when running spark-submit through CLI or in AWS");
        options.addOption(modeOption);

        final Option envOption = new Option("e", "env", true, " Optional argument. aws env when running job in cluster mode in AWS.");
        options.addOption(envOption);

        final Option bucketOption = new Option("b", "bucket", true, "AWS S3 bucket name");
        options.addOption(bucketOption);

        final Option pathOption = new Option("c", "config", true, "config path [local/non-aws remote -> absolute/S3 -> full path to config.properties after bucket name]");
        options.addOption(pathOption);

        final CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (final ParseException e) {
            LOGGER.error("Error parsing configuration arguments: ", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("SparkBulkLoader", options);
            throw new IllegalArgumentException(e);
        }
    }

    static private void exponentialBackoff(final int attempt) {
        // This is to prevent overflow since we cap at 10000ms anyways.
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
