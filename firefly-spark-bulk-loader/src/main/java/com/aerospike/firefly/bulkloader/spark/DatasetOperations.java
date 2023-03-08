package com.aerospike.firefly.bulkloader.spark;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.firefly.bulkloader.graph.GraphOperations;
import com.aerospike.firefly.bulkloader.spark.executorservice.EdgeWriteThread;
import com.aerospike.firefly.bulkloader.spark.executorservice.VertexWriteThread;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyVertex;
import com.aerospike.firefly.bulkloader.storage.FileLoader;
import com.aerospike.firefly.bulkloader.storage.ObjectLoader;
import com.aerospike.firefly.bulkloader.storage.S3ObjectLoader;
import com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.configuration2.Configuration;
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

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.aerospike.firefly.bulkloader.SparkBulkLoader.exponentialBackoff;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ID_CACHE_SIZE;

public class DatasetOperations implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetOperations.class);
    private static final int threadPoolBufferSize = 4;
    private static final AtomicReference<Configuration> config = new AtomicReference<>();
    private static final Set<Long> supernodes = new HashSet<>();
    private static final int RETRY_LIMIT = 100;

    /**
     * Function to load datasets from a parent directory and merge/union them
     * @param spark Spark session
     * @param directories Set of paths to subdirectories within the parent directory
     * @param REQUIRED_HEADERS required headers for the dataset
     * @return Merged Dataset<Row> from all the subdirectories
     */
    public static Dataset<Row> loadAndMergeDatasets(final SparkSession spark,
                                                    final Set<String> directories,
                                                    final String[] REQUIRED_HEADERS) {
        Dataset<Row> unionDS = spark.emptyDataFrame();
        for (final String directory : directories) {
            final Map<String, String> options = new HashMap<>();
            options.put("header", "true");
            if (unionDS.isEmpty())
                unionDS = spark.read().options(options).csv(directory);
            else
                unionDS = unionDS.unionByName(spark.read().options(options).csv(directory), true);
        }
        Set<String> headers = new HashSet<>();
        for (final String header : unionDS.columns())
            headers.add(header.toLowerCase());
        for (final String requiredHeader : REQUIRED_HEADERS) {
            if (!headers.contains(requiredHeader)) {
                throw new IllegalArgumentException("Unable to find all required column header values in source dataset: " +
                        directories);
            }
        }
        return unionDS;
    }

    public static List<Long> vertexWrite(final Dataset<Row> unionVertexDS,
                                         final String configPath,
                                         final String env,
                                         final String bucketName) {
        final List<Long> cumulativeTime = Collections.synchronizedList(new ArrayList<>());
        return unionVertexDS.mapPartitions((MapPartitionsFunction<Row, Long>) rowIterator -> {
            LOGGER.info("PartitionId in VertexDataset = " + TaskContext.getPartitionId()); // Numerical value
            setConfig(configPath, env, bucketName);
            final boolean ignoreFailedProperties =
                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.IGNORE_PARSE_FAILED_PROPERTIES, config.get()));
            final boolean ignoreElementCreationFailed =
                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.IGNORE_ELEMENT_CREATION_FAILED, config.get()));
            final String nullValue = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE, config.get());

            ThreadFactory vertexThreadFactory =
                    new ThreadFactoryBuilder().setNameFormat("Vertex-write-thread-for-partition-id-" + TaskContext.getPartitionId()).build();
            final ExecutorService executor = Executors.newFixedThreadPool(threadPoolBufferSize, vertexThreadFactory);
            final Instant startOfGraphOperations = Instant.now();
            try (final FireflyGraph graph = FireflyGraph.open(config.get())) {
                while (rowIterator.hasNext()) {
                    final GenericRowWithSchema row = (GenericRowWithSchema) rowIterator.next();
                    executor.execute(new VertexWriteThread(ignoreFailedProperties, ignoreElementCreationFailed,
                            nullValue, graph, row, TaskContext.getPartitionId()));
                }
                executor.shutdown();
                while(!executor.awaitTermination(10, TimeUnit.SECONDS)) {}
                final Instant endOfGraphOperations = Instant.now();
                final Duration interval = Duration.between(startOfGraphOperations, endOfGraphOperations);
                cumulativeTime.add(interval.getSeconds());
            }
            return cumulativeTime.iterator();
        }, Encoders.LONG()).collectAsList();
    }

    public static void verifyVertices(final Dataset<Row> sampledVertexDatasets,
                                      final String configPath,
                                      final String env,
                                      final String bucketName) {
        sampledVertexDatasets.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
            setConfig(configPath, env, bucketName);
            final boolean ignoreFailedProperties =
                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.IGNORE_PARSE_FAILED_PROPERTIES, config.get()));
            final boolean ignoreElementCreationFailed =
                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.IGNORE_ELEMENT_CREATION_FAILED, config.get()));
            final String nullValue = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE, config.get());

            try (final FireflyGraph graph = FireflyGraph.open(config.get())) {
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

    public static List<Long> writeEdges(final String configPath,
                                        final Dataset<Row> persistedEdgeDS,
                                        final String env,
                                        final String bucketName) {
        final List<Long> cumulativeTime = Collections.synchronizedList(new ArrayList<>());
        return persistedEdgeDS.mapPartitions((MapPartitionsFunction<Row, Long>) rowIterator -> {
            LOGGER.info("PartitionId in EdgeDataset = " + TaskContext.getPartitionId()); // Numerical value
            setConfig(configPath, env, bucketName);
            final boolean ignoreFailedProperties =
                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.IGNORE_PARSE_FAILED_PROPERTIES, config.get()));
            final boolean useProvidedId = Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.USE_PROVIDED_EDGE_ID, config.get()));
            final boolean keepProvidedId =
                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY, config.get()));
            final String providedIdPropertyName = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME, config.get());
            final boolean ignoreElementCreationFailed =
                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.IGNORE_ELEMENT_CREATION_FAILED, config.get()));
            final String nullValue = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE, config.get());

            ThreadFactory edgeThreadFactory =
                    new ThreadFactoryBuilder().setNameFormat("Edge-write-thread-for-partition-id-" + TaskContext.getPartitionId()).build();
            final ExecutorService executor = Executors.newFixedThreadPool(threadPoolBufferSize, edgeThreadFactory);
            final Instant startOfGraphOperations = Instant.now();
            try (final FireflyGraph graph = FireflyGraph.open(config.get())) {
                final Map<Long, Map<String, List<Value>>> vertexOutEdgeMap = new ConcurrentHashMap<>();
                final Map<Long, Map<String, List<Value>>> vertexInEdgeMap = new ConcurrentHashMap<>();
                while (rowIterator.hasNext()) {
                    final GenericRowWithSchema row = (GenericRowWithSchema) rowIterator.next();
                    executor.execute(new EdgeWriteThread(supernodes, ignoreFailedProperties, useProvidedId,
                            keepProvidedId, providedIdPropertyName, ignoreElementCreationFailed, nullValue,
                            graph, vertexOutEdgeMap, vertexInEdgeMap, row, TaskContext.getPartitionId()));
                }
                executor.shutdown();
                while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {}
                GraphOperations.flushEdgeMap(graph, Direction.OUT, vertexOutEdgeMap, ignoreElementCreationFailed);
                GraphOperations.flushEdgeMap(graph, Direction.IN, vertexInEdgeMap, ignoreElementCreationFailed);
                final Instant endOfGraphOperations = Instant.now();
                final Duration interval = Duration.between(startOfGraphOperations, endOfGraphOperations);
                cumulativeTime.add(interval.getSeconds());
            }
            return cumulativeTime.iterator();
        }, Encoders.LONG()).collectAsList();
    }

    public static void verifyEdges(final String configPath,
                                   final Dataset<Row> edgeDatasetsSample,
                                   final String env,
                                   final String bucketName) {
        edgeDatasetsSample.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
            setConfig(configPath, env, bucketName);
            final boolean ignoreFailedProperties =
                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.IGNORE_PARSE_FAILED_PROPERTIES, config.get()));
            final boolean useProvidedId = Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.USE_PROVIDED_EDGE_ID, config.get()));
            final boolean keepProvidedId =
                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY, config.get()));
            final String providedIdPropertyName = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME, config.get());
            final boolean ignoreElementCreationFailed =
                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.IGNORE_ELEMENT_CREATION_FAILED, config.get()));
            final String nullValue = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE, config.get());
            try (final FireflyGraph graph = FireflyGraph.open(config.get())) {
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
    }

    public static void extractSupernodes(final Dataset<Row> persistedEdgeDS, final Configuration config) {
        final JavaRDD<Row> edgeRDD = persistedEdgeDS.javaRDD();

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
        final Long supernodeThreshold = Long.parseLong(ConfigurationHelper.getOrDefault(ID_CACHE_SIZE, config));
        LOGGER.info("supernodeThreshold: " + supernodeThreshold);

        // Filter out the vertex IDs that appeared more than the supernode threshold amount of times.
        final JavaPairRDD<Long, Long> filteredFromCountPairRDD = fromCountPairRDD.filter(
                (Function<Tuple2<Long, Long>, Boolean>)
                        longLongTuple2 -> longLongTuple2._2 > supernodeThreshold);

        final JavaPairRDD<Long, Long> filteredToCountPairRDD = toCountPairRDD.filter(
                (Function<Tuple2<Long, Long>, Boolean>)
                        longLongTuple2 -> longLongTuple2._2 > supernodeThreshold);

        final JavaRDD<Long> fromSupernodes = filteredFromCountPairRDD.keys();
        final JavaRDD<Long> toSupernodes = filteredToCountPairRDD.keys();

        final List<Long> fromSuperNodeList = fromSupernodes.collect();
        LOGGER.info("Identified ~from supernodes: " + fromSuperNodeList);
        final List<Long> toSuperNodeList = toSupernodes.collect();
        LOGGER.info("Identified ~to supernodes: " + toSuperNodeList);

        // Combine into a tracking set.
        supernodes.addAll(fromSuperNodeList);
        supernodes.addAll(toSuperNodeList);
        LOGGER.info("Final supernodes set: " + supernodes);

        // Disable edge caches for supernodes.
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
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
                            if (!Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.IGNORE_ELEMENT_CREATION_FAILED, config))) {
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

    private static void setConfig(final String configPath, final String env, final String bucketName) {
        ObjectLoader loader;
        if (env.equalsIgnoreCase("aws")) {
            loader = S3ObjectLoader.getInstance();
            ((S3ObjectLoader) loader).setBucketName(bucketName);
        }
        else loader = FileLoader.getInstance();
        config.set(loader.loadConfiguration(configPath));
    }
}
