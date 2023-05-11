package com.aerospike.firefly.bulkloader.spark;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
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
import com.aerospike.firefly.bulkloader.exception.FireflyBulkLoaderException;
import com.aerospike.firefly.bulkloader.util.PropertyValueParser;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.aerospike.firefly.bulkloader.SparkBulkLoaderMain.exponentialBackoff;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

public class DatasetOperations implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetOperations.class);
    private static final int THREAD_POOL_BUFFER_SIZE = 4;
    private static final AtomicReference<Configuration> CONFIG = new AtomicReference<>();
    private static final Set<Object> SUPERNODES = new HashSet<>();
    private static final int RETRY_LIMIT = 100;
    private static final String FILENAME_COLUMN = "~fileName";
    private static final String LINENUMBER_COLUMN = "~line";
    private static final String DIRECTORY_COLUMN = "~directory";
    private static final Set<String> COLUMNSET_TO_REMOVE = new HashSet<>((Arrays.asList(DIRECTORY_COLUMN, FILENAME_COLUMN, LINENUMBER_COLUMN)));

    /**
     * Function to merge Datasets.
     *
     * @param spark     Spark session
     * @param datasets  List of Datasets to merge
     * @return Merged Dataset<Row> from all the given Datasets
     */
    public static Dataset<Row> mergeDatasets(final SparkSession spark,
                                             final List<Dataset<Row>> datasets) {
        Dataset<Row> unionDs = spark.emptyDataFrame();
        for (final Dataset<Row> dataset : datasets) {
            if (unionDs.isEmpty()) {
                unionDs = dataset.select(input_file_name().as(FILENAME_COLUMN), col("*"))
                        .withColumn(LINENUMBER_COLUMN, monotonically_increasing_id());
            } else {
                unionDs = unionDs.unionByName(dataset.select(input_file_name().as(FILENAME_COLUMN), col("*"))
                        .withColumn(LINENUMBER_COLUMN, monotonically_increasing_id()), true);
            }
        }
        return unionDs;
    }

    /**
     * Convert csv files to a list of Datasets.
     * @param spark             Spark session
     * @param csvPaths          List of paths to csv files to convert to Datasets
     * @param requiredHeaders   List of required headers in the csv files
     * @return  List of Dataset<Row> from the list of csv files
     */
    public static List<Dataset<Row>> createDatasets(final SparkSession spark, final List<String> csvPaths,
                                                    final List<String> requiredHeaders) {
        final List<Dataset<Row>> datasets = new ArrayList<>();
        for (final String csv : csvPaths) {
            final Dataset<Row> dataset = spark.read().option("header", "true").csv(csv)
                    .withColumn(DIRECTORY_COLUMN, lit(csv));
            final Set<String> headers = Set.of(dataset.columns());
            for (final String requiredHeader : requiredHeaders) {
                if (!headers.contains(requiredHeader)) {
                    throw new RuntimeException("Unable to find the required column header values '" +
                            requiredHeaders + "' in source file '" + csv + "'.");
                }
            }
            datasets.add(dataset);
        }
        return datasets;
    }

    public static boolean verifyVertexRows(final Dataset<Row> unionVertexDS,
                                           final String configPath,
                                           final String env,
                                           final String bucketName) {
        final List<Integer> failures = unionVertexDS.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
            setConfig(configPath, env, bucketName);
            final String nullValue = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE, CONFIG.get());
            final AtomicInteger failureCount = new AtomicInteger(0);
            while (rowIterator.hasNext()) {
                final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                final GenericRowWithSchema fireflyRow =  removeColumns(metadataRow, COLUMNSET_TO_REMOVE);
                try {
                    SparkFireflyVertex.createVertex(fireflyRow, nullValue);
                } catch (FireflyBulkLoaderException e) {
                    LOGGER.error("Format validation of CSV data failed on line '{}' of file {}.",
                            metadataRow.get(metadataRow.fieldIndex(LINENUMBER_COLUMN)), metadataRow.get(metadataRow.fieldIndex(DIRECTORY_COLUMN)));
                    failureCount.incrementAndGet();
                }
            }
            return Collections.singletonList(failureCount.get()).iterator();
        }, Encoders.INT()).collectAsList();
        for (final int failure : failures) {
            if (failure != 0) {
                return false;
            }
        }
        return true;
    }

    public static List<Long> writeVertices(final Dataset<Row> unionVertexDS,
                                           final String configPath,
                                           final String env,
                                           final String bucketName) {
        final List<Long> cumulativeTime = Collections.synchronizedList(new ArrayList<>());
        return unionVertexDS.mapPartitions((MapPartitionsFunction<Row, Long>) rowIterator -> {
            LOGGER.info("PartitionId in VertexDataset = " + TaskContext.getPartitionId()); // Numerical value
            setConfig(configPath, env, bucketName);
            final String nullValue = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE, CONFIG.get());

            ThreadFactory vertexThreadFactory =
                    new ThreadFactoryBuilder().setNameFormat("Vertex-write-thread-for-partition-id-" + TaskContext.getPartitionId()).build();
            final ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_BUFFER_SIZE, vertexThreadFactory);
            final List<Future<Boolean>> futures = new ArrayList<>();
            final Instant startOfGraphOperations = Instant.now();
            try (final FireflyGraph graph = FireflyGraph.open(CONFIG.get())) {
                while (rowIterator.hasNext()) {
                    final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                    final GenericRowWithSchema fireflyRow =  removeColumns(metadataRow, COLUMNSET_TO_REMOVE);

                    futures.add(executor.submit(new VertexWriteThread(nullValue, graph,
                            fireflyRow, TaskContext.getPartitionId(), metadataRow)));
                }
                boolean error = false;
                while (!futures.isEmpty()) {
                    final List<Future<Boolean>> toRemove = futures.stream().filter(Future::isDone).collect(Collectors.toList());
                    boolean errorOccurred = toRemove.stream().anyMatch(f -> {
                        try {
                            return f.get();
                        } catch (Exception e) {
                            return true;
                        }
                    });
                    error = error || errorOccurred;
                    futures.removeAll(toRemove);

                    // Wait for a second before checking again.
                    if (!futures.isEmpty()) {
                        Thread.sleep(1000);
                    }
                }
                executor.shutdown();
                final Instant endOfGraphOperations = Instant.now();
                final Duration interval = Duration.between(startOfGraphOperations, endOfGraphOperations);
                cumulativeTime.add(interval.getSeconds());
                if (error) {
                    throw new RuntimeException("Error occurred while writing vertices, see logs for more details.");
                }
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
            final String nullValue = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE, CONFIG.get());

            try (final FireflyGraph graph = FireflyGraph.open(CONFIG.get())) {
                final GraphTraversalSource g = graph.traversal();
                while (rowIterator.hasNext()) {
                    final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                    final GenericRowWithSchema fireflyRow = removeColumns(metadataRow, COLUMNSET_TO_REMOVE);
                    final SparkFireflyVertex sparkVertex =
                            SparkFireflyVertex.createVertex(fireflyRow, nullValue);

                    final Object id = sparkVertex.getId();
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

    public static boolean verifyEdgeRows(final Dataset<Row> unionVertexDS,
                                           final String configPath,
                                           final String env,
                                           final String bucketName) {
        final List<Integer> failures = unionVertexDS.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
            setConfig(configPath, env, bucketName);
            final boolean keepProvidedId =
                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY, CONFIG.get()));
            final String providedIdPropertyName = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME, CONFIG.get());
            final String nullValue = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE, CONFIG.get());
            final AtomicInteger failureCount = new AtomicInteger(0);
            while (rowIterator.hasNext()) {
                final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                final GenericRowWithSchema fireflyRow =  removeColumns(metadataRow, COLUMNSET_TO_REMOVE);
                try {
                    SparkFireflyEdge.createEdge(fireflyRow, keepProvidedId, providedIdPropertyName, nullValue, null, true);
                } catch (FireflyBulkLoaderException e) {
                    LOGGER.error("Format validation of CSV data failed on line '{}' of file {}.",
                            metadataRow.get(metadataRow.fieldIndex(LINENUMBER_COLUMN)), metadataRow.get(metadataRow.fieldIndex(DIRECTORY_COLUMN)));
                    failureCount.incrementAndGet();
                }
            }
            return Collections.singletonList(failureCount.get()).iterator();
        }, Encoders.INT()).collectAsList();
        for (final int failure : failures) {
            if (failure != 0) {
                return false;
            }
        }
        return true;
    }

    public static List<Long> writeEdges(final String configPath,
                                        final Dataset<Row> persistedEdgeDS,
                                        final String env,
                                        final String bucketName) {
        final List<Long> cumulativeTime = Collections.synchronizedList(new ArrayList<>());
        return persistedEdgeDS.mapPartitions((MapPartitionsFunction<Row, Long>) rowIterator -> {
            LOGGER.info("PartitionId in EdgeDataset = " + TaskContext.getPartitionId()); // Numerical value
            setConfig(configPath, env, bucketName);
            final boolean keepProvidedId =
                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY, CONFIG.get()));
            final String providedIdPropertyName = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME, CONFIG.get());
            final String nullValue = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE, CONFIG.get());

            ThreadFactory edgeThreadFactory =
                    new ThreadFactoryBuilder().setNameFormat("Edge-write-thread-for-partition-id-" + TaskContext.getPartitionId()).build();
            final ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_BUFFER_SIZE, edgeThreadFactory);
            final List<Future<Boolean>> futures = new ArrayList<>();
            final Instant startOfGraphOperations = Instant.now();
            try (final FireflyGraph graph = FireflyGraph.open(CONFIG.get())) {
                final Map<Object, Map<String, List<Value>>> vertexOutEdgeMap = new ConcurrentHashMap<>();
                final Map<Object, Map<String, List<Value>>> vertexInEdgeMap = new ConcurrentHashMap<>();
                while (rowIterator.hasNext()) {
                    final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                    final GenericRowWithSchema fireflyRow =  removeColumns(metadataRow, COLUMNSET_TO_REMOVE);
                    futures.add(executor.submit(
                            new EdgeWriteThread(SUPERNODES, keepProvidedId, providedIdPropertyName,
                                    nullValue, graph, vertexOutEdgeMap, vertexInEdgeMap,
                                    fireflyRow, TaskContext.getPartitionId(), metadataRow)));
                }
                boolean error = false;
                while (!futures.isEmpty()) {
                    final List<Future<Boolean>> toRemove = futures.stream().filter(Future::isDone).collect(Collectors.toList());
                    boolean errorOccurred = toRemove.stream().anyMatch(f -> {
                        try {
                            return f.get();
                        } catch (Exception e) {
                            return true;
                        }
                    });
                    error = error || errorOccurred;
                    futures.removeAll(toRemove);

                    // Wait for a second before checking again.
                    if (!futures.isEmpty()) {
                        Thread.sleep(1000);
                    }
                }
                executor.shutdown();
                if (error) {
                    throw new RuntimeException("Error occurred while writing edges, see logs for more details");
                }
                try {
                    GraphOperations.flushEdgeMap(graph, Direction.OUT, vertexOutEdgeMap);
                    GraphOperations.flushEdgeMap(graph, Direction.IN, vertexInEdgeMap);
                } catch (RuntimeException e) {
                    LOGGER.error("Failed to flush edge maps", e);
                    throw e;
                }
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
            final boolean keepProvidedId =
                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY, CONFIG.get()));
            final String providedIdPropertyName = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME, CONFIG.get());
            final String nullValue = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE, CONFIG.get());
            try (final FireflyGraph graph = FireflyGraph.open(CONFIG.get())) {
                final GraphTraversalSource g = graph.traversal();
                while (rowIterator.hasNext()) {
                    final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                    final GenericRowWithSchema fireflyRow = removeColumns(metadataRow, COLUMNSET_TO_REMOVE);
                    final SparkFireflyEdge sparkEdge = SparkFireflyEdge.createEdge(fireflyRow, keepProvidedId, providedIdPropertyName, nullValue, graph, true);

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

        // Values in csv for ~from and ~to will return as strings but can be strings or longs.
        final JavaPairRDD<Object, Long> fromPairRDD = edgeRDD.mapToPair((PairFunction<Row, Object, Long>) row ->
                new Tuple2<>(PropertyValueParser.parseId(row.getAs("~from")), 1L));

        final JavaPairRDD<Object, Long> toPairRDD = edgeRDD.mapToPair((PairFunction<Row, Object, Long>) row ->
                new Tuple2<>(PropertyValueParser.parseId(row.getAs("~to")), 1L));

        // Aggregate together by keys (sum the count of how many times a vertex ID appeared).
        final JavaPairRDD<Object, Long> fromCountPairRDD =
                fromPairRDD.reduceByKey((Function2<Long, Long, Long>) Long::sum);
        final JavaPairRDD<Object, Long> toCountPairRDD =
                toPairRDD.reduceByKey((Function2<Long, Long, Long>) Long::sum);

        // Get the supernode threshold from Firefly config.
        final Long supernodeThreshold = Long.parseLong(ConfigurationHelper.getOrDefault(ON_RECORD_ID_LIMIT, config));
        LOGGER.info("supernodeThreshold: " + supernodeThreshold);

        // Filter out the vertex IDs that appeared more than the supernode threshold amount of times.
        final JavaPairRDD<Object, Long> filteredFromCountPairRDD = fromCountPairRDD.filter(
                (Function<Tuple2<Object, Long>, Boolean>)
                        longLongTuple2 -> longLongTuple2._2 > supernodeThreshold);

        final JavaPairRDD<Object, Long> filteredToCountPairRDD = toCountPairRDD.filter(
                (Function<Tuple2<Object, Long>, Boolean>)
                        longLongTuple2 -> longLongTuple2._2 > supernodeThreshold);

        final JavaRDD<Object> fromSupernodes = filteredFromCountPairRDD.keys();
        final JavaRDD<Object> toSupernodes = filteredToCountPairRDD.keys();

        final List<Object> fromSuperNodeList = fromSupernodes.collect();
        LOGGER.info("Identified ~from supernodes: " + fromSuperNodeList);
        final List<Object> toSuperNodeList = toSupernodes.collect();
        LOGGER.info("Identified ~to supernodes: " + toSuperNodeList);

        // Combine into a tracking set.
        SUPERNODES.addAll(fromSuperNodeList);
        SUPERNODES.addAll(toSuperNodeList);
        LOGGER.info("Final supernodes set: " + SUPERNODES);

        // Disable edge caches for supernodes.
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            for (final Object supernodeId : SUPERNODES) {
                final AerospikeConnection db = graph.getBaseGraph();
                final FireflyId vertexId = graph.getIdFactory().createId(supernodeId, FireflyVertex.class);
                final Key key = getKey(db, db.VERTEX_AERO_SET, vertexId);
                final Bin cacheDisabledBin = new Bin(db.EDGE_CACHE_DISABLED, true);
                final Operation disableEdgeCache = Operation.put(cacheDisabledBin);
                int tryCount = 0;
                boolean succeeded = false;
                while (!succeeded) {
                    try {
                        db.operate(null, key, disableEdgeCache);
                        succeeded = true;
                    } catch (final AerospikeException e) {
                        if (++tryCount > RETRY_LIMIT) {
                            LOGGER.error("Failed to disable edge cache for vertex with ID " + supernodeId +
                                    " after " + tryCount + " attempts.", e);
                            throw e;
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
        } else loader = FileLoader.getInstance();
        CONFIG.set(loader.loadConfiguration(configPath));
    }

    private static GenericRowWithSchema removeColumns(GenericRowWithSchema row, Set<String> columnsToRemove) {
        Object[] values = new Object[row.size() - columnsToRemove.size()];
        StructType oldSchema = row.schema();
        StructType newSchema = new StructType(Arrays.stream(oldSchema.fields())
                .filter(field -> !columnsToRemove.contains(field.name()))
                .toArray(StructField[]::new));
        int index = 0;
        for (int i = 0; i < row.size(); i++) {
            if (!columnsToRemove.contains(row.schema().fields()[i].name())) {
                values[index] = row.get(i);
                index++;
            }
        }
        return new GenericRowWithSchema(values, newSchema);
    }
}
