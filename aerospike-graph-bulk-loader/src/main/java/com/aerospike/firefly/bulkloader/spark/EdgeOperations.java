package com.aerospike.firefly.bulkloader.spark;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.firefly.bulkloader.graph.GraphOperations;
import com.aerospike.firefly.bulkloader.spark.executorservice.EdgeWriteTask;
import com.aerospike.firefly.bulkloader.spark.resilience.ExponentialBackoffRetry;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge;
import com.aerospike.firefly.bulkloader.util.PropertyValueParser;
import com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper;
import com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyLoadingException;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.aerospike.firefly.bulkloader.SparkBulkLoaderMain.exponentialBackoff;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.COLUMNS_TO_REMOVE;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.EDGE_ID_COLUMN;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.RETRY_LIMIT;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.processBatch;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.FROM_VERTEX_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.TO_VERTEX_HEADER;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.READ_ONLY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.EDGE_WRITE_BUFFER;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.NULL_VALUE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.VERIFY_OUTPUT_DATA;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.DISABLE_EDGE_WRITE;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.GLOBAL_EDGE_CACHE_ENABLED;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT;


public class EdgeOperations implements Serializable {
    public static final List<String> REQUIRED_EDGE_HEADERS = List.of(FROM_VERTEX_HEADER, TO_VERTEX_HEADER);
    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeOperations.class);
    public final List<String> edgePaths;
    private final BulkLoaderConfigHelper config;
    private Set<Object> supernodes = new HashSet<>();
    final boolean keepProvidedId;
    final String providedIdPropertyName;
    final String nullValue;
    final boolean usePersistedEdgeId;

    public EdgeOperations(final BulkLoaderConfigHelper config, final List<String> edgeCSVFiles) {
        this.config = Objects.requireNonNull(config);
        this.edgePaths = Objects.requireNonNull(edgeCSVFiles);
        this.keepProvidedId =
                Boolean.parseBoolean(this.config.getOrDefault(KEEP_PROVIDED_EDGE_ID_AS_PROPERTY));
        this.providedIdPropertyName = this.config.getOrDefault(PROVIDED_EDGE_ID_PROPERTY_NAME);
        this.nullValue = this.config.getOrDefault(NULL_VALUE);
        this.usePersistedEdgeId = !config.hasAction(READ_ONLY);
    }

    public void writeEdges(final Dataset<Row> persistedEdgeDS) {
        persistedEdgeDS.foreachPartition(rowIterator -> {
            LOGGER.info("Starting to write EdgeDataset in PartitionId: " + TaskContext.getPartitionId());

            try (final FireflyGraph graph = FireflyGraph.open(this.config.getFireflyConfig())) {
                LOGGER.info(String.format("Graph cache enabled:  %s", graph.getBaseGraph().GLOBAL_EDGE_CACHE_ENABLED_FLAG));
                final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexOutEdgeMap = new ConcurrentHashMap<>();
                final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexInEdgeMap = new ConcurrentHashMap<>();

                final ScheduledExecutorService executor = DatasetOperations.getScheduledThreadPoolService();
                ExponentialBackoffRetry retry = new ExponentialBackoffRetry("edge-write-partitionid-" + TaskContext.getPartitionId());
                int bufferSize = getEdgeWriteBufferSize();
                LOGGER.info(String.format("Edge write buffer size %d", bufferSize));

                Instant start = Instant.now();
                int batch = 1;
                int partitionId = TaskContext.getPartitionId();
                final List<CompletionStage<Void>> futures = new ArrayList<>();
                while (rowIterator.hasNext()) {
                    if (futures.size() >= bufferSize) {
                        CompletableFuture megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                        megaTask.join();
                        writeEdgeCacheToDB(graph, vertexOutEdgeMap, vertexInEdgeMap);
                        LOGGER.info(String.format("Edge write, partitionId=%d, batch= %d, time taken(in milli-seconds)= %d, super node size: %d, cleaning all cached vertex maps", partitionId,
                                batch, Duration.between(start, Instant.now()).toMillis(), supernodes.size()));
                        start = Instant.now();
                        batch = batch + 1;
                        if (megaTask.isCompletedExceptionally()) {
                            throw new RuntimeException("Error occurred while writing vertices, see logs for more details");
                        }
                        futures.clear();
                    }

                    final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next().copy();
                    final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, DatasetOperations.COLUMNS_TO_REMOVE);

                    final EdgeWriteTask ewt = new EdgeWriteTask(retry, supernodes, keepProvidedId, providedIdPropertyName, nullValue, graph, vertexOutEdgeMap, vertexInEdgeMap,
                            fireflyRow, metadataRow, usePersistedEdgeId);
                    futures.add(ewt.write(executor).thenRunAsync(() -> ewt.updateCacheMap(),executor));
                }

                LOGGER.info(String.format("Done submitting edge write task; waiting for their completion in partitionId %d", TaskContext.getPartitionId()));
                CompletableFuture megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                megaTask.join();
                LOGGER.info(String.format("Completed edge write task in partitionId %d", TaskContext.getPartitionId()));
                futures.clear();
                if (megaTask.isCompletedExceptionally()) {
                    throw new RuntimeException("Error occurred while writing edges; see logs for more details");
                } else {
                    // Flush Vertex Edge cache maps when all Edge writes are done.
                    try {
                        writeEdgeCacheToDB(graph, vertexOutEdgeMap, vertexInEdgeMap);
                    } catch (final RuntimeException e) {
                        LOGGER.error("Failed to flush Vertex Edge cache maps", e);
                        throw e;
                    }
                }
            }
        });
    }


    private static void writeEdgeCacheToDB(FireflyGraph graph, ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexOutEdgeMap, ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexInEdgeMap) {
        GraphOperations.flushEdgeMap(graph, Direction.OUT, vertexOutEdgeMap);
        GraphOperations.flushEdgeMap(graph, Direction.IN, vertexInEdgeMap);
    }

    public void verifySampleEdgesAfterWrite(final Dataset<Row> edgeDatasetsSample) {
        final String errorMessage= "Error occurred while verifying edges; see logs for more details.";
        final int bufferSize = getEdgeWriteBufferSize();
        boolean hasEdgeID = Arrays.asList(edgeDatasetsSample.schema().fieldNames()).contains(EDGE_ID_COLUMN);
        edgeDatasetsSample.foreachPartition(rowIterator -> {
            final boolean keepProvidedId =
                    Boolean.parseBoolean(this.config.getOrDefault(KEEP_PROVIDED_EDGE_ID_AS_PROPERTY));
            final String providedIdPropertyName = this.config.getOrDefault(PROVIDED_EDGE_ID_PROPERTY_NAME);
            final String nullValue = this.config.getOrDefault(NULL_VALUE);
            try (final FireflyGraph graph = FireflyGraph.open(this.config.getFireflyConfig())) {
                final GraphTraversalSource g = graph.traversal();
                int batch = 1;
                final List<Future<?>> futures = new ArrayList<>();
                final ScheduledExecutorService ses = DatasetOperations.getScheduledThreadPoolService();
                while (rowIterator.hasNext()) {
                    batch = processBatch(bufferSize, batch, futures, errorMessage);
                    GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                    futures.add(CompletableFuture.supplyAsync(() -> {
                        verifyEdge(metadataRow, keepProvidedId, providedIdPropertyName, nullValue, graph, g, hasEdgeID);
                        return null;
                    }, ses).exceptionally(e -> {
                        LOGGER.error(String.format("Exception occurred in verifying Edge row"), e);  // Log the error when final failure happens
                        throw new RuntimeException(e);
                    }));
                }
                final CompletableFuture megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                megaTask.join();
                if (megaTask.isCompletedExceptionally()) {
                    throw new RuntimeException("Error occurred while verifying Edges, see logs for more details");
                }
                futures.clear();
            }
        });
    }

    private static void verifyEdge(final GenericRowWithSchema metadataRow, boolean keepProvidedId,
                                   final String providedIdPropertyName, final String nullValue,
                                   final FireflyGraph graph, final GraphTraversalSource g,
                                   final boolean hasEdgeId) {
        int tryCount = 0;
        // Multiple edges can exist that match the label between the FROM and TO vertices.
        // Assume if one is found with all the properties we've succeeded.
        boolean isEdgeFound = false;

        while (!isEdgeFound) {
            try {
                final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNS_TO_REMOVE);
                final SparkFireflyEdge sparkEdge = SparkFireflyEdge.createEdge(fireflyRow, keepProvidedId,
                        providedIdPropertyName, nullValue, graph, true, getEdgeIdSupplied(metadataRow, hasEdgeId));

                final GraphTraversal<Vertex, Edge> edgeTraversal = g.V(sparkEdge.getOutVertexId())
                        .outE(sparkEdge.getLabel()).filter(__.inV().has(T.id, sparkEdge.getInVertexId()));
                final List<Map.Entry<String, Object>> sparkEdgeProperties = sparkEdge.getProperties();

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
                    throw new AssertionError("Validation failed: Could not find Edge with label "
                            + sparkEdge.getLabel() + " from Vertex ID " + sparkEdge.getOutVertexId() + " to Vertex ID "
                            + sparkEdge.getInVertexId() + " with properties " + sparkEdge.getProperties());
                }
            } catch (final AerospikeException ae) {
                final FireflyLoadingException fle = new FireflyLoadingException(ae);
                if (!fle.isRetryable()) {
                    LOGGER.error("Failed to verify loaded Edge due to non-retryable error: " + metadataRow, ae);
                    throw ae;
                } else if (++tryCount > RETRY_LIMIT) {
                    LOGGER.error("Failed to verify loaded Edge after " + tryCount + " attempts: " + metadataRow, ae);
                    throw ae;
                } else {
                    LOGGER.warn("Failed to verify loaded Edge: " + metadataRow + ". Attempt count: " + tryCount, ae);
                    exponentialBackoff(tryCount);
                }
            }
        }
    }

    /***
     * Extracts {@link DatasetOperations#EDGE_ID_COLUMN} from a DataFrame Row if present, else returns null.
     * @param metadataRow
     * @param usePersistedEdgeId
     * @return
     */
    @Nullable
    public static byte[] getEdgeIdSupplied(final GenericRowWithSchema metadataRow, boolean usePersistedEdgeId) {
        return usePersistedEdgeId ? EdgeOperations.decodeEdgeIDFromString(metadataRow.getAs(EDGE_ID_COLUMN)) : null;
    }

    public Set<Object> extractSupernodes(final Dataset<Row> edgeDataset) {
        final Configuration fireflyConfig = this.config.getFireflyConfig();
        // If the global edge cache flag is off, then all vertices written have their edge caches disabled upon
        // creation. No need to find and disable them.
        boolean extract = Boolean.parseBoolean(ConfigurationHelper.getOrDefaultString(GLOBAL_EDGE_CACHE_ENABLED, fireflyConfig));
        if (extract) {
            final String taskName = "Compute Supernodes";
            edgeDataset.sparkSession().sparkContext()
                    .setJobGroup(taskName, "Compute Supernodes RDD operation", true);
            LOGGER.info("Supernode extraction starting...");
            // Csv format is: ~id, ~from, ~to, ...
            final JavaRDD<Row> edgeRDD = edgeDataset.javaRDD();

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
            final Long supernodeThreshold = Long.parseLong(ConfigurationHelper.getOrDefaultString(ON_RECORD_ID_LIMIT, fireflyConfig));
            LOGGER.info("Supernode threshold: " + supernodeThreshold);

            // Filter out the vertex IDs that appeared more than the supernode threshold amount of times.
            final JavaPairRDD<Object, Long> filteredFromCountPairRDD = fromCountPairRDD.filter(
                    (Function<Tuple2<Object, Long>, Boolean>)
                            longLongTuple2 -> longLongTuple2._2 > supernodeThreshold);

            final JavaPairRDD<Object, Long> filteredToCountPairRDD = toCountPairRDD.filter(
                    (Function<Tuple2<Object, Long>, Boolean>)
                            longLongTuple2 -> longLongTuple2._2 > supernodeThreshold);

            final JavaRDD<Object> fromSupernodes = filteredFromCountPairRDD.keys();
            final JavaRDD<Object> toSupernodes = filteredToCountPairRDD.keys();
            fromSupernodes.union(toSupernodes).collect().
                    forEach(item -> supernodes.add(item));
            LOGGER.info("Final supernodes set: " + supernodes);
        }
        return this.supernodes;
    }

    public void verifySampleEdgeAfterWrite(final Dataset<Row> sampledEdgeDataset) {
        if (this.config.hasAction(VERIFY_OUTPUT_DATA)) {
            final String taskName = "Verify Edges";
            sampledEdgeDataset.sparkSession().sparkContext().setJobGroup(taskName,"Verify Edges task", true);
            LOGGER.info("verify_output_data is enabled, starting the Edge write verification.");
            verifySampleEdgesAfterWrite(sampledEdgeDataset);
            sampledEdgeDataset.sparkSession().sparkContext().cancelJobGroup(taskName);
        }
    }

    private class EdgeIDAdditionFunction implements MapPartitionsFunction, Serializable {
        final Map<String, Object> fileConfig ;
        final StructType schema;

        public EdgeIDAdditionFunction(final Map<String, Object> fileConfig, final StructType writeSchema) {
            this.fileConfig = fileConfig;
            this.schema = writeSchema;
        }
        @Override
        public Iterator call(final Iterator input) {
            return new EdgeIDOutputIterator(fileConfig, input, schema);
        }
    }

    private class EdgeIDOutputIterator implements Iterator<Row>, Serializable {
        private long counter = 0L;
        private final Logger ITER_LOGGER = LoggerFactory.getLogger(EdgeIDOutputIterator.class);
        final Map<String, Object> config;
        final Iterator<Row> data;
        final StructType schema;
        final Instant start;

        private final Supplier<FireflyGraph> graphSupplier = new Supplier<>() {
            private FireflyGraph instance = null;
            @Override
            public FireflyGraph get() {
                if (instance == null) {
                    synchronized (this) {
                        if (instance == null ) {
                            instance = FireflyGraph.open(new MapConfiguration(config));
                        }
                    }
                }
                return instance;
            }
        };

        public FireflyGraph getFireflyGraph() {
            return graphSupplier.get();
        }
        public EdgeIDOutputIterator(final Map<String, Object> conf, final Iterator<Row> data, final StructType writeSchema) {
            start = Instant.now();
            this.config = conf;
            this.data = data;
            this.schema = writeSchema;
        }

        @Override
        public boolean hasNext() {
            final boolean hasMore = data.hasNext();
            if(!hasMore) {
                ITER_LOGGER.info("Done processing edgeID write task in partition:{}, rows:{}, time taken(in seconds):{}", TaskContext.getPartitionId(), counter, Duration.between(start, Instant.now()).getSeconds());
                getFireflyGraph().close();
            }
            return hasMore;
        }

        @Override
        public Row next() {
            counter++;
            final Row input = data.next();
            final List<Object> outputRow = new ArrayList<>();
            for (int i = 0; i < input.size(); i++) {
                outputRow.add(input.get(i));
            }
            //add the edgeID
            final byte[] nextId =getFireflyGraph().edgeIdManager.getNextId(getFireflyGraph());
            outputRow.add(encodeID(nextId)); //encode using our custom encoder
            return new GenericRowWithSchema(outputRow.toArray(), schema);
        }
    }

    /***
     * Assigns each edge record with an ~edgeID and writes them to user specified location.
     * @param edgeDataSet
     * @param writeLocation
     * @param config
     */
    public void writeEdgeIDsToStorage(final Dataset<Row> edgeDataSet, final String writeLocation,
                                      final Map<String, Object> config) {
        final Instant startWriteEdge = Instant.now();
        final String taskName = "Edges ID write";
        final StructType writeSchema = edgeDataSet.schema().add(DataTypes.createStructField(EDGE_ID_COLUMN, DataTypes.StringType, false));
        edgeDataSet.sparkSession().sparkContext().setJobGroup(taskName,
                "Edges ID write task", true);
        final ExpressionEncoder<Row> encoder = RowEncoder.apply(writeSchema);
        final Dataset<Row> EdgeIdDF =  edgeDataSet.mapPartitions(new EdgeIDAdditionFunction(config, writeSchema), encoder);
        EdgeIdDF.write().option("header",true).mode(SaveMode.Overwrite).option("compression","bzip2").csv(writeLocation);
        edgeDataSet.sparkSession().sparkContext().cancelJobGroup(taskName);
        LOGGER.info("Execution time in seconds for Edge ID write task: " + Duration.between(startWriteEdge, Instant.now()).getSeconds());
    }

    /***
     * Converts byte[] id to String by converting individual component to string and concatenating them with a delimiter ":"
     * @param arr id generated by id manager
     * @return encoded
     */
    private static String encodeID(final byte[] arr) {

        // Isolate bytes from both component
        final byte[] recycleBytes = new byte[8];
        final byte[] numericBytes = new byte[8];
        System.arraycopy(arr, 0, recycleBytes,0, 8);
        System.arraycopy(arr, 8, numericBytes, 0, 8);

        // Extract numeric content from both components
        final ByteBuffer bb = ByteBuffer.wrap(recycleBytes);
        final Long recycleId = bb.getLong();
        final ByteBuffer bb1 = ByteBuffer.wrap(numericBytes);
        final Long numericID = bb1.getLong();

        // Encode each number to string using a delimiter
        final StringBuilder sb = new StringBuilder();
        sb.append(recycleId);
        sb.append(":");
        sb.append(numericID);
        return sb.toString();
    }

    /***
     * Decode the Stringified encoded edgeId to byte[] as it was originally generated by @
     * @param idString edgeIds read from file
     * @return byte[] id generated equivalent to ID manager.
     */
    public static byte[] decodeEdgeIDFromString(final String idString) {
        if (idString == null) {
            return null;
        }

        if (idString.trim().isEmpty()) {
            throw new RuntimeException("idString was empty!");
        }

        final String[] tokens = idString.split(":");
        Preconditions.checkArgument(tokens.length == 2, String.format("Tokenized idString must have only 2 tokes, found %d", tokens.length));

        final Long recycleId = Objects.requireNonNull(Long.valueOf(tokens[0]), "parsed recycleId can't be null");
        final Long numericID = Objects.requireNonNull(Long.valueOf(tokens[1]), "parsed numericId can't be null");

        final byte[] recycleByte = Longs.toByteArray(recycleId);
        final byte[] numByte = Longs.toByteArray(numericID);

        final byte[] graphID = new byte[16];
        System.arraycopy(recycleByte, 0, graphID,0, 8);
        System.arraycopy(numByte, 0, graphID, 8, 8);

        return  graphID;
    }

    public void writeEdgeToDB(final Dataset<Row> edgeIdDataSet) {
        if (!this.config.hasAction(DISABLE_EDGE_WRITE)) {
            final Instant startWriteEdge = Instant.now();
            final String taskName = "Edges write to Aerospike Database";
            edgeIdDataSet.sparkSession().sparkContext().setJobGroup(taskName,
                    "Edges write task", true);
            writeEdges(edgeIdDataSet);
            edgeIdDataSet.sparkSession().sparkContext().cancelJobGroup(taskName);
            LOGGER.info("Execution time in seconds for Edge write task: " + Duration.between(startWriteEdge, Instant.now()).getSeconds());
        }
    }

    private int getEdgeWriteBufferSize() {
        return Integer.parseInt(this.config.getOrDefault(EDGE_WRITE_BUFFER).trim());
    }
}
