package com.aerospike.firefly.bulkloader.spark;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.firefly.bulkloader.exception.FireflyBulkLoaderException;
import com.aerospike.firefly.bulkloader.graph.GraphOperations;
import com.aerospike.firefly.bulkloader.spark.executorservice.EdgeWriteTask;
import com.aerospike.firefly.bulkloader.spark.resilience.ExponentialBackoffRetry;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge;
import com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper;
import com.aerospike.firefly.bulkloader.util.PropertyValueParser;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration2.MapConfiguration;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aerospike.firefly.bulkloader.SparkBulkLoaderMain.exponentialBackoff;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.COLUMNSET_TO_REMOVE;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.RETRY_LIMIT;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.THREAD_POOL_BUFFER_SIZE;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.FROM_VERTEX_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.TO_VERTEX_HEADER;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ADJACENCY_INDEX_ENABLED;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.EDGE_CACHE_DISABLED_GLOBALLY;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT;

public class EdgeOperations implements Serializable {
    public static final List<String> REQUIRED_EDGE_HEADERS = List.of(FROM_VERTEX_HEADER, TO_VERTEX_HEADER);
    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeOperations.class);
    public final List<String> edgePaths;
    private final CommandLine cmd;
    public Set<Object> SUPERNODES = new HashSet<>();
    private final Map<String, Object> config;

    public EdgeOperations(CommandLine cmd, Map<String, Object> config, List<String> edgeCSVFiles) {
        this.config = Objects.requireNonNull(config);
        this.cmd = Objects.requireNonNull(cmd);
        this.edgePaths = Objects.requireNonNull(edgeCSVFiles);
    }

    public static String getEdgeDirectory(Map<String, Object> configMap) {
        return BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.EDGE_DIRECTORY_KEY, configMap);
    }

    public static boolean dryRunEdgeRows(final Dataset<Row> edgeDataset, final Map<String, Object> config) {
        final List<Integer> failures = edgeDataset.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
            final boolean keepProvidedId =
                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY, config));
            final String providedIdPropertyName = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME, config);
            final String nullValue = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE, config);
            final AtomicInteger failureCount = new AtomicInteger(0);
            while (rowIterator.hasNext()) {
                final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNSET_TO_REMOVE);
                try {
                    SparkFireflyEdge.createEdge(fireflyRow, keepProvidedId, providedIdPropertyName, nullValue, null, true);
                } catch (FireflyBulkLoaderException e) {
                    LOGGER.error("Format validation of CSV data failed on line '{}' of file {}.",
                            metadataRow.get(metadataRow.fieldIndex(DatasetOperations.LINENUMBER_COLUMN)), metadataRow.get(metadataRow.fieldIndex(DatasetOperations.DIRECTORY_COLUMN)));
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

    public void writeEdges(final Dataset<Row> persistedEdgeDS) {
        persistedEdgeDS.foreachPartition(rowIterator -> {
            LOGGER.info("starting to write EdgeDataset in PartitionId: " + TaskContext.getPartitionId());
            final boolean keepProvidedId =
                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY, config));
            final String providedIdPropertyName = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME, config);
            final String nullValue = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE, config);

            try (final FireflyGraph graph = FireflyGraph.open(new MapConfiguration(config))) {
                LOGGER.info(String.format("graph cache disabled:  %s", graph.getBaseGraph().EDGE_CACHE_DISABLED_GLOBALLY));
                final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexOutEdgeMap = new ConcurrentHashMap<>();
                final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexInEdgeMap = new ConcurrentHashMap<>();

                ThreadFactory edgeThreadFactory =
                        new ThreadFactoryBuilder().setNameFormat("Edge-write-thread-for-partition-id-" + TaskContext.getPartitionId()).setDaemon(true).build();
                final ScheduledExecutorService ses = new ScheduledThreadPoolExecutor(THREAD_POOL_BUFFER_SIZE, edgeThreadFactory);
                ExponentialBackoffRetry retry = new ExponentialBackoffRetry(Optional.of("edge-write-partitionid-" + TaskContext.getPartitionId()));
                int bufferSize = getEdgeWriteBufferSize(config);
                LOGGER.info(String.format("edge write buffer size %d", bufferSize));

                Instant start = Instant.now();
                int batch = 1;
                int partitionId = TaskContext.getPartitionId();
                final List<Future<?>> futures = new ArrayList<>();
                while (rowIterator.hasNext()) {
                    if (futures.size() >= bufferSize) {
                        CompletableFuture megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                        megaTask.join();
                        GraphOperations.flushEdgeMap(graph, Direction.OUT, vertexOutEdgeMap);
                        GraphOperations.flushEdgeMap(graph, Direction.IN, vertexInEdgeMap);
                        final Instant end = Instant.now();
                        LOGGER.info(String.format("edge write, partitionId=%d, batch= %d, time taken(in milli-seconds)= %d, super node size: %d, cleaning all cached vertex maps", partitionId,
                                batch, Duration.between(start, end).toMillis(), SUPERNODES.size()));
                        start = Instant.now();
                        batch = batch + 1;
                        if (megaTask.isCompletedExceptionally()) {
                            throw new RuntimeException("Error occurred while writing vertices, see logs for more details");
                        }

                        futures.clear();
                    }

                    final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                    final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNSET_TO_REMOVE);

                    final EdgeWriteTask ewt = new EdgeWriteTask(retry, SUPERNODES, keepProvidedId, providedIdPropertyName, nullValue, graph, vertexOutEdgeMap, vertexInEdgeMap,
                            fireflyRow, TaskContext.getPartitionId(), metadataRow);
                    futures.add(ewt.write(ses));
                }

                LOGGER.info(String.format("done submitting edge write task, waiting for their completion in partitionId %d", TaskContext.getPartitionId()));
                CompletableFuture megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                megaTask.join();
                LOGGER.info(String.format("completed edge write task in partitionId %d", TaskContext.getPartitionId()));
                futures.clear();
                ses.shutdown();
                if (megaTask.isCompletedExceptionally()) {
                    throw new RuntimeException("Error occurred while writing edges, see logs for more details");
                } else {
                    //write everything to disk in case of everything completed
                    try {
                        GraphOperations.flushEdgeMap(graph, Direction.OUT, vertexOutEdgeMap);
                        GraphOperations.flushEdgeMap(graph, Direction.IN, vertexInEdgeMap);
                    } catch (RuntimeException e) {
                        LOGGER.error("Failed to flush edge maps", e);
                        throw e;
                    }
                }
            }
        });
    }


    public void verifySampleEdgesAfterWrite(final Dataset<Row> edgeDatasetsSample) {
        edgeDatasetsSample.foreachPartition( rowIterator -> {
            final boolean keepProvidedId =
                    Boolean.parseBoolean(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY, config));
            final String providedIdPropertyName = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME, config);
            final String nullValue = BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.NULL_VALUE, config);
            try (final FireflyGraph graph = FireflyGraph.open(new MapConfiguration(config))) {
                final GraphTraversalSource g = graph.traversal();
                while (rowIterator.hasNext()) {
                    final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                    final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNSET_TO_REMOVE);
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
        });
    }

    public void extractSupernodes(Dataset<Row> edgeDataset) {
        boolean extract = cmd.hasOption("supernode") &&
                (!Boolean.parseBoolean(
                        ConfigurationHelper.getOrDefault(EDGE_CACHE_DISABLED_GLOBALLY, new MapConfiguration(config))) ||
                        Boolean.parseBoolean(
                                ConfigurationHelper.getOrDefault(ADJACENCY_INDEX_ENABLED, new MapConfiguration(config))));
        if (extract) {
            edgeDataset.sparkSession().sparkContext()
                    .setJobGroup("Compute Supernodes", "Compute Supernodes RDD operation", true);
            LOGGER.info("supernode extraction starting...");
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
            final Long supernodeThreshold = Long.parseLong(ConfigurationHelper.getOrDefault(ON_RECORD_ID_LIMIT, new MapConfiguration(config)));
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
            fromSupernodes.union(toSupernodes).collect().
                    forEach(item -> SUPERNODES.add(item));
            LOGGER.info("Final supernodes set: " + SUPERNODES);

            // Disable edge caches for supernodes.
            disableEdgeCacheForSuperNode();
        }
    }

    private void disableEdgeCacheForSuperNode() {
        try (final FireflyGraph graph = FireflyGraph.open(new MapConfiguration(config))) {

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

    public void verifySampleEdgeAfterWrite(Dataset<Row> sampledEdgeDataset) {
        if (cmd.hasOption("verifyedge")) {
            sampledEdgeDataset.sparkSession().sparkContext().setJobGroup("Verify Edges", "Verify Edges task", true);
            LOGGER.info("verifyedge is enabled, starting the edge verification write");
            verifySampleEdgesAfterWrite(sampledEdgeDataset);
        }
    }

    public void writeEdgeToDB(Dataset<Row> edgeDataSet) {

        if (cmd.hasOption("writeedge")) {
            final Instant startWriteEdge = Instant.now();
            edgeDataSet.sparkSession().sparkContext().setJobGroup("Edges write",
                    "Edges write task", true);
            writeEdges(edgeDataSet);
            final Instant endWriteEdge = Instant.now();
            Duration edgeInterval = Duration.between(startWriteEdge, endWriteEdge);
            LOGGER.info("Execution time in seconds for Edge write task: " + edgeInterval.getSeconds());
        }
    }

    private int getEdgeWriteBufferSize(Map<String, Object> conf) {
        return  Integer.parseInt(BulkLoaderConfigHelper.getOrDefault(BulkLoaderConfigHelper.EDGE_WRITE_BUFFER, conf).trim());
    }
}
