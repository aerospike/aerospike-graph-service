package com.aerospike.firefly.bulkloader.spark;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Value;
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
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
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

import static com.aerospike.firefly.bulkloader.SparkBulkLoaderMain.exponentialBackoff;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.COLUMNS_TO_REMOVE;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.RETRY_LIMIT;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.processBatch;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.FROM_VERTEX_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.TO_VERTEX_HEADER;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.EDGE_WRITE_BUFFER;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.NULL_VALUE;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME;
import static com.aerospike.firefly.bulkloader.util.CommandLineParser.VERIFY_EDGE;
import static com.aerospike.firefly.bulkloader.util.CommandLineParser.WRITE_EDGE;
import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.GLOBAL_EDGE_CACHE_ENABLED;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT;

public class EdgeOperations implements Serializable {
    public static final List<String> REQUIRED_EDGE_HEADERS = List.of(FROM_VERTEX_HEADER, TO_VERTEX_HEADER);
    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeOperations.class);
    public final List<String> edgePaths;
    private final BulkLoaderConfigHelper config;
    public Set<Object> SUPERNODES = new HashSet<>();

    public EdgeOperations(final BulkLoaderConfigHelper config, final List<String> edgeCSVFiles) {
        this.config = Objects.requireNonNull(config);
        this.edgePaths = Objects.requireNonNull(edgeCSVFiles);
    }

    public void writeEdges(final Dataset<Row> persistedEdgeDS) {
        persistedEdgeDS.foreachPartition(rowIterator -> {
            LOGGER.info("Starting to write EdgeDataset in PartitionId: " + TaskContext.getPartitionId());
            final boolean keepProvidedId =
                    Boolean.parseBoolean(this.config.getOrDefault(KEEP_PROVIDED_EDGE_ID_AS_PROPERTY));
            final String providedIdPropertyName = this.config.getOrDefault(PROVIDED_EDGE_ID_PROPERTY_NAME);
            final String nullValue = this.config.getOrDefault(NULL_VALUE);

            try (final FireflyGraph graph = FireflyGraph.open(this.config.getFireflyConfig())) {
                LOGGER.info(String.format("Graph cache enabled:  %s", graph.getBaseGraph().GLOBAL_EDGE_CACHE_ENABLED_FLAG));
                final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexOutEdgeMap = new ConcurrentHashMap<>();
                final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexInEdgeMap = new ConcurrentHashMap<>();

                final ScheduledExecutorService executor = DatasetOperations.getScheduledThreadPoolService();
                ExponentialBackoffRetry retry = new ExponentialBackoffRetry(Optional.of("edge-write-partitionid-" + TaskContext.getPartitionId()));
                int bufferSize = getEdgeWriteBufferSize();
                LOGGER.info(String.format("Edge write buffer size %d", bufferSize));

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
                        LOGGER.info(String.format("Edge write, partitionId=%d, batch= %d, time taken(in milli-seconds)= %d, super node size: %d, cleaning all cached vertex maps", partitionId,
                                batch, Duration.between(start, end).toMillis(), SUPERNODES.size()));
                        start = Instant.now();
                        batch = batch + 1;
                        if (megaTask.isCompletedExceptionally()) {
                            throw new RuntimeException("Error occurred while writing vertices, see logs for more details");
                        }

                        futures.clear();
                    }

                    final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                    final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNS_TO_REMOVE);

                    final EdgeWriteTask ewt = new EdgeWriteTask(retry, SUPERNODES, keepProvidedId, providedIdPropertyName, nullValue, graph, vertexOutEdgeMap, vertexInEdgeMap,
                            fireflyRow, TaskContext.getPartitionId(), metadataRow);
                    futures.add(ewt.write(executor));
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
                        GraphOperations.flushEdgeMap(graph, Direction.OUT, vertexOutEdgeMap);
                        GraphOperations.flushEdgeMap(graph, Direction.IN, vertexInEdgeMap);
                    } catch (final RuntimeException e) {
                        LOGGER.error("Failed to flush Vertex Edge cache maps", e);
                        throw e;
                    }
                }
            }
        });
    }

    public void verifySampleEdgesAfterWrite(final Dataset<Row> edgeDatasetsSample) {
        final String errorMessage= "Error occurred while verifying edges; see logs for more details.";
        final int bufferSize = getEdgeWriteBufferSize();
        edgeDatasetsSample.foreachPartition( rowIterator -> {
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
                                verifyEdge(metadataRow, keepProvidedId, providedIdPropertyName, nullValue, graph, g);
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

    private static void verifyEdge(GenericRowWithSchema metadataRow, boolean keepProvidedId, String providedIdPropertyName, String nullValue, FireflyGraph graph, GraphTraversalSource g) {
        final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNS_TO_REMOVE);
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
            throw new AssertionError("Validation failed: Could not find Edge with label "
                    + sparkEdge.getLabel() + " from Vertex ID " + sparkEdge.getOutVertexId() + " to Vertex ID "
                    + sparkEdge.getInVertexId() + " with properties " + sparkEdge.getProperties());
        }
    }

    public void extractSupernodes(Dataset<Row> edgeDataset) {
        final Configuration fireflyConfig = this.config.getFireflyConfig();
        // If the global edge cache flag is off, then all vertices written have their edge caches disabled upon
        // creation. No need to find and disable them.
        boolean extract = Boolean.parseBoolean(ConfigurationHelper.getOrDefaultString(GLOBAL_EDGE_CACHE_ENABLED, fireflyConfig));
        if (extract) {
            String taskName= "Compute Supernodes";
            edgeDataset.sparkSession().sparkContext()
                    .setJobGroup("Compute Supernodes", "Compute Supernodes RDD operation", true);
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
                    forEach(item -> SUPERNODES.add(item));
            LOGGER.info("Final supernodes set: " + SUPERNODES);

            // Disable edge caches for supernodes.
            disableEdgeCacheForSuperNode();
        }
    }

    private void disableEdgeCacheForSuperNode() {
        try (final FireflyGraph graph = FireflyGraph.open(this.config.getFireflyConfig())) {
            for (final Object supernodeId : SUPERNODES) {
                final AerospikeConnection db = graph.getBaseGraph();
                final FireflyId vertexId = graph.getIdFactory().createId(supernodeId, FireflyVertex.class);
                final Key key = getKey(db, db.VERTEX_AERO_SET, vertexId);
                final Bin cacheDisabledBin = new Bin(db.EDGE_CACHE_DISABLED_BIN, true);
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

    public void verifySampleEdgeAfterWrite(final Dataset<Row> sampledEdgeDataset) {
        if (this.config.hasAction(VERIFY_EDGE)) {
            final String taskName = "Verify Edges";
            sampledEdgeDataset.sparkSession().sparkContext().setJobGroup(taskName,"Verify Edges task", true);
            LOGGER.info("verifyedge is enabled, starting the Edge write verification.");
            verifySampleEdgesAfterWrite(sampledEdgeDataset);
            sampledEdgeDataset.sparkSession().sparkContext().cancelJobGroup(taskName);
        }
    }

    public void writeEdgeToDB(final Dataset<Row> edgeDataSet) {
        if (this.config.hasAction(WRITE_EDGE)) {
            final Instant startWriteEdge = Instant.now();
            final String taskName = "Edges write";
            edgeDataSet.sparkSession().sparkContext().setJobGroup(taskName,
                    "Edges write task", true);
            writeEdges(edgeDataSet);
            final Instant endWriteEdge = Instant.now();
            final Duration edgeInterval = Duration.between(startWriteEdge, endWriteEdge);
            edgeDataSet.sparkSession().sparkContext().cancelJobGroup(taskName);
            LOGGER.info("Execution time in seconds for Edge write task: " + edgeInterval.getSeconds());
        }
    }

    private int getEdgeWriteBufferSize() {
        return Integer.parseInt(this.config.getOrDefault(EDGE_WRITE_BUFFER).trim());
    }
}
