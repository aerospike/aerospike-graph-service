package com.aerospike.firefly.bulkloader.spark;

import com.aerospike.client.Value;
import com.aerospike.firefly.bulkloader.graph.GraphOperations;
import com.aerospike.firefly.bulkloader.spark.executorservice.EdgeWriteTask;
import com.aerospike.firefly.bulkloader.spark.resilience.ExponentialBackoffRetry;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge;
import com.aerospike.firefly.bulkloader.util.PropertyValueParser;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper;
import com.aerospike.firefly.process.call.bulkload.utils.exception.BadCsvEntryException;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
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
import org.apache.spark.storage.StorageLevel;
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
import scala.Tuple3;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import java.util.stream.Collectors;

import static com.aerospike.firefly.bulkloader.SparkBulkLoaderMain.exponentialBackoff;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.COLUMNS_TO_REMOVE;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.EDGE_ID_COLUMN;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.PACKING_ID_COLUMN;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.RETRY_LIMIT;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.processBatch;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.FROM_VERTEX_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.TO_VERTEX_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyElement.ID_HEADER;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ALLOWED_BAD_EDGES_COUNT;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ALLOWED_BAD_ENTRY_COUNT;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.DISABLE_EDGE_WRITE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.EDGE_WRITE_BUFFER;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.NULL_VALUE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.READ_ONLY;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.RECOVERY_FAILURE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.VERIFY_OUTPUT_DATA;
import static com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyLoadingException.isRetryable;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.GLOBAL_EDGE_CACHE_ENABLED;
import static org.apache.spark.sql.functions.col;

public class EdgeOperations implements Serializable {
    public static final List<String> REQUIRED_EDGE_HEADERS = List.of(FROM_VERTEX_HEADER, TO_VERTEX_HEADER);
    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeOperations.class);
    private static final String NOT_RECYCLED_ID_TOKEN = "nr";
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
                config.getOrDefaultBool(KEEP_PROVIDED_EDGE_ID_AS_PROPERTY);
        this.providedIdPropertyName = this.config.getOrDefault(PROVIDED_EDGE_ID_PROPERTY_NAME);
        this.nullValue = this.config.getOrDefault(NULL_VALUE);
        this.usePersistedEdgeId = !config.hasAction(READ_ONLY);
    }

    public void writeEdges(final Dataset<Row> persistedEdgeDS,
                           final Set<Long> completedEdgePartitions,
                           final boolean readOnly, final boolean generateEdgeCaches) {
        persistedEdgeDS.foreachPartition(rowIterator -> {
            final int partitionId = TaskContext.getPartitionId();
            if (!readOnly) {
                final Long partitionIdLong = Long.valueOf(partitionId);
                if (completedEdgePartitions.contains(partitionIdLong)) {
                    LOGGER.info("Edges PartitionId " + partitionId + " is already written, skipping.");
                    return;
                }
            }

            // This is a testing config, used to force failure in specific spots to allow us to test the recovery modes.
            final String recoveryFailure = config.getOrDefault(RECOVERY_FAILURE);
            if (recoveryFailure != null && recoveryFailure.startsWith("EDGE_WRITE")) {
                // The failure position is supplied as "EDGE_WRITE:<partition_id>".
                final int partitionToFailOn = recoveryFailure.split(":", 2).length > 1 ? Integer.parseInt(recoveryFailure.split(":", 2)[1]) : -1;
                if (partitionToFailOn == -1) {
                    // If a specific partition was not supplied, fail instantly.
                    throw new RuntimeException("Failed to get partition to fail on from recovery failure property, please contact support.");
                } else {
                    // Otherwise fail on the specific partition a few mins later to allow other partitions to complete.
                    if (partitionId == partitionToFailOn) {
                        // Wait so other partitions can complete before we fail this partition.
                        try {
                            Thread.sleep(300000);
                        } catch (final InterruptedException ignored) {
                        }
                        throw new RuntimeException("Testing recovery failure, please contact support.");
                    }
                }
            }

            LOGGER.info("Starting to write EdgeDataset in PartitionId: " + partitionId);

            try (final FireflyGraph graph = FireflyGraph.open(this.config.getFireflyConfig())) {
                final long allowedDetachedEdges = this.config.getOrDefaultInt(ALLOWED_BAD_EDGES_COUNT);
                graph.fireflySummaryUpdater.startEdgePartition(partitionId);
                LOGGER.info(String.format("Graph cache enabled:  %s", graph.getBaseGraph().GLOBAL_EDGE_CACHE_ENABLED_FLAG));
                final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexOutEdgeMap = new ConcurrentHashMap<>();
                final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexInEdgeMap = new ConcurrentHashMap<>();
                final List<Tuple3<FireflyId, Object, Object>> edgeToFromIdList = new ArrayList<>();
                Long currentId = null;
                List<EdgeWriteTask> tasks = new ArrayList<>();
                final long allowBadEntryCount = this.config.getOrDefaultInt(ALLOWED_BAD_ENTRY_COUNT);
                final ScheduledExecutorService executor = DatasetOperations.getScheduledThreadPoolService();
                final ExponentialBackoffRetry retry = new ExponentialBackoffRetry("edge-write-partitionid-" + partitionId);
                final int bufferSize = getEdgeWriteBufferSize();
                LOGGER.info(String.format("Edge write buffer size %d", bufferSize));

                final Instant totalStart = Instant.now();
                Instant start = Instant.now();
                int batch = 1;
                final List<CompletionStage<Void>> futures = new ArrayList<>();
                while (rowIterator.hasNext()) {
                    if (futures.size() >= bufferSize) {
                        final CompletableFuture megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                        megaTask.join();
                        if (!generateEdgeCaches) {
                            writeEdgeCacheToDB(graph, vertexOutEdgeMap, vertexInEdgeMap);
                        } else {
                            removeDetatchedEdgesPreGenerated(graph, edgeToFromIdList, bufferSize, allowedDetachedEdges);
                        }
                        LOGGER.info(String.format("Edge write, partitionId: %d, batch: %d, time taken(in milli-seconds): %d, super node size: %d, cleaning all cached vertex maps", partitionId,
                                batch, Duration.between(start, Instant.now()).toMillis(), supernodes.size()));
                        start = Instant.now();
                        batch = batch + 1;
                        if (megaTask.isCompletedExceptionally()) {
                            throw new RuntimeException("Error occurred while writing edges - see logs for more details");
                        }
                        futures.clear();
                    }

                    final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next().copy();
                    final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, DatasetOperations.COLUMNS_TO_REMOVE);

                    try {
                        if (generateEdgeCaches) {
                            final Long packingId = metadataRow.getLong(metadataRow.fieldIndex(PACKING_ID_COLUMN));
                            final EdgeWriteTask ewt = new EdgeWriteTask(retry, supernodes, keepProvidedId,
                                    providedIdPropertyName, nullValue, graph, vertexOutEdgeMap, vertexInEdgeMap, fireflyRow,
                                    metadataRow, usePersistedEdgeId, partitionId);
                            edgeToFromIdList.add(new Tuple3<>(ewt.edgeId, ewt.inVertexId, ewt.outVertexId));
                            if (!packingId.equals(currentId)) {
                                // Kick off the previous batch.
                                futures.add(EdgeWriteTask.writeBatch(executor, graph, tasks));

                                // Set up next batch.
                                tasks.clear();
                                currentId = packingId;
                            }
                            tasks.add(ewt);
                        } else {
                            final EdgeWriteTask ewt = new EdgeWriteTask(retry, supernodes, keepProvidedId,
                                    providedIdPropertyName, nullValue, graph, vertexOutEdgeMap, vertexInEdgeMap, fireflyRow,
                                    metadataRow, usePersistedEdgeId, partitionId);
                            futures.add(ewt.write(executor).thenRunAsync(ewt::updateCacheMap, executor));
                        }
                    } catch (final BadCsvEntryException e) {
                        if (allowBadEntryCount == 0) {
                            throw e;
                        }
                    }
                }

                if (generateEdgeCaches) {
                    // Kick off the last batch.
                    futures.add(EdgeWriteTask.writeBatch(executor, graph, tasks));
                }

                LOGGER.info(String.format("Done submitting edge write task; waiting for their completion in partitionId %d", partitionId));
                final CompletableFuture megaTask = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
                megaTask.join();
                LOGGER.info(String.format("Completed edge write task in partitionId %d", partitionId));
                futures.clear();
                if (megaTask.isCompletedExceptionally()) {
                    throw new RuntimeException("Error occurred while writing edges - see logs for more details");
                } else {
                    // Flush Vertex Edge cache maps when all Edge writes are done.
                    try {
                        if (!generateEdgeCaches) {
                            writeEdgeCacheToDB(graph, vertexOutEdgeMap, vertexInEdgeMap);
                        } else {
                            removeDetatchedEdgesPreGenerated(graph, edgeToFromIdList, bufferSize, allowedDetachedEdges);
                        }
                    } catch (final RuntimeException e) {
                        LOGGER.error("Failed to flush Vertex Edge cache maps", e);
                        throw e;
                    }
                }
                final String taskName = String.format("Edge write in partition:{}", partitionId);
                if (!readOnly) {
                    LOGGER.info("Writing edge partition complete for partitionId: {}", partitionId);
                    RecoveryUtil.writeEdgePartitionComplete(graph.getBaseGraph(), partitionId);
                }
                graph.fireflySummaryUpdater.completeEdgePartition(partitionId);
                LOGGER.info("Task:{}; Total time taken(in milliseconds):{}", taskName, Duration.between(totalStart, Instant.now()).toMillis());
            }
        });
    }

    private void removeDetatchedEdgesPreGenerated(final FireflyGraph graph,
                                                  final List<Tuple3<FireflyId, Object, Object>> edgeToFromIdList,
                                                  final int bufferSize,
                                                  final long allowedDetachedEdges) {

        if (edgeToFromIdList.size() > bufferSize) {
            final Set<Object> vertexIds = new HashSet<>();
            for (final Tuple3<FireflyId, Object, Object> edgeToFrom : edgeToFromIdList) {
                vertexIds.add(edgeToFrom._2());
                vertexIds.add(edgeToFrom._3());
            }
            final List<Object> vertexIdList = new ArrayList<>(vertexIds);
            final List<Boolean> vertexExistsList = graph.bulkVertexExists(vertexIdList);
            final Set<Object> vertexDoesntExistList = new HashSet<>();
            for (int i = 0; i < vertexIdList.size(); i++) {
                final Object vertexId = vertexIdList.get(i);
                if (!vertexExistsList.get(i)) {
                    // If the vertex does not exist, we need to remove it from the supernodes set.
                    vertexDoesntExistList.add(vertexId);
                }
            }
            final Set<byte[]> edgesToRemove = new HashSet<>();
            for (final Tuple3<FireflyId, Object, Object> edgeToFrom : edgeToFromIdList) {
                if (vertexDoesntExistList.contains(edgeToFrom._2()) ||
                        vertexDoesntExistList.contains(edgeToFrom._3())) {
                    edgesToRemove.add(edgeToFrom._1().getKeyHash());
                }
            }
            GraphOperations.dropDetachedEdges(graph, edgesToRemove, allowedDetachedEdges);
        }
    }


    private void writeEdgeCacheToDB(final FireflyGraph graph,
                                    final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexOutEdgeMap,
                                    final ConcurrentHashMap<Object, ConcurrentHashMap<String, Set<Value>>> vertexInEdgeMap) {
        final long allowedDetachedEdges = this.config.getOrDefaultInt(ALLOWED_BAD_EDGES_COUNT);
        final Set<byte[]> invalidEdgeIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

        GraphOperations.flushEdgeMap(graph, Direction.OUT, vertexOutEdgeMap, allowedDetachedEdges, invalidEdgeIds);
        GraphOperations.flushEdgeMap(graph, Direction.IN, vertexInEdgeMap, allowedDetachedEdges, invalidEdgeIds);

        if (!invalidEdgeIds.isEmpty()) {
            GraphOperations.dropDetachedEdges(graph, invalidEdgeIds, allowedDetachedEdges);
        }
    }

    public void verifySampleEdgesAfterWrite(final Dataset<Row> edgeDatasetsSample) {
        final String errorMessage = "Error occurred while verifying edges; see logs for more details.";
        final int bufferSize = getEdgeWriteBufferSize();
        boolean hasEdgeID = Arrays.asList(edgeDatasetsSample.schema().fieldNames()).contains(EDGE_ID_COLUMN);
        edgeDatasetsSample.foreachPartition(rowIterator -> {
            final boolean keepProvidedId =
                    config.getOrDefaultBool(KEEP_PROVIDED_EDGE_ID_AS_PROPERTY);
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
                        LOGGER.error("Exception occurred in verifying Edge row", e);  // Log the error when final failure happens
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
            } catch (final AerospikeGraphException e) {
                if (!isRetryable(e)) {
                    LOGGER.error("Failed to verify loaded Edge due to non-retryable error: " + metadataRow, e);
                    throw e;
                } else if (++tryCount > RETRY_LIMIT) {
                    LOGGER.error("Failed to verify loaded Edge after " + tryCount + " attempts: " + metadataRow, e);
                    throw e;
                } else {
                    LOGGER.warn("Failed to verify loaded Edge: " + metadataRow + ". Attempt count: " + tryCount, e);
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

    private JavaPairRDD<Object, Long> readExistingVertices(final JavaPairRDD<Object, Long> input,
                                                           final Direction direction,
                                                           final Long onRecordIdLimit) {
        return input.mapPartitionsToPair(iterator -> {
            final List<Tuple2<Object, Long>> results = new ArrayList<>();
            try (final FireflyGraph graph = FireflyGraph.open(config.getFireflyConfig())) {
                final int bufferSize = graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE;
                final Map<Object, Long> idToEdgeCount = new ConcurrentHashMap<>();
                while (iterator.hasNext()) {
                    final Tuple2<Object, Long> row = iterator.next();
                    idToEdgeCount.put(row._1, row._2);
                    if (idToEdgeCount.size() > bufferSize) {
                        batchReadToTuple(direction, results, graph, idToEdgeCount, onRecordIdLimit);
                        idToEdgeCount.clear();
                    }
                }
                if (!idToEdgeCount.isEmpty()) {
                    batchReadToTuple(direction, results, graph, idToEdgeCount, onRecordIdLimit);
                    idToEdgeCount.clear();
                }
            }
            return results.iterator();
        });
    }

    private void batchReadToTuple(final Direction direction,
                                  final List<Tuple2<Object, Long>> results,
                                  final FireflyGraph graph,
                                  final Map<Object, Long> idToEdgeCount,
                                  final Long onRecordIdLimit) {
        List<FireflyVertex> vertices;
        int tryCount = 0;
        while (true) {
            try {
                // No has containers, also we are pushing down the ids to the graph to read in bulk omitting properties.
                vertices = graph.readVertices(List.of(),
                        idToEdgeCount.keySet().stream().map(id ->
                                graph.getIdFactory().createVertexId(id)).collect(Collectors.toList()),
                        List.of());
                break;
            } catch (final AerospikeGraphException e) {
                if (!isRetryable(e)) {
                    LOGGER.error("Failed to batch read vertices for supernode detection.", e);
                    throw e;
                } else if (++tryCount > RETRY_LIMIT) {
                    LOGGER.error("Failed to batch read vertices for supernode detection. " + tryCount + " attempts.", e);
                    throw e;
                } else {
                    LOGGER.warn("Failed to batch read vertices for supernode detection. Attempt count: " + tryCount + ".", e);
                    exponentialBackoff(tryCount);
                }
            }
        }
        final long overflow = 2 * onRecordIdLimit;
        for (final FireflyVertex vertex : vertices) {
            if (vertex.isEdgeCacheOverflowed()) {
                // If the edge cache is overflowed, we should not bother counting.
                // We could be adding an edge to a supernode, and we don't want to count that.
                results.add(new Tuple2<>(vertex.id.getUserId(), overflow));
            } else {
                // Edge cache is not overflowed so count it.
                long existingEdgeCount = vertex.getEdgeCount(direction);
                final Long newEdgeCount = idToEdgeCount.remove(vertex.id());
                results.add(new Tuple2<>(vertex.id.getUserId(), newEdgeCount + existingEdgeCount));
                if (newEdgeCount + existingEdgeCount >= onRecordIdLimit) {
                    // This is going to become a supernode, mark it now.
                    graph.getOperations().setCacheDisabled(vertex);
                }
            }
        }
        idToEdgeCount.forEach((id, count) -> results.add(new Tuple2<>(id, count)));
    }

    public void setSupernodes(final Set<Object> supernodes) {
        this.supernodes = supernodes;
    }

    public Set<Object> extractSupernodesDirection(final Dataset<Row> edgeDataset,
                                                  final Direction direction,
                                                  final long onRecordIdLimit,
                                                  final double sampleSize,
                                                  final boolean incremental) {
        LOGGER.info("Extracting supernodes for: " + direction);
        final String column = direction.equals(Direction.IN) ? "~from" : "~to";
        final JavaRDD<Row> edgeRDD = edgeDataset.javaRDD();

        // Values in csv for ~from and ~to will return as strings but can be strings or longs.
        final JavaPairRDD<Object, Long> pairRDD;
        if (sampleSize != 0) {
            LOGGER.info("Supernode pair mapping for: " + direction + " with sampling of " + sampleSize);
            final Long value = ((Double) (1L / sampleSize)).longValue();
            pairRDD = edgeRDD.sample(false, sampleSize, 1).
                    mapToPair((PairFunction<Row, Object, Long>) row ->
                            new Tuple2<>(PropertyValueParser.parseId(row.getAs(column)), value));
        } else {
            LOGGER.info("Supernode pair mapping for: " + direction + " without sampling");
            pairRDD = edgeRDD.
                    mapToPair((PairFunction<Row, Object, Long>) row ->
                            new Tuple2<>(PropertyValueParser.parseId(row.getAs(column)), 1L));
        }
        edgeRDD.unpersist();

        LOGGER.info("Supernode aggregation for: " + direction);
        JavaPairRDD<Object, Long> countPairRDD =
                pairRDD.reduceByKey((Function2<Long, Long, Long>) Long::sum);
        pairRDD.unpersist();

        if (incremental) {
            LOGGER.info("Checking existing edge caches for: " + direction);
            countPairRDD = readExistingVertices(countPairRDD, direction, onRecordIdLimit);
        }

        LOGGER.info("Filtering for edge cache size for: " + direction);
        final JavaPairRDD<Object, Long> filteredCountPairRDD = countPairRDD.filter(
                (Function<Tuple2<Object, Long>, Boolean>)
                        longLongTuple2 -> longLongTuple2._2 >= onRecordIdLimit);
        countPairRDD.unpersist();

        LOGGER.info("Getting keys from filter output: " + direction);
        final JavaRDD<Object> fromSupernodes = filteredCountPairRDD.keys();
        filteredCountPairRDD.unpersist();

        final Set<Object> localSupernodes = new HashSet<>(fromSupernodes.collect());
        LOGGER.info("Obtained supernodes " + localSupernodes + " for: " + direction);
        fromSupernodes.unpersist();
        return localSupernodes;
    }

    public Set<Object> extractSupernodes(final Dataset<Row> edgeDataset, final long onRecordIdLimit, final boolean incremental, final double supernodeSamplingPercentage) {
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
            supernodes.addAll(extractSupernodesDirection(edgeDataset, Direction.IN, onRecordIdLimit, supernodeSamplingPercentage, incremental));
            supernodes.addAll(extractSupernodesDirection(edgeDataset, Direction.OUT, onRecordIdLimit, supernodeSamplingPercentage, incremental));
            LOGGER.info("Final supernodes set: " + supernodes);
        }
        return this.supernodes;
    }

    public void verifySampleEdgeAfterWrite(final Dataset<Row> sampledEdgeDataset) {
        if (this.config.hasAction(VERIFY_OUTPUT_DATA) &&
                !this.config.hasAction(DISABLE_EDGE_WRITE)) {
            final long allowedDetachedEdges = this.config.getOrDefaultInt(ALLOWED_BAD_EDGES_COUNT);
            if (allowedDetachedEdges > 0) {
                LOGGER.warn(ALLOWED_BAD_EDGES_COUNT + " is set to a value greater than 0. Edge verification cannot be performed and will be skipped.");
                return;
            }
            final long allowBadEntries = this.config.getOrDefaultInt(ALLOWED_BAD_ENTRY_COUNT);
            if (allowBadEntries > 0) {
                LOGGER.warn(ALLOWED_BAD_ENTRY_COUNT + " is set to a value greater than 0. Edge verification cannot be performed and will be skipped.");
                return;
            }
            final String taskName = "Verify Edges";
            sampledEdgeDataset.sparkSession().sparkContext().setJobGroup(taskName, "Verify Edges task", true);
            LOGGER.info("verify_output_data is enabled, starting the Edge write verification.");
            verifySampleEdgesAfterWrite(sampledEdgeDataset);
            sampledEdgeDataset.sparkSession().sparkContext().cancelJobGroup(taskName);
        }
    }

    static private class EdgeIDAdditionFunction implements MapPartitionsFunction, Serializable {
        final Map<String, Object> fileConfig;
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

    static private class EdgeIDOutputIterator implements Iterator<Row>, Serializable {
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
                        if (instance == null) {
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
            if (!hasMore) {
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
            // Add the edgeID
            final FireflyPhatEdgeId edgeId = (FireflyPhatEdgeId) getFireflyGraph().getIdFactory().generateId(getFireflyGraph(), FireflyEdge.class);
            final String encodedId = encodeID(edgeId);
            outputRow.add(encodedId);
            final Long packingId = Long.parseLong(encodedId.split(":")[0]) / getFireflyGraph().getBaseGraph().PHAT_EDGE_SIZE;
            outputRow.add(packingId);
            return new GenericRowWithSchema(outputRow.toArray(), schema);
        }
    }

    /***
     * Converts FireflyPhatEdgeId to String by converting individual component to string and concatenating them with a
     * delimiter ":"
     * @param edgeId id generated by id manager
     * @return encoded
     */
    private static String encodeID(final FireflyPhatEdgeId edgeId) {
        // Isolate bytes from both component
        final long packingId = edgeId.getPackingId();

        // Encode each number to string using a delimiter
        final StringBuilder builder = new StringBuilder();
        builder.append(packingId);
        builder.append(":");
        builder.append(edgeId.isRecycled() ? edgeId.getUniqueId() : NOT_RECYCLED_ID_TOKEN);

        return builder.toString();
    }

    /***
     * Assigns each edge record with an ~edgeID and writes them to user specified location.
     * @param edgeDataset
     * @param writeLocation
     * @param config
     */
    public Dataset<Row> writeEdgeIDsToDataframe(final Dataset<Row> edgeDataset, final String writeLocation,
                                                final Map<String, Object> config, final boolean readOnly) {
        final Instant startWriteEdge = Instant.now();
        final String taskName = "Edges ID write";
        final StructType writeSchema = edgeDataset.schema().
                add(DataTypes.createStructField(EDGE_ID_COLUMN, DataTypes.StringType, false)).
                add(DataTypes.createStructField(PACKING_ID_COLUMN, DataTypes.LongType, false));
        edgeDataset.sparkSession().sparkContext().setJobGroup(taskName, "Edges ID write task", true);
        final ExpressionEncoder<Row> edgeIdEncoder = RowEncoder.apply(writeSchema);
        final Dataset<Row> edgeIdDataset = edgeDataset.mapPartitions(new EdgeIDAdditionFunction(config, writeSchema), edgeIdEncoder);
        if (!readOnly) {
            edgeIdDataset.write().option("header", true).mode(SaveMode.Overwrite).option("compression", "bzip2").csv(writeLocation);
            edgeIdDataset.persist(StorageLevel.DISK_ONLY());

        }
        edgeDataset.sparkSession().sparkContext().cancelJobGroup(taskName);
        LOGGER.info("Execution time in seconds for Edge ID write task: " + Duration.between(startWriteEdge, Instant.now()).getSeconds());
        return edgeIdDataset;
    }

    public void writeBadEdges(final Dataset<Row> edgeDataset,
                              final Dataset<Row> vertexDataset,
                              final BulkLoaderConfigHelper config) {
        final Instant startWriteEdge = Instant.now();
        final String taskName = "Write bad edges";
        edgeDataset.sparkSession().sparkContext().setJobGroup(taskName, "Write bad edges", true);

        // Step 1: Prepare filtered vertex set
        Dataset<Row> vertexIdSet = vertexDataset.select(ID_HEADER).distinct();

        // Step 2: Find unmatched ~to values
        Dataset<Row> unmatchedTo = edgeDataset
                .select(col(TO_VERTEX_HEADER).alias("missing_vertex_id"))
                .join(vertexIdSet, col("missing_vertex_id").equalTo(vertexIdSet.col(ID_HEADER)), "left_anti");

        // Step 3: Find unmatched ~from values
        Dataset<Row> unmatchedFrom = edgeDataset
                .select(col(FROM_VERTEX_HEADER).alias("missing_vertex_id"))
                .join(vertexIdSet, col("missing_vertex_id").equalTo(vertexIdSet.col(ID_HEADER)), "left_anti");

        // Step 4: Combine both, then group and count
        Dataset<Row> allUnmatched = unmatchedTo.union(unmatchedFrom);

        Dataset<Row> unmatchedCounts = allUnmatched.groupBy("missing_vertex_id").count();
        unmatchedCounts.foreachPartition(
                partition -> {
                    try (final FireflyGraph graph = FireflyGraph.open(config.getFireflyConfig())) {
                        while (partition.hasNext()) {
                            final Row row = partition.next();
                            final String vertexId = row.getString(0);
                            final long count = row.getLong(1);
                            graph.getOperations().writeBadEdge(vertexId, count);
                            LOGGER.error("Vertex ID {} has {} unmatched edges", vertexId, count);
                        }
                    }
                });

        edgeDataset.sparkSession().sparkContext().cancelJobGroup(taskName);
        LOGGER.info("Execution time in seconds for Write bad edges: " + Duration.between(startWriteEdge, Instant.now()).getSeconds());
    }

    /***
     * Decode the Stringified encoded edgeId to byte[] as it was originally generated by @encodeId
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
        Preconditions.checkArgument(tokens.length == 2, String.format("Tokenized idString must have only 2 tokens, found %d", tokens.length));

        final Long packingId = Objects.requireNonNull(Long.valueOf(tokens[0]), "parsed packing ID can't be null");
        final byte[] packingByte = Longs.toByteArray(packingId);
        final byte[] graphId;
        if (NOT_RECYCLED_ID_TOKEN.equals(tokens[1])) {
            graphId = new byte[8];
            System.arraycopy(packingByte, 0, graphId, 0, 8);
        } else {
            graphId = new byte[16];
            final Long uniqueId = Objects.requireNonNull(Long.valueOf(tokens[1]), "parsed unique ID can't be null");
            final byte[] uniqueByte = Longs.toByteArray(uniqueId);

            System.arraycopy(packingByte, 0, graphId, 0, 8);
            System.arraycopy(uniqueByte, 0, graphId, 8, 8);
        }

        return graphId;
    }

    public void writeEdgeToDB(final Dataset<Row> edgeIdDataSet,
                              final Set<Long> completedEdgePartitions,
                              final boolean readOnly, final boolean generateEdgeCaches) {
        if (!this.config.hasAction(DISABLE_EDGE_WRITE)) {
            final Instant startWriteEdge = Instant.now();
            final String taskName = "Edges write to Aerospike Database";
            edgeIdDataSet.sparkSession().sparkContext().setJobGroup(taskName,
                    "Edges write task", true);
            writeEdges(edgeIdDataSet, completedEdgePartitions, readOnly, generateEdgeCaches);
            edgeIdDataSet.sparkSession().sparkContext().cancelJobGroup(taskName);
            LOGGER.info("Execution time in seconds for Edge write task: " + Duration.between(startWriteEdge, Instant.now()).getSeconds());
        }
    }

    private int getEdgeWriteBufferSize() {
        return this.config.getOrDefaultInt(EDGE_WRITE_BUFFER);
    }
}
