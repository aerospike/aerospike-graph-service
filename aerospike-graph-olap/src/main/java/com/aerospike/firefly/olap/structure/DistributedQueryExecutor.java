package com.aerospike.firefly.olap.structure;

import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.query.paged.GraphQueryHelper;
import com.aerospike.firefly.io.aerospike.query.paged.PageFetcher;
import com.aerospike.firefly.io.aerospike.query.paged.PartitionedSindexPageFetcher;
import com.aerospike.firefly.io.aerospike.query.paged.ScanPageFetcher;
import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.codec.RowCodec;
import com.aerospike.firefly.olap.codec.RowCodecFactory;
import com.aerospike.firefly.olap.config.DistributedConfigHelper;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyEdgeFactory;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.TimeoutHelper;
import com.amazonaws.services.logs.model.QueryInfo;
import com.google.common.collect.Lists;
import org.apache.hadoop.shaded.org.checkerframework.checker.nullness.Opt;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aerospike.firefly.olap.codec.RowCodecHelper.getId;
import static com.aerospike.firefly.olap.codec.RowCodecHelper.getIdType;

public class DistributedQueryExecutor {

    private static final String START_COL = "~start";
    private static final String COUNT_COL = "~count";

    private static Dataset<Row> getIndexQuery(final SparkSession spark,
                                              final FireflyGraph rootGraph,
                                              final DistributedConfigHelper configHelper,
                                              final List<HasContainer> initialHasContainers,
                                              final Traversal<?, ?> traversal,
                                              final StructType outputSchema,
                                              final int maxParallelQuery,
                                              final int maxWorkers,
                                              final QueryInfo queryInfo) {
        final List<Row> queryRanges = Range.splitPartitions(Math.min(maxParallelQuery, maxWorkers)).
                stream().map(range -> RowFactory.create(range.start, range.count)).collect(Collectors.toList());
        final StructType inputSchema = new StructType().
                add(START_COL, DataTypes.IntegerType, false).
                add(COUNT_COL, DataTypes.IntegerType, false);
        final Dataset<Row> queryRangeDataset = spark.createDataFrame(queryRanges, inputSchema);
        final FireflyIndexMetadata.IndexInfo indexInfo = queryInfo.indexInfo.get();
        final HasContainer hasContainer = queryInfo.indexTopHasContainer.get();
        final String startStep = traversal.asAdmin().getStartStep().getNextStep().getId();
        final List<HasContainer> hasContainers = queryInfo.fireflyHasContainers;
        return queryRangeDataset.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
            // Junk required.
            final LinkedBlockingQueue<PageFetcher.Page> pageQueue = new LinkedBlockingQueue<>();
            final Codec codec = new Codec(traversal);

            // Open graph.
            try (final FireflyGraph graph = FireflyGraph.open(configHelper.getFireflyConfig())) {
                final Filter filter = GraphQueryHelper.predicateToFilter(graph.getBaseGraph(), hasContainer.getPredicate(), indexInfo);
                while (iterator.hasNext()) {
                    final Row row = iterator.next();
                    final PartitionFilter partitionFilter = PartitionFilter.range(
                            row.getInt(row.fieldIndex(START_COL)),
                            row.getInt(row.fieldIndex(COUNT_COL)));

                    // TODO: Can omit some bins here.
                    final int evaluationTimeout = Long.valueOf(TimeoutHelper.calculate(traversal.asAdmin())).intValue();
                    final QueryPolicy policy = new QueryPolicy();
                    policy.setTimeout(evaluationTimeout);
                    final PageFetcher<?> pageFetcher = new PartitionedSindexPageFetcher<>(
                            graph,
                            policy,
                            indexInfo.setName,
                            graph.getBaseGraph().getNamespace(),
                            filter,
                            graph.getBaseGraph().PAGINATION_PAGE_SIZE,
                            graph::vertexFromRecord,
                            indexInfo.indexName,
                            partitionFilter,
                            pageQueue);

                    // Intentionally not using return value here.
                    pageFetcher.startQueryDirect();
                }

                final List<Row> outputRows = new ArrayList<>();
                while (true) {
                    final PageFetcher.Page page = pageQueue.take();
                    if (page instanceof PageFetcher.ErrorPage) {
                        final PageFetcher.ErrorPage errorPage = (PageFetcher.ErrorPage) page;
                        throw new RuntimeException("Error fetching page: " + errorPage.errorMessage, errorPage.exception);
                    } else if (page instanceof PageFetcher.PoisonPill) {
                        break;
                    }
                    page.keyRecords.forEachRemaining(keyRecord -> {
                        // TODO: Improve performance with ReferenceVertex.
                        final FireflyVertex vertex = graph.vertexFromRecord(keyRecord);
                        if (HasContainer.testAll(vertex, hasContainers)) {
                            outputRows.add(codec.encode(vertex, startStep));
                        }
                    });
                }
                return outputRows.iterator();
            }
        }, RowEncoder.apply(outputSchema));
    }


    private static Dataset<Row> getPiQuery(final SparkSession spark,
                                           final FireflyGraph rootGraph,
                                           final DistributedConfigHelper configHelper,
                                           final List<HasContainer> initialHasContainers,
                                           final Traversal<?, ?> traversal,
                                           final StructType outputSchema,
                                           final int maxParallelQuery,
                                           final int maxWorkers,
                                           final QueryInfo queryInfo) {
        final List<Object> initialIds = queryInfo.ids.get();
        System.out.println("IDS: " + initialIds);
        final List<Object> ids = new ArrayList<>();
        if (initialIds.size() == 1 && initialIds.get(0) instanceof P) {
            final P p = (P) initialIds.get(0);
            if (!p.getBiPredicate().toString().equals("within")) {
                throw new IllegalArgumentException("Batch read only supports within predicate");
            }
            if (!(p.getValue() instanceof List)) {
                throw new IllegalArgumentException("Batch read only supports a single list of keys");
            }
            ids.addAll((List) p.getValue());
        } else {
            ids.addAll(initialIds);
        }
        System.out.println("Actual ids: " + ids);
        initialIds.clear();
        final List<Row> rows = ids.stream().filter(Objects::nonNull).map(id -> RowFactory.create(id.toString(), getIdType(id).ordinal())).collect(Collectors.toList());
        final StructType inputSchema = new StructType().
                add(RowCodec.ID_COL, DataTypes.StringType, false).
                add(RowCodec.ID_TYPEHINT_COL, DataTypes.IntegerType, false);
        final Dataset<Row> idDataset = spark.createDataFrame(rows, inputSchema);
        final GraphStep graphStep = (GraphStep) traversal.asAdmin().getStartStep();
        final String startStep = traversal.asAdmin().getStartStep().getNextStep().getId();
        final List<HasContainer> hasContainers = queryInfo.fireflyHasContainers;
        return idDataset.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
            final LinkedBlockingQueue<PageFetcher.Page> pageQueue = new LinkedBlockingQueue<>();
            final TraversalMatrix<?, ?> traversalMatrix = new TraversalMatrix<>(traversal.asAdmin());
            final Codec codec = new Codec(traversal);

            try (final FireflyGraph graph = FireflyGraph.open(configHelper.getFireflyConfig())) {
                final List<FireflyId> ffids = new ArrayList<>();
                while (iterator.hasNext()) {
                    final Row row = iterator.next();
                    final Object id = getId(
                            row.getString(row.fieldIndex(RowCodec.ID_COL)),
                            row.getInt(row.fieldIndex(RowCodec.ID_TYPEHINT_COL)));
                    System.out.println("id: " + id);
                    ffids.add(graphStep.returnsVertex() ? graph.getIdFactory().createVertexId(id) : graph.getIdFactory().createEdgeId(id));
                }
                final List<List<FireflyId>> partitionedFfidList = Lists.partition(ffids, graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE);
                if (graphStep.returnsVertex()) {
                    final List<Row> outputVertices = new ArrayList<>();
                    for (final List<FireflyId> ffidList : partitionedFfidList) {
                        // TODO: Pushdown.
                        System.out.println("Reading " + ffidList);
                        List<FireflyVertex> vertices = graph.readVertices(List.of(), ffidList, null);
                        System.out.println("Output : " + vertices);
                        for (final Vertex vertex : vertices) {
                            System.out.println("Vertex: " + vertex);
                            if (HasContainer.testAll(vertex, hasContainers)) {
                                outputVertices.add(codec.encode(vertex, startStep));
                            } else {
                                System.out.println("Did not match : " + hasContainers);
                            }
                        }
                        // vertices.stream().filter(v -> HasContainer.testAll(v, hasContainers)).forEach(vertex -> outputVertices.add(codec.encode(vertex, startStep)));
                    }
                    return outputVertices.iterator();
                } else {
                    final List<Row> outputEdges = new ArrayList<>();
                    for (final List<FireflyId> ffidList : partitionedFfidList) {
                        List<FireflyEdge> edges = graph.readEdges(List.of(), ffidList, null);
                        edges.stream().filter(e -> HasContainer.testAll(e, hasContainers)).forEach(edge -> outputEdges.add(codec.encode(edge, startStep)));
                    }
                    return outputEdges.iterator();
                }
            }
        }, RowEncoder.apply(outputSchema));
    }

    private static Dataset<Row> getScanQuery(final SparkSession spark,
                                             final FireflyGraph rootGraph,
                                             final DistributedConfigHelper configHelper,
                                             final List<HasContainer> initialHasContainers,
                                             final Traversal<?, ?> traversal,
                                             final StructType outputSchema,
                                             final int maxParallelQuery,
                                             final int maxWorkers,
                                             final QueryInfo queryInfo) {
        final List<Row> queryRanges = Range.splitPartitions(Math.min(maxParallelQuery, maxWorkers)).
                stream().map(range -> RowFactory.create(range.start, range.count)).collect(Collectors.toList());
        final StructType inputSchema = new StructType().
                add(START_COL, DataTypes.IntegerType, false).
                add(COUNT_COL, DataTypes.IntegerType, false);
        final Dataset<Row> queryRangeDataset = spark.createDataFrame(queryRanges, inputSchema);
        final Expression expression = queryInfo.expression.orElse(null);
        final String startStep = traversal.asAdmin().getStartStep().getNextStep().getId();
        final GraphStep graphStep = (GraphStep) traversal.asAdmin().getStartStep();
        final List<HasContainer> hasContainers = queryInfo.fireflyHasContainers;
        return queryRangeDataset.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
            // Junk required.
            final LinkedBlockingQueue<PageFetcher.Page> pageQueue = new LinkedBlockingQueue<>();
            final Codec codec = new Codec(traversal);

            // Open graph.
            try (final FireflyGraph graph = FireflyGraph.open(configHelper.getFireflyConfig())) {
                while (iterator.hasNext()) {
                    final Row row = iterator.next();
                    final PartitionFilter partitionFilter = PartitionFilter.range(
                            row.getInt(row.fieldIndex(START_COL)),
                            row.getInt(row.fieldIndex(COUNT_COL)));

                    // TODO: Can omit some bins here.
                    final int evaluationTimeout = Long.valueOf(TimeoutHelper.calculate(traversal.asAdmin())).intValue();
                    final ScanPolicy policy = new ScanPolicy();
                    policy.setTimeout(evaluationTimeout);
                    policy.filterExp = expression;

                    final PageFetcher<?> pageFetcher = new ScanPageFetcher<>(
                            graph,
                            policy,
                            graphStep.returnsVertex() ? graph.getBaseGraph().VERTEX_AERO_SET : graph.getBaseGraph().EDGE_AERO_SET,
                            null,
                            graph.getBaseGraph().PAGINATION_PAGE_SIZE,
                            null,
                            partitionFilter,
                            Executors.newSingleThreadExecutor(),
                            pageQueue,
                            graph::vertexFromRecord);

                    // Intentionally not using return value here.
                    pageFetcher.startQueryDirect();
                }

                final List<Row> outputRows = new ArrayList<>();
                while (true) {
                    final PageFetcher.Page page = pageQueue.take();
                    if (page instanceof PageFetcher.ErrorPage) {
                        final PageFetcher.ErrorPage errorPage = (PageFetcher.ErrorPage) page;
                        throw new RuntimeException("Error fetching page: " + errorPage.errorMessage, errorPage.exception);
                    } else if (page instanceof PageFetcher.PoisonPill) {
                        break;
                    }
                    page.keyRecords.forEachRemaining(keyRecord -> {
                        // TODO: Improve performance with ReferenceVertex.
                        if (graphStep.returnsVertex()) {
                            final FireflyVertex vertex = graph.vertexFromRecord(keyRecord);
                            if (HasContainer.testAll(vertex, hasContainers))
                                outputRows.add(codec.encode(vertex, startStep));
                        } else {
                            final Map<ByteBuffer, List> edgeData = (Map<ByteBuffer, List>) keyRecord.record.getMap(graph.getBaseGraph().EDGE_DATA_BIN);
                            // Implicitly assume that if the key is found for label, which is required, then the key exists for the
                            // other phat edge maps, since they are all written in the same operate.
                            for (final ByteBuffer edgeIdMapKey : edgeData.keySet()) {
                                final String label = (String) edgeData.get(edgeIdMapKey).get(FireflyEdge.LABEL_POSITION);

                                final String outV = (String) edgeData.get(edgeIdMapKey).get(FireflyEdge.OUT_V_POSITION);
                                final FireflyId outVertex = graph.getIdFactory().createVertexIdFromHash(outV);

                                final String inV = (String) edgeData.get(edgeIdMapKey).get(FireflyEdge.IN_V_POSITION);
                                final FireflyId inVertex = graph.getIdFactory().createVertexIdFromHash(inV);

                                final Map<String, Object> properties = (Map<String, Object>) edgeData.get(edgeIdMapKey).get(FireflyEdge.PROPERTIES_POSITION);
                                final Map<String, Object> typeHints = (Map<String, Object>) edgeData.get(edgeIdMapKey).get(FireflyEdge.TYPE_HINTS_POSITION);

                                final Map<ByteBuffer, String> outSupernodes = (Map<ByteBuffer, String>) keyRecord.record.getMap(graph.getBaseGraph().SUPERNODES_OUT_BIN);
                                final Map<ByteBuffer, String> inSupernodes = (Map<ByteBuffer, String>) keyRecord.record.getMap(graph.getBaseGraph().SUPERNODES_IN_BIN);
                                final boolean isOutSupernode = outSupernodes != null && outSupernodes.containsKey(edgeIdMapKey);
                                final boolean isInSupernode = inSupernodes != null && inSupernodes.containsKey(edgeIdMapKey);

                                final FireflyEdge edge = FireflyEdgeFactory.create(graph.getIdFactory().createEdgeId(edgeIdMapKey), label, graph, outVertex, inVertex, properties, typeHints, isOutSupernode, isInSupernode, keyRecord.record.generation);
                                // TODO: g.E().hasLabel("knows") ?
                                if (HasContainer.testAll(edge, hasContainers))
                                    outputRows.add(codec.encode(edge, startStep));
                            }
                        }

                    });
                }
                return outputRows.iterator();
            }
        }, RowEncoder.apply(outputSchema));
    }


    public static Dataset<Row> getStartingPoint(final SparkSession spark,
                                                final FireflyGraph rootGraph,
                                                final DistributedConfigHelper configHelper,
                                                final List<HasContainer> initialHasContainers,
                                                final Object[] ids,
                                                final Traversal<?, ?> traversal,
                                                final StructType outputSchema,
                                                final int maxParallelQuery,
                                                final int maxWorkers) {
        traversal.asAdmin().applyStrategies();
        final QueryInfo queryInfo = QueryInfo.getQueryInfo(rootGraph, (GraphStep) traversal.asAdmin().getStartStep(), initialHasContainers, ids);
        switch (queryInfo.queryType) {
            case INDEX:
                return getIndexQuery(spark, rootGraph, configHelper, initialHasContainers, traversal, outputSchema, maxParallelQuery, maxWorkers, queryInfo);
            case PI:
                return getPiQuery(spark, rootGraph, configHelper, initialHasContainers, traversal, outputSchema, maxParallelQuery, maxWorkers, queryInfo);
            case SCAN:
                return getScanQuery(spark, rootGraph, configHelper, initialHasContainers, traversal, outputSchema, maxParallelQuery, maxWorkers, queryInfo);
            default:
                throw new IllegalStateException("Unknown query type: " + queryInfo.queryType);
        }
    }

    private static class QueryInfo implements Serializable {
        enum QueryType implements Serializable {
            INDEX,
            SCAN,
            PI
        }

        private final QueryType queryType;
        private final List<HasContainer> fireflyHasContainers;
        private final Optional<FireflyIndexMetadata.IndexInfo> indexInfo; // Only valid for index queries.
        private final Optional<HasContainer> indexTopHasContainer; // Only valid for index queries.
        private final Optional<List<Object>> ids; // Only valid for PI queries.
        private final Optional<Expression> expression; // Only valid for scan queries.

        private QueryInfo(final FireflyIndexMetadata.IndexInfo indexInfo,
                          final HasContainer hasContainer,
                          final List<HasContainer> initialHasContainers) {
            this.queryType = QueryType.INDEX;
            this.indexInfo = Optional.of(indexInfo);
            this.indexTopHasContainer = Optional.of(hasContainer);
            this.ids = Optional.empty();
            this.expression = Optional.empty();
            this.fireflyHasContainers = initialHasContainers;
        }

        private QueryInfo(final List<Object> ids,
                          final List<HasContainer> initialHasContainers) {
            this.queryType = QueryType.PI;
            this.indexInfo = Optional.empty();
            this.indexTopHasContainer = Optional.empty();
            this.ids = Optional.of(ids);
            this.expression = Optional.empty();
            this.fireflyHasContainers = initialHasContainers;
        }

        private QueryInfo(final Expression expression,
                          final List<HasContainer> initialHasContainers) {
            this.queryType = QueryType.SCAN;
            this.indexInfo = Optional.empty();
            this.indexTopHasContainer = Optional.empty();
            this.ids = Optional.empty();
            this.expression = Optional.ofNullable(expression);
            this.fireflyHasContainers = initialHasContainers;
        }

        private QueryInfo(final List<HasContainer> initialHasContainers) {
            this.queryType = QueryType.SCAN;
            this.indexInfo = Optional.empty();
            this.indexTopHasContainer = Optional.empty();
            this.ids = Optional.empty();
            this.expression = Optional.empty(); // Scan all.
            this.fireflyHasContainers = initialHasContainers;
        }

        private static QueryInfo getQueryInfo(final FireflyGraph graph,
                                              final GraphStep step,
                                              final List<HasContainer> initialHasContainers,
                                              final Object[] idsInput) {
            final List<HasContainer> positiveFilters = initialHasContainers.stream().filter(it -> !it.getBiPredicate().equals(Contains.without)).collect(Collectors.toList());

            if (idsInput != null && idsInput.length > 0) {
                final List<Object> ids = Stream.of(idsInput).collect(Collectors.toList());
                return new QueryInfo(ids, initialHasContainers);
            }

            // Check for PI query based on hasContainers.
            if (!positiveFilters.isEmpty()) {
                final List<Object> ids = positiveFilters
                        .stream()
                        .filter(it -> "~id".equals(it.getKey()))
                        .map(HasContainer::getValue)
                        .flatMap(it -> it instanceof List ? ((List<?>) it).stream() : Stream.of(it))
                        .collect(Collectors.toList());
                final List<HasContainer> nonIdContainers = initialHasContainers.stream().filter(it -> !"~id".equals(it.getKey())).collect(Collectors.toList());

                // If there are id has containers, we can do a batch read.
                if (!ids.isEmpty()) {
                    return new QueryInfo(ids, nonIdContainers);
                }
            }

            if (step.returnsVertex()) {
                final List<FireflyGraphStep.HasContainerWithCardinality> sortedHasContainers = FireflyBatchReadHelper.getHasContainersWithCardinalityOrder(graph, FireflyVertex.class, initialHasContainers);
                final List<HasContainer> aerospikeSideHasContainers = FireflyBatchReadHelper.getAerospikeHasContainers(sortedHasContainers);
                final List<HasContainer> fireflySideHasContainers = FireflyBatchReadHelper.getFireflyHasContainers(sortedHasContainers);
                final HasContainer topContainer = aerospikeSideHasContainers.isEmpty() ? null : aerospikeSideHasContainers.get(0);
                final Optional<FireflyIndexMetadata.IndexInfo> propertyIndexInfo = topContainer != null ?
                        graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, topContainer.getKey(), topContainer.getValue()) :
                        Optional.empty();
                if (propertyIndexInfo.isPresent()) {
                    // Index.
                    // Remove top container since it is captured inside property index info.
                    aerospikeSideHasContainers.remove(0);
                    return new QueryInfo(propertyIndexInfo.get(), topContainer, initialHasContainers);
                } else {
                    // Scan.
                    final Expression expression = GraphQueryHelper.hasContainerListToExpression(graph.getBaseGraph(), aerospikeSideHasContainers, FireflyVertex.class);
                    return new QueryInfo(expression, initialHasContainers);
                }
            } else {
                return new QueryInfo(initialHasContainers);
            }
        }
    }


    private static class Range {
        private static final int TOTAL_PARTITIONS = 4096;
        private final int start;
        private final int count;

        public Range(final int start, final int count) {
            this.start = start;
            this.count = count;
        }

        public static List<Range> splitPartitions(int bound) {
            final List<Range> ranges = new ArrayList<>();
            final int partitionsPerWorker = TOTAL_PARTITIONS / bound;
            int remainder = TOTAL_PARTITIONS % bound;

            int count;
            for (int start = 0; start < TOTAL_PARTITIONS; start += count) {
                count = partitionsPerWorker;

                // Distribute the remainder
                if (remainder > 0) {
                    count++;
                    remainder--;
                }
                ranges.add(new Range(start, count));
            }

            return ranges;
        }

        @Override
        public String toString() {
            return "[" + start + " - " + (start + count - 1) + "]";
        }
    }
}
