package com.aerospike.firefly.olap.structure;

import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.query.paged.GraphQueryHelper;
import com.aerospike.firefly.io.aerospike.query.paged.PageFetcher;
import com.aerospike.firefly.io.aerospike.query.paged.PaginationIterator;
import com.aerospike.firefly.io.aerospike.query.paged.PartitionedSindexPageFetcher;
import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.codec.RowCodec;
import com.aerospike.firefly.olap.config.DistributedConfigHelper;
import com.aerospike.firefly.olap.helper.TaskLogger;
import com.aerospike.firefly.olap.process.BatchJob;
import com.aerospike.firefly.olap.process.TraversalProgram;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.TimeoutHelper;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.DefaultTraverserGeneratorFactory;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DistributedWorkerExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedWorkerExecutor.class);

    private static final String START_COL = "~start";
    private static final String COUNT_COL = "~count";

    public static Dataset<Row> execute(final SparkSession spark,
                                       final FireflyGraph rootGraph,
                                       final DistributedConfigHelper configHelper,
                                       final List<HasContainer> initialHasContainers,
                                       final Object[] ids,
                                       final Traversal<?, ?> traversal,
                                       final StructType outputSchema,
                                       final int maxParallelQuery,
                                       final boolean isFirst,
                                       Dataset<Row> input,
                                       final DistributedMemory memory,
                                       final Configuration vertexProgramConfig,
                                       final StructType schema,
                                       final int workerCount) {
        QueryInfo queryInfo;
        Dataset<Row> df = null;
        if (input != null) {
            df = input;
            queryInfo = null;
            System.out.println("Starting with " + input.rdd().partitions().length + " partitions.");
            if (input.rdd().partitions().length < workerCount / 2) {
                System.out.println("Repartitioning to " + workerCount + " partitions.");
                input = DistributedGraphComputer.magicSwap(input.repartition(workerCount));
            }
            System.out.println("Ending with " + input.rdd().partitions().length + " partitions.");
        } else if (isFirst) {
            traversal.asAdmin().applyStrategies();
            queryInfo = QueryInfo.getQueryInfo(rootGraph, (GraphStep) traversal.asAdmin().getStartStep(), initialHasContainers, ids);
            // Special case for returning something like g.V().hasId(List.of())
            if (queryInfo == null) {
                return spark.createDataFrame(new ArrayList<Row>(), outputSchema);
            }
            if (queryInfo.queryType.equals(QueryInfo.QueryType.INDEX) || queryInfo.queryType.equals(QueryInfo.QueryType.SCAN)) {
                final List<Row> queryRanges = Range.splitPartitions(Math.min(maxParallelQuery, workerCount)).
                        stream().map(range -> RowFactory.create(range.start, range.count)).collect(Collectors.toList());
                System.out.println("Query ranges: " + queryRanges.size());
                final StructType inputSchema = new StructType().
                        add(START_COL, DataTypes.IntegerType, false).
                        add(COUNT_COL, DataTypes.IntegerType, false);
                Dataset<Row> queryRangeDataset = spark.createDataFrame(queryRanges, inputSchema);
                System.out.println("Starting query with " + queryRangeDataset.rdd().partitions().length + " partitions.");
                queryRangeDataset = DistributedGraphComputer.magicSwap(queryRangeDataset.repartitionByRange(queryRanges.size(), new Column(START_COL)));
                //queryRangeDataset.
                System.out.println("Repartitioned query with " + queryRangeDataset.rdd().partitions().length + " partitions.");
                System.out.println("Partition balancing for index: " + queryRangeDataset.javaRDD().mapPartitions(iter -> {
                    long count = 0;
                    while (iter.hasNext()) {
                        iter.next();
                        count++;
                    }
                    return java.util.Collections.singletonList(count).iterator();
                }).collect());
                df = queryRangeDataset;
            } else {
                throw new RuntimeException("Error, input is null and this is not the first step. Please contact support.");
            }
        } else {
            queryInfo = null;
            throw new RuntimeException("Error, input is null and this is not the first step. Please contact support.");
        }
        return df.mapPartitions((MapPartitionsFunction<Row, Row>) itty -> {
            TaskLogger.logDebuggingMessage("starting with " + (itty.hasNext() ? "non-empty" : "empty") + " partition.", LOGGER);

            // Open graph.
            Iterator<Traverser> iterator;
            try (FireflyGraph graph = FireflyGraph.open(configHelper.getFireflyConfig())) {
                final Codec codec = new Codec(traversal);
                final VertexProgram vertexProgram = VertexProgram.createVertexProgram(graph, vertexProgramConfig);

                final PureTraversal<?, ?> pureTraversal = ((TraversalProgram) vertexProgram).getTraversal().clone();
                pureTraversal.get().applyStrategies();
                final Traversal traversal1 = pureTraversal.get();

                final Set<TraverserRequirement> traverserRequirements = traversal1.asAdmin().getTraverserRequirements();
                final TraverserGenerator traverserGenerator = DefaultTraverserGeneratorFactory.instance().getTraverserGenerator(traverserRequirements);
                final TraversalMatrix<?, ?> traversalMatrix = new TraversalMatrix<>(traversal1.asAdmin());
                if (isFirst) {
                    switch (queryInfo.queryType) {
                        case INDEX:
                            iterator = new IndexIterator(
                                    graph,
                                    queryInfo.indexTopHasContainer,
                                    queryInfo.fireflyHasContainers,
                                    traversal.asAdmin().getStartStep().getNextStep().getId(),
                                    codec,
                                    queryInfo.indexInfo,
                                    itty,
                                    traversal,
                                    traversalMatrix,
                                    traverserGenerator);
                            break;
                        case PI:
                            iterator = IteratorUtils.emptyIterator();
                            break;
                        case SCAN:
                            iterator = IteratorUtils.emptyIterator();
                            break;
                        default:
                            throw new IllegalStateException("Unknown query type: " + queryInfo.queryType);
                    }
                } else {
                    iterator = FireflyCloseableIteratorUtils.map(itty, r -> codec.decode(r, traverserGenerator, traversalMatrix));
                }


                graph.logInfo = TaskLogger.instance;
                final List<Row> output = new ArrayList<>();

                // Set memory is in execute.
                memory.setInExecute(true);

                // Create VertexProgram for worker and prset iteration start.
                final TraversalProgram workerVertexProgram = vertexProgram instanceof TraversalProgram
                        ? (TraversalProgram) vertexProgram
                        : new TraversalProgram((TraversalVertexProgram) vertexProgram);

                workerVertexProgram.workerIterationStart(memory.asImmutable());

                // Loop through input rows, transform to vertices, and execute workerVertexProgram.
                final TraverserSet<Object> traverserSet = new TraverserSet<>();

                int runningTotal = 0;
                while (iterator.hasNext()) {
                    while (traverserSet.size() < 5000 && iterator.hasNext()) {
                        System.out.println("Adding traverser");
                        traverserSet.add(iterator.next().asAdmin());
                    }
                    System.out.println("traverserSet: " + traverserSet);

                    runningTotal += traverserSet.size();
                    TaskLogger.logDebuggingMessage("Input TraverserSet size: " + traverserSet.size() + "/" + runningTotal, LOGGER);
                    if (!traverserSet.isEmpty()) {
                        TaskLogger.logDebuggingMessage("Step: " + new ArrayList<>(traverserSet).get(0).getStepId(), LOGGER);
                    }

                    final BatchJob job = new BatchJob(traverserSet);
                    workerVertexProgram.execute(job, memory);
                    traverserSet.clear();

                    // TODO: Is this correct for all cases ?
                    final TraverserSet<Traverser.Admin> traversers = job.getResults();
                    traversers.forEach(t -> output.add(codec.encode(t)));
                    job.clear();
                }

                // End worker iteration.
                workerVertexProgram.workerIterationEnd(memory.asImmutable());

                // Set memory is not in execute.
                memory.setInExecute(false);

                // Return results.
                TaskLogger.logDebuggingMessage("ending with " + output.size() + " rows.", LOGGER);
                return output.iterator();
            } catch (Exception e) {
                TaskLogger.logDebuggingMessage("ERROR", LOGGER);
                e.printStackTrace();
                throw e;
            }
        }, RowEncoder.apply(schema));
    }

    static class IndexIterator implements CloseableIterator<Traverser> {
        final FireflyGraph graph;
        final Filter filter;
        final LinkedBlockingQueue<PageFetcher.Page> pageQueue = new LinkedBlockingQueue<>();
        final List<Row> rows = new ArrayList<>();
        int rowCount = 0;
        final List<HasContainer> hasContainers;
        final String startStep;
        final Codec codec;
        final Traversal traversal;
        final FireflyIndexMetadata.IndexInfo indexInfo;
        final TraverserGenerator tg;
        final TraversalMatrix tm;
        PageFetcher<?> pageFetcher = null;
        PageFetcher.Page page = null;

        IndexIterator(final FireflyGraph graph,
                      final HasContainer hasContainer,
                      final List<HasContainer> hasContainers,
                      final String startStep,
                      final Codec codec,
                      final FireflyIndexMetadata.IndexInfo indexInfo,
                      final Iterator<Row> iterator,
                      final Traversal traversal,
                      final TraversalMatrix tm,
                      final TraverserGenerator tg) {
            this.tg = tg;
            this.tm = tm;
            this.indexInfo = indexInfo;
            this.graph = graph;
            this.filter = GraphQueryHelper.predicateToFilter(graph.getBaseGraph(), hasContainer.getPredicate(), indexInfo);
            while (iterator.hasNext()) {
                rows.add(iterator.next());
            }
            this.hasContainers = hasContainers;
            this.startStep = startStep;
            this.codec = codec;
            this.traversal = traversal;
        }

        @Override
        public boolean hasNext() {
            System.out.println("hasNext");
            if (rows.isEmpty() || rows.size() == rowCount) {
                return false;
            }

            if (page != null) {
                if (page.keyRecords.hasNext()) {
                    return true;
                } else {
                    final PaginationIterator pi = (PaginationIterator) page.keyRecords;
                    pi.close();
                }
            }

            // Now current index.
            if (pageFetcher == null) {
                int attemptCount = 0;
                while (true) {
                    try {
                        System.out.println("???");
                        final Row row = rows.get(rowCount);
                        final PartitionFilter partitionFilter = PartitionFilter.range(
                                row.getInt(row.fieldIndex(START_COL)),
                                row.getInt(row.fieldIndex(COUNT_COL)));

                        TaskLogger.logDebuggingMessage("Row number " + rowCount, LOGGER);

                        // TODO: Can omit some bins here.
                        final int evaluationTimeout = Long.valueOf(TimeoutHelper.calculate(traversal.asAdmin())).intValue();
                        final QueryPolicy policy = new QueryPolicy();
                        policy.setTimeout(evaluationTimeout);
                        attemptCount++;
                        pageFetcher = new PartitionedSindexPageFetcher<>(
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
                        break;
                    } catch (final Exception e) {
                        TaskLogger.logDebuggingMessage("Got exception " + e.getMessage(), LOGGER);
                        if (attemptCount > 10) {
                            throw new RuntimeException("Failed to run query after " + attemptCount + " attempts.", e);
                        } else if (e.getMessage().contains("Operation not allowed at this time")) {
                            TaskLogger.logDebuggingMessage("Sleeping.", LOGGER);
                            try {
                                Thread.sleep(1000L * (attemptCount + 1));
                            } catch (InterruptedException e1) {
                                throw new RuntimeException(e1);
                            }
                        }
                    }
                }
            }

            try {
                page = pageQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (page instanceof PageFetcher.ErrorPage) {
                System.out.println("Error page");
                final PageFetcher.ErrorPage errorPage = (PageFetcher.ErrorPage) page;
                throw new RuntimeException("Error fetching page: " + errorPage.errorMessage, errorPage.exception);
            } else if (page instanceof PageFetcher.PoisonPill) {
                System.out.println("Poison pill");
                pageFetcher.shutdownAwait();
                pageFetcher = null;
                page = null;
                rowCount++;
            }

            return hasNext();
        }

        @Override
        public Traverser next() {
            System.out.println("next");
            if (hasNext()) {
                if (!page.keyRecords.hasNext()) {
                    throw new NoSuchElementException("No more elements. Please contact support.");
                }
                final KeyRecord kr = page.keyRecords.next();
                final FireflyVertex vertex = graph.vertexFromRecord(kr);
                System.out.println("Creating traverser " + vertex.id.getUserId());
                Traverser t = tg.generate(vertex, tm.getStepById(startStep), 1L);
                t.asAdmin().setStepId(startStep);
                return t;
            } else {
                throw new NoSuchElementException("No more elements.");
            }
        }

        @Override
        public void close() {
            if (page != null) {
                final PaginationIterator pi = (PaginationIterator) page.keyRecords;
                pi.close();
            }
            if (pageFetcher != null) {
                pageFetcher.shutdownAwait();
            }
        }
    }


    public static class QueryInfo implements Serializable {
        enum QueryType implements Serializable {
            INDEX,
            SCAN,
            PI
        }

        private final QueryInfo.QueryType queryType;
        private final List<HasContainer> fireflyHasContainers;
        private final FireflyIndexMetadata.IndexInfo indexInfo; // Only valid for index queries.
        private final HasContainer indexTopHasContainer; // Only valid for index queries.
        private final List<Object> ids; // Only valid for PI queries.
        private final Expression expression; // Only valid for scan queries.

        private QueryInfo(final FireflyIndexMetadata.IndexInfo indexInfo,
                          final HasContainer hasContainer,
                          final List<HasContainer> initialHasContainers) {
            this.queryType = QueryInfo.QueryType.INDEX;
            this.indexInfo = indexInfo;
            this.indexTopHasContainer = hasContainer;
            this.ids = null;
            this.expression = null;
            this.fireflyHasContainers = initialHasContainers;
        }

        private QueryInfo(final List<Object> ids,
                          final List<HasContainer> initialHasContainers) {
            this.queryType = QueryInfo.QueryType.PI;
            this.indexInfo = null;
            this.indexTopHasContainer = null;
            this.ids = ids;
            this.expression = null;
            this.fireflyHasContainers = initialHasContainers;
        }

        private QueryInfo(final Expression expression,
                          final List<HasContainer> initialHasContainers) {
            this.queryType = QueryInfo.QueryType.SCAN;
            this.indexInfo = null;
            this.indexTopHasContainer = null;
            this.ids = null;
            this.expression = expression;
            this.fireflyHasContainers = initialHasContainers;
        }

        private QueryInfo(final List<HasContainer> initialHasContainers) {
            this.queryType = QueryInfo.QueryType.SCAN;
            this.indexInfo = null;
            this.indexTopHasContainer = null;
            this.ids = null;
            this.expression = null; // Scan all.
            this.fireflyHasContainers = initialHasContainers;
        }

        private static QueryInfo getQueryInfo(final FireflyGraph graph,
                                              final GraphStep step,
                                              final List<HasContainer> initialHasContainers,
                                              final Object[] idsInput) {
            final List<HasContainer> positiveFilters = initialHasContainers.stream().filter(it -> !it.getBiPredicate().equals(Contains.without)).collect(Collectors.toList());

            // Special case.
            if (idsInput == null) {
                return null;
            }
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

    public static class Range {
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
