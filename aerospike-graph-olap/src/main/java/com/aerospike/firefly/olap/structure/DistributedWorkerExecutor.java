package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.olap.codec.BulkedRowSet;
import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.codec.RowCodec;
import com.aerospike.firefly.olap.config.DistributedConfigHelper;
import com.aerospike.firefly.olap.helper.TaskLogger;
import com.aerospike.firefly.olap.helper.TimeLog;
import com.aerospike.firefly.olap.iterators.IndexIterator;
import com.aerospike.firefly.olap.iterators.PIIterator;
import com.aerospike.firefly.olap.iterators.QueryInfo;
import com.aerospike.firefly.olap.iterators.ScanIterator;
import com.aerospike.firefly.olap.process.BatchJob;
import com.aerospike.firefly.olap.process.TraversalProgram;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.TaskContext;
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
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.DefaultTraverserGeneratorFactory;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.olap.codec.RowCodecHelper.getIdType;
import static com.aerospike.firefly.olap.structure.DistributedGraphComputer.magicSwap;

public class DistributedWorkerExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedWorkerExecutor.class);

    public static final String START_COL = "~start";
    public static final String COUNT_COL = "~count";

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
        final Optional<Integer> partitions = configHelper.getPartitions();
        Dataset<Row> df;
        QueryInfo queryInfo;
        if (input != null) {
            queryInfo = null;
            df = input;
            if (configHelper.isDebugDf()) {
                System.out.println("Starting with " + input.rdd().partitions().length + " partitions.");
            }
            if (partitions.isPresent()) {
                df = magicSwap(df.repartition(partitions.get()));
            } else {
                df = magicSwap(df.repartition(workerCount));
            }
        } else {
            final Pair<Dataset<Row>, QueryInfo> initialInfo = getInitialDataset(spark,
                    rootGraph,
                    configHelper,
                    initialHasContainers,
                    ids,
                    traversal,
                    outputSchema,
                    maxParallelQuery,
                    isFirst,
                    workerCount);
            df = initialInfo.getValue0();
            queryInfo = initialInfo.getValue1();
            if (configHelper.isDebugDf()) {
                System.out.println("Starting with " + df.rdd().partitions().length + " partitions.");
            }
            if (partitions.isPresent()) {
                df = magicSwap(df.repartition(partitions.get()));
            }
        }
        if (configHelper.isDebugDf()) {
            System.out.println("Ending with " + df.rdd().partitions().length + " partitions.");
        }
        return df.mapPartitions((MapPartitionsFunction<Row, Row>) itty -> {
            TaskLogger.instance.setDebugging(configHelper.isDebugDf());
            TaskLogger.logDebuggingMessage("Starting with " + (itty.hasNext() ? "non-empty" : "empty") + " partition.", LOGGER);

            if (Thread.interrupted()) {
                throw new InterruptedException();
            }

            try (final FireflyGraph graph = FireflyGraph.open(configHelper.getFireflyConfig())) {
                graph.logInfo = TaskLogger.instance;
                TimeLog.reset();

                TaskLogger.logDebuggingMessage("Graph created.", LOGGER);

                final Codec codec = new Codec(traversal);
                final VertexProgram vertexProgram = VertexProgram.createVertexProgram(graph, vertexProgramConfig);

                final PureTraversal<?, ?> pureTraversal = ((TraversalProgram) vertexProgram).getTraversal().clone();
                pureTraversal.get().applyStrategies();
                final Traversal traversal1 = pureTraversal.get();

                final Set<TraverserRequirement> traverserRequirements = traversal1.asAdmin().getTraverserRequirements();
                final TraverserGenerator traverserGenerator = DefaultTraverserGeneratorFactory.instance().getTraverserGenerator(traverserRequirements);
                final TraversalMatrix<?, ?> traversalMatrix = new TraversalMatrix<>(traversal1.asAdmin());
                Iterator<Traverser> iterator;
                if (isFirst) {
                    switch (queryInfo.queryType) {
                        case INDEX:
                            iterator = new IndexIterator(
                                    graph,
                                    (GraphStep) traversal.asAdmin().getStartStep(),
                                    queryInfo.indexTopHasContainer,
                                    queryInfo.fireflyHasContainers,
                                    traversal.asAdmin().getStartStep().getNextStep().getId(),
                                    codec,
                                    queryInfo.indexInfo,
                                    itty,
                                    traversal,
                                    traversalMatrix,
                                    traverserGenerator);
                            iterator = FireflyCloseableIteratorUtils.filter(iterator, t -> {
                                final Element e = (Element) t.get();
                                return HasContainer.testAll(e, queryInfo.fireflyHasContainers);
                            });
                            break;
                        case SCAN:
                            iterator = new ScanIterator(
                                    graph,
                                    (GraphStep) traversal.asAdmin().getStartStep(),
                                    queryInfo.indexTopHasContainer,
                                    queryInfo.fireflyHasContainers,
                                    traversal.asAdmin().getStartStep().getNextStep().getId(),
                                    codec,
                                    itty,
                                    traversal,
                                    traversalMatrix,
                                    queryInfo.expression,
                                    traverserGenerator);
                            iterator = FireflyCloseableIteratorUtils.filter(iterator, t -> {
                                final Element e = (Element) t.get();
                                return HasContainer.testAll(e, queryInfo.fireflyHasContainers);
                            });
                            break;
                        case PI:
                            iterator = PIIterator.getIterator(
                                    graph,
                                    (GraphStep) traversal.asAdmin().getStartStep(),
                                    queryInfo.fireflyHasContainers,
                                    traversal.asAdmin().getStartStep().getNextStep(),
                                    traverserGenerator,
                                    itty);
                            break;
                        default:
                            throw new IllegalStateException("Unknown query type: " + queryInfo.queryType);
                    }
                } else {
                    iterator = FireflyCloseableIteratorUtils.map(itty, r -> codec.decode(r, traverserGenerator, traversalMatrix));
                }

                final LocalWorkerMemory workerMemory = new LocalWorkerMemory(memory);

                // Create VertexProgram for worker and preset iteration start.
                final TraversalProgram workerVertexProgram = vertexProgram instanceof TraversalProgram
                        ? (TraversalProgram) vertexProgram
                        : new TraversalProgram((TraversalVertexProgram) vertexProgram);

                workerVertexProgram.workerIterationStart(workerMemory.asImmutable());

                // Loop through input rows, transform to vertices, and execute workerVertexProgram.
                final TraverserSet<Object> traverserSet = new TraverserSet<>();

                int runningTotal = 0;
                final Runtime runtime = Runtime.getRuntime();
                final int maxBatchSize = graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE;

                TimeLog.complete("Setup");

                final BulkedRowSet output = new BulkedRowSet(codec);
                while (iterator.hasNext()) {
                    TaskLogger.logDebuggingMessage("Input TraverserSet size: " + traverserSet.size() + "/" + runningTotal
                            + " Total allocated=" + runtime.totalMemory() / (1024 * 1024 * 1024) +
                            ", Free memory=" + runtime.freeMemory() / (1024 * 1024 * 1024) +
                            " Used memory=" + (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024 * 1024), LOGGER);
                    while (traverserSet.size() < maxBatchSize && iterator.hasNext()) {
                        traverserSet.add(iterator.next().asAdmin());
                    }
                    if (TaskContext.get().isInterrupted()) {
                        throw new InterruptedException();
                    }
                    TimeLog.complete("Read iterator");

                    runningTotal += traverserSet.size();
                    if (!traverserSet.isEmpty()) {
                        TaskLogger.logDebuggingMessage("Step: " + new ArrayList<>(traverserSet).get(0).getStepId(), LOGGER);
                    }

                    final BatchJob job = new BatchJob(traverserSet);
                    workerVertexProgram.execute(job, workerMemory);
                    traverserSet.clear();
                    TimeLog.complete("Running BatchJob");

                    // TODO: Is this correct for all cases ?
                    final TraverserSet<Traverser.Admin> traversers = job.getResults();
                    output.addAll(traversers);
                    job.clear();
                    TimeLog.complete("Encoding");
                }

                // End worker iteration.
                workerVertexProgram.workerIterationEnd(workerMemory.asImmutable());
                workerMemory.complete();

                TimeLog.complete("Worker iteration end");

                // Return results.
                TaskLogger.logDebuggingMessage("Ending with " + output.rowCount() + " rows.", LOGGER);
                TimeLog.log(graph);
                return output.iterator();
            } catch (final Exception e) {
                TaskLogger.logDebuggingMessage("ERROR", LOGGER);
                e.printStackTrace();
                throw e;
            }
        }, RowEncoder.apply(schema));
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

    private static Pair<Dataset<Row>, QueryInfo> getInitialDataset(final SparkSession spark,
                                                                   final FireflyGraph rootGraph,
                                                                   final DistributedConfigHelper configHelper,
                                                                   final List<HasContainer> initialHasContainers,
                                                                   final Object[] ids,
                                                                   final Traversal<?, ?> traversal,
                                                                   final StructType outputSchema,
                                                                   final int maxParallelQuery,
                                                                   final boolean isFirst,
                                                                   final int workerCount) {
        QueryInfo queryInfo;
        if (isFirst) {
            traversal.asAdmin().applyStrategies();
            queryInfo = QueryInfo.getQueryInfo(rootGraph, (GraphStep) traversal.asAdmin().getStartStep(), initialHasContainers, ids);
            // Special case for returning something like g.V().hasId(List.of())
            if (queryInfo == null) {
                return new Pair<>(spark.createDataFrame(new ArrayList<>(), outputSchema), queryInfo);
            }
            if (configHelper.isDebugDf()) {
                System.out.println("Query type: " + queryInfo.queryType.toString());
            }
            System.out.println("Generating query ranges for " + maxParallelQuery + " max parallel queries and " + workerCount + " workers.");
            if (queryInfo.queryType.equals(QueryInfo.QueryType.INDEX) || queryInfo.queryType.equals(QueryInfo.QueryType.SCAN)) {
                final List<Row> queryRanges = Range.splitPartitions(Math.min(maxParallelQuery, workerCount)).
                        stream().map(range -> RowFactory.create(range.start, range.count)).collect(Collectors.toList());
                if (configHelper.isDebugDf()) {
                    System.out.println("Query ranges: " + queryRanges.size());
                }
                final StructType inputSchema = new StructType().
                        add(START_COL, DataTypes.IntegerType, false).
                        add(COUNT_COL, DataTypes.IntegerType, false);
                Dataset<Row> queryRangeDataset = spark.createDataFrame(queryRanges, inputSchema);
                if (configHelper.isDebugDf()) {
                    System.out.println("Starting query with " + queryRangeDataset.rdd().partitions().length + " partitions.");
                }
                queryRangeDataset = magicSwap(queryRangeDataset.repartitionByRange(queryRanges.size(), new Column(START_COL)));
                if (configHelper.isDebugDf()) {
                    System.out.println("Repartitioned query with " + queryRangeDataset.rdd().partitions().length + " partitions.");
                    System.out.println("Partition balancing for index: " + queryRangeDataset.javaRDD().mapPartitions(iter -> {
                        long count = 0;
                        while (iter.hasNext()) {
                            iter.next();
                            count++;
                        }
                        return java.util.Collections.singletonList(count).iterator();
                    }).collect());
                }
                return new Pair<>(queryRangeDataset, queryInfo);
            } else {
                final List<Object> initialIds = queryInfo.ids;
                if (configHelper.isDebugDf()) {
                    System.out.println("IDS: " + initialIds);
                }
                final List<Object> idsList = new ArrayList<>();
                if (initialIds.size() == 1 && initialIds.get(0) instanceof P) {
                    final P p = (P) initialIds.get(0);
                    if (!p.getBiPredicate().toString().equals("within")) {
                        throw new IllegalArgumentException("Batch read only supports within predicate");
                    }
                    if (!(p.getValue() instanceof List)) {
                        throw new IllegalArgumentException("Batch read only supports a single list of keys");
                    }
                    idsList.addAll((List) p.getValue());
                } else {
                    idsList.addAll(initialIds);
                }
                if (configHelper.isDebugDf()) {
                    System.out.println("Actual ids: " + Arrays.toString(ids));
                }
                initialIds.clear();
                final List<Row> rows = idsList.stream().filter(Objects::nonNull).map(id -> RowFactory.create(id.toString(), getIdType(id).ordinal())).collect(Collectors.toList());
                final StructType inputSchema = new StructType().
                        add(RowCodec.ID_COL, DataTypes.StringType, false).
                        add(RowCodec.ID_TYPEHINT_COL, DataTypes.IntegerType, false);
                Dataset<Row> idDataset = spark.createDataFrame(rows, inputSchema);
                if (configHelper.isDebugDf()) {
                    System.out.println("Starting query with " + idDataset.rdd().partitions().length + " partitions.");
                }
                return new Pair<>(magicSwap(idDataset.repartition(maxParallelQuery)), queryInfo);
            }
        } else {
            throw new RuntimeException("Error, input is null and this is not the first step. Please contact support.");
        }
    }
}
