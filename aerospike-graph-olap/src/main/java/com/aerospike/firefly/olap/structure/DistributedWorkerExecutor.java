/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.olap.codec.BulkedRowSet;
import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.codec.RowCodec;
import com.aerospike.firefly.olap.config.DistributedConfigHelper;
import com.aerospike.firefly.olap.helper.TaskLogger;
import com.aerospike.firefly.olap.helper.TimeLog;
import com.aerospike.firefly.olap.iterators.IndexIterator;
import com.aerospike.firefly.olap.iterators.PIIterator;
import com.aerospike.firefly.olap.iterators.PIStepIterator;
import com.aerospike.firefly.olap.iterators.QueryInfo;
import com.aerospike.firefly.olap.iterators.ScanIterator;
import com.aerospike.firefly.olap.process.BatchJob;
import com.aerospike.firefly.olap.process.FireflyProgram;
import com.aerospike.firefly.olap.process.traversal.step.SparkOperation;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.FireflyHelper;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.aerospike.firefly.olap.codec.RowCodecHelper.getIdType;
import static com.aerospike.firefly.olap.helper.ProgramHelper.createVertexProgram;
import static com.aerospike.firefly.olap.process.TraversalProgram.MUTATED_MEMORY_KEYS;
import static com.aerospike.firefly.olap.process.TraversalProgram.SPARK_FLAG;
import static com.aerospike.firefly.olap.process.TraversalProgram.VOTE_TO_HALT;
import static com.aerospike.firefly.olap.structure.DistributedGraphComputer.magicSwap;
import static com.aerospike.firefly.process.traversal.step.util.TraversalUtil.fireflyTestAll;

public class DistributedWorkerExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedWorkerExecutor.class);

    public static final String START_COL = "~start";
    public static final String COUNT_COL = "~count";
    public static final String FIRST_COL = "~first";

    public static Dataset<Row> execute(final SparkSession spark,
                                       final FireflyGraph rootGraph,
                                       final DistributedConfigHelper configHelper,
                                       final List<HasContainer> initialHasContainers,
                                       final Object[] ids,
                                       final Traversal<?, ?> traversal,
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
                    schema,
                    maxParallelQuery,
                    isFirst,
                    partitions,
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
        // grab cache when we are on master
        final Map<String, Object> memoryCache = memory.getBroadcastValues();
        final int iteration = Arrays.asList(df.schema().fieldNames()).contains(Codec.ITERATION)
                ? (int) df.first().getAs(Codec.ITERATION) + 1
                : memory.getIteration();
        if (iteration != memory.getIteration() && configHelper.isDebugDf()) {
            TaskLogger.logDebuggingMessage("Iteration number mismatch, probably spark recovery is in progress. In memory"
                    + memory.getIteration() + "; in df " + iteration, LOGGER);
        }

        final GraphFilter graphFilter = FireflyHelper.getGraphComputerView(rootGraph).getGraphFilter();

        return df.mapPartitions((MapPartitionsFunction<Row, Row>) itty -> {
            String limitStepKey = null;
            TaskLogger.instance.setDebugging(configHelper.isDebugDf());
            TaskLogger.logDebuggingMessage("Starting with " + (itty.hasNext() ? "non-empty" : "empty") + " partition.", LOGGER);

            if (Thread.interrupted()) {
                throw new InterruptedException();
            }

            Iterator<Traverser> iterator = null;
            try (final FireflyGraph graph = FireflyGraph.open(configHelper.getFireflyConfig())) {
                graph.logInfo = TaskLogger.instance;
                memory.setGraph(graph);
                FireflyHelper.createGraphComputerView(graph, graphFilter, Collections.emptySet());

                TimeLog.reset();

                TaskLogger.logDebuggingMessage("Graph created.", LOGGER);

                // Create VertexProgram for worker and preset iteration start.
                final FireflyProgram vertexProgram = createVertexProgram(vertexProgramConfig, graph);
                if (!vertexProgram.validPostProcessSteps()) {
                    throw new IllegalStateException("Attempted to run an algorithm that does not filter results after execution. " +
                            "To apply filtering, please consult the documentation. " +
                            "If working with a small dataset, you may rerun the query with: " +
                            "\".with('aerospike.graph.analytics.unfiltered.algorithm.enabled', true)\"");
                }

                final Codec codec = vertexProgram.getCodec();
                final TraverserGenerator traverserGenerator = codec.getTraverserGenerator();
                final List<Step> steps = vertexProgram.getTraversal().get().getSteps();

                for (final Step step : steps) {
                    if (step instanceof RangeGlobalStep) {
                        final RangeGlobalStep rangeGlobalStep = (RangeGlobalStep) step;
                        if (rangeGlobalStep.getLowRange() != 0) {
                            break;
                        }
                        limitStepKey = AerospikeComputeKey.createAccumulator(rangeGlobalStep.getId());
                        break;
                    }
                }

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
                                    traverserGenerator);
                            iterator = FireflyCloseableIteratorUtils.filter(iterator, t -> {
                                final Element e = (Element) t.get();
                                return fireflyTestAll(e, queryInfo.fireflyHasContainers);
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
                                    queryInfo.expression,
                                    traverserGenerator);
                            iterator = FireflyCloseableIteratorUtils.filter(iterator, t -> {
                                final Element e = (Element) t.get();
                                return fireflyTestAll(e, queryInfo.fireflyHasContainers);
                            });
                            break;
                        case SUPERNODE:
                            final FireflyId ffid = graph.getIdFactory().createVertexId(queryInfo.ids.get(0));
                            final Traverser start = traverserGenerator.generate(graph.readVertex(ffid), (GraphStep) traversal.asAdmin().getStartStep(), 1L);
                            final Direction direction = ((VertexStep) traversal.asAdmin().getStartStep().getNextStep()).getDirection();
                            if (direction == Direction.BOTH) {
                                final List rows = IteratorUtils.toList(itty);
                                Iterator ittyIn = new PIStepIterator(
                                        graph,
                                        ffid,
                                        (GraphStep) traversal.asAdmin().getStartStep(),
                                        (VertexStep) traversal.asAdmin().getStartStep().getNextStep(),
                                        traversal.asAdmin().getStartStep().getNextStep().getNextStep().getId(),
                                        null,
                                        rows,
                                        traversal,
                                        start,
                                        Direction.IN);
                                Iterator ittyOut = new PIStepIterator(
                                        graph,
                                        ffid,
                                        (GraphStep) traversal.asAdmin().getStartStep(),
                                        (VertexStep) traversal.asAdmin().getStartStep().getNextStep(),
                                        traversal.asAdmin().getStartStep().getNextStep().getNextStep().getId(),
                                        null,
                                        rows,
                                        traversal,
                                        start,
                                        Direction.OUT);
                                iterator = FireflyCloseableIteratorUtils.concat(ittyIn, ittyOut);
                            } else {
                                iterator = new PIStepIterator(
                                        graph,
                                        ffid,
                                        (GraphStep) traversal.asAdmin().getStartStep(),
                                        (VertexStep) traversal.asAdmin().getStartStep().getNextStep(),
                                        traversal.asAdmin().getStartStep().getNextStep().getNextStep().getId(),
                                        null,
                                        IteratorUtils.toList(itty),
                                        traversal,
                                        start,
                                        direction);
                            }
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
                    iterator = FireflyCloseableIteratorUtils.map(itty, r -> codec.decode(r));
                }

                final LocalWorkerMemory workerMemory = new LocalWorkerMemory(memory, graph, memoryCache, iteration);

                vertexProgram.workerIterationStart(workerMemory.asImmutable());

                // Loop through input rows, transform to vertices, and execute workerVertexProgram.
                final TraverserSet<Object> traverserSet = new TraverserSet<>();

                int runningTotal = 0;
                final Runtime runtime = Runtime.getRuntime();
                final int maxBatchSize = configHelper.getBatchJobSize();

                TimeLog.complete("Setup");

                // only one of following will be used
                final BulkedRowSet output = new BulkedRowSet(codec);
                final List<Row> outputList = new ArrayList<>();
                boolean isBulkingDisabled = configHelper.isBulkingDisabled();
                int count = 0;
                while (iterator.hasNext()) {
                    count++;
                    while (traverserSet.size() < maxBatchSize && iterator.hasNext()) {
                        traverserSet.add(iterator.next().asAdmin());
                    }
                    TimeLog.complete("Read iterator");

                    if (configHelper.isForceGC()) {
                        if ((runtime.freeMemory() / (1024 * 1024)) < 200) {
                            TaskLogger.logDebuggingMessage("Running GC 2x.", LOGGER);
                            System.gc();
                            System.gc();
                            TaskLogger.logDebuggingMessage("GC complete.", LOGGER);
                        } else if ((runtime.freeMemory() / (1024 * 1024)) < 500) {
                            TaskLogger.logDebuggingMessage("Running GC 1x.", LOGGER);
                            System.gc();
                            TaskLogger.logDebuggingMessage("GC complete.", LOGGER);
                        }
                    }

                    if (configHelper.isDebugDf()) {
                        TaskLogger.logDebuggingMessage("Input TraverserSet size: " + traverserSet.size() + "/" + runningTotal
                                + " Total allocated(Mb)=" + runtime.totalMemory() / (1024 * 1024) +
                                ", Free memory=" + runtime.freeMemory() / (1024 * 1024) +
                                " Used memory=" + (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024), LOGGER);
                    }
                    if (TaskContext.get().isInterrupted()) {
                        throw new InterruptedException();
                    }
                    TimeLog.complete("Debug logging");

                    runningTotal += traverserSet.size();
                    if (!traverserSet.isEmpty()) {
                        TaskLogger.logDebuggingMessage("Step: " + traverserSet.peek().getStepId(), LOGGER);
                    }

                    final BatchJob job = new BatchJob(traverserSet);
                    vertexProgram.execute(job, workerMemory);
                    traverserSet.clear();
                    TimeLog.complete("Running BatchJob");

                    if (!isBulkingDisabled) {
                        output.addAll(job.getResults());
                    } else {
                        for (final Traverser.Admin t : job.getResults()) {
                            outputList.add(codec.encode(t));
                        }
                    }
                    job.clear();
                    TimeLog.complete("Encoding");

                    if (limitStepKey != null && (Long) memory.get(limitStepKey) <= 0) {
                        TaskLogger.logDebuggingMessage("Limit reached " + memory.get(limitStepKey), LOGGER);
                        break;
                    }
                    if (count % 20 == 0)
                        TimeLog.log(graph);
                }

                if (configHelper.isDebugDf()) {
                    TaskLogger.logDebuggingMessage("Ending with " + (isBulkingDisabled ? outputList.size() : output.rowCount()) + " rows."
                            + " Total allocated(Mb)=" + runtime.totalMemory() / (1024 * 1024) +
                            ", Free memory=" + runtime.freeMemory() / (1024 * 1024) +
                            " Used memory=" + (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024), LOGGER);
                }

                // End worker iteration.
                vertexProgram.workerIterationEnd(workerMemory.asImmutable());
                workerMemory.complete();

                TimeLog.complete("Worker iteration end");

                // Return results.
                TimeLog.log(graph);
                if (isBulkingDisabled) {
                    TaskLogger.logDebuggingMessage("Returning " + outputList.size() + " rows.", LOGGER);
                    return outputList.iterator();
                }
                return output.iterator();
            } catch (final Exception e) {
                TaskLogger.logDebuggingMessage("ERROR", LOGGER);
                e.printStackTrace();
                throw e;
            } finally {
                if (iterator != null) {
                    if (iterator instanceof CloseableIterator) {
                        ((CloseableIterator) iterator).close();
                    }
                }
            }
        }, Encoders.row(schema));
    }

    public static Pair<Boolean, Dataset<Row>> executeNative(final Traversal<?, ?> traversal,
                                                            final Dataset<Row> input,
                                                            final DistributedMemory memory,
                                                            final Codec codec) {
        // no spark native step or not reached so far
        if (!memory.exists(SPARK_FLAG) || !memory.<Boolean>get(SPARK_FLAG)) {
            return Pair.with(false, null);
        }

        if (input.isEmpty()) {
            memory.set(SPARK_FLAG, false);
            return Pair.with(false, null);
        }

        // another option to get current sortStep is from MUTATED_MEMORY_KEYS
        final Traverser.Admin t = codec.decode(input.first()).asAdmin();
        final Optional<Step> step = traversal.asAdmin().getSteps().stream().filter(s -> s.getId().equals(t.getStepId())).findFirst();

        // travserser already set to next step, so need to step back to get SparkOperation.
        // Or SparkOperation can be last step
        final SparkOperation op = step.isPresent() ? (SparkOperation) step.get().getPreviousStep() : (SparkOperation) traversal.asAdmin().getEndStep();
        final Dataset<Row> results = op.operate(input);

        if (op.canContinueDistributed())
            return Pair.with(true, results);

        // copy rows to traverserSet
        final List<Row> rows = results.collectAsList();
        final TraverserSet traversers = new TraverserSet();
        rows.stream().forEach(row -> traversers.add(codec.decode(row).asAdmin()));

        // set barrier with sorted data
        memory.set(((Step) op).getId(), traversers);
        memory.set(MUTATED_MEMORY_KEYS, new HashSet<>(Collections.singleton(((Step) op).getId())));
        memory.set(VOTE_TO_HALT, true);

        // following steps will be local
        return Pair.with(true, results.limit(0));
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
                                                                   final Optional<Integer> partitions,
                                                                   final int workerCount) {
        QueryInfo queryInfo;
        if (isFirst) {
            traversal.asAdmin().applyStrategies();
            queryInfo = QueryInfo.getQueryInfo(rootGraph, (GraphStep) traversal.asAdmin().getStartStep(), initialHasContainers, ids, configHelper);
            // Special case for returning something like g.V().hasId(List.of())
            if (queryInfo == null) {
                return new Pair<>(spark.createDataFrame(new ArrayList<>(), outputSchema), queryInfo);
            }
            if (configHelper.isDebugDf()) {
                System.out.println("Query type: " + queryInfo.queryType.toString());
            }
            System.out.println("Generating query ranges for " + maxParallelQuery + " max parallel queries and " + workerCount + " workers.");
            if (queryInfo.queryType.equals(QueryInfo.QueryType.INDEX) ||
                    queryInfo.queryType.equals(QueryInfo.QueryType.SCAN) ||
                    queryInfo.queryType.equals(QueryInfo.QueryType.SUPERNODE)) {
                final List<Row> queryRanges;
                final StructType inputSchema;
                if (!queryInfo.queryType.equals(QueryInfo.QueryType.SUPERNODE)) {
                    if (partitions.isPresent()) {
                        queryRanges = Range.splitPartitions(partitions.get()).
                                stream().map(range -> RowFactory.create(range.start, range.count)).collect(Collectors.toList());
                    } else {
                        queryRanges = Range.splitPartitions(Math.min(maxParallelQuery, workerCount)).
                                stream().map(range -> RowFactory.create(range.start, range.count)).collect(Collectors.toList());
                    }
                    inputSchema = new StructType().
                            add(START_COL, DataTypes.IntegerType, false).
                            add(COUNT_COL, DataTypes.IntegerType, false);
                } else {
                    final List<Range> ranges;
                    if (partitions.isPresent()) {
                        ranges = Range.splitPartitions(partitions.get());
                    } else {
                        ranges = Range.splitPartitions(Math.min(maxParallelQuery, workerCount));
                    }
                    queryRanges = new ArrayList<>();
                    for (int i = 0; i < ranges.size(); i++) {
                        final Range range = ranges.get(i);
                        queryRanges.add(RowFactory.create(range.start, range.count, i == 0));
                    }
                    inputSchema = new StructType().
                            add(START_COL, DataTypes.IntegerType, false).
                            add(COUNT_COL, DataTypes.IntegerType, false).
                            add(FIRST_COL, DataTypes.BooleanType, false);
                }
                if (configHelper.isDebugDf()) {
                    System.out.println("Query ranges: " + queryRanges.size());
                }
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
                if (partitions.isPresent()) {
                    return new Pair<>(magicSwap(idDataset.repartition(partitions.get())), queryInfo);
                } else {
                    return new Pair<>(magicSwap(idDataset.repartition(maxParallelQuery)), queryInfo);
                }
            }
        } else {
            throw new RuntimeException("Error, input is null and this is not the first step. Please contact support.");
        }
    }
}
