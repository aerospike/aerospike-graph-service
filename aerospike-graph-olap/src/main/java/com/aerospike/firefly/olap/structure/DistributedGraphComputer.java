package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.config.DistributedConfigHelper;
import com.aerospike.firefly.olap.config.DistributedConfiguration;
import com.aerospike.firefly.olap.helper.AttachmentHelper;
import com.aerospike.firefly.olap.process.FireflyProgram;
import com.aerospike.firefly.olap.process.TraversalProgram;
import com.aerospike.firefly.olap.process.packing.DistributedAerospikeConnection;
import com.aerospike.firefly.olap.process.AlgorithmProgram;
import com.aerospike.firefly.olap.process.traversal.strategy.SparkOptimizationStrategy;
import com.aerospike.firefly.olap.service.JobCancellationService;
import com.aerospike.firefly.olap.service.JobClearService;
import com.aerospike.firefly.olap.service.JobListService;
import com.aerospike.firefly.olap.structure.job.Job;
import com.aerospike.firefly.process.computer.local.LocalGraphComputerView;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.strategy.verification.FireflyComputerVerificationStrategy;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.FireflyHelper;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.MessagePassingReductionStrategy;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CallStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aerospike.firefly.olap.codec.RowCodec.HALTED_COL;
import static com.aerospike.firefly.olap.helper.ProgramHelper.createVertexProgram;
import static org.apache.spark.sql.functions.lit;
import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.HALTED_TRAVERSERS;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DistributedGraphComputer implements GraphComputer {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedGraphComputer.class);
    private ResultGraph resultGraph = null;
    private Persist persist = null;
    private SparkSession spark = null;

    private FireflyProgram vertexProgram;
    private final FireflyGraph graph;
    private final Set<MapReduce> mapReducers = new HashSet<>();
    private int workers = -1;
    private final GraphFilter graphFilter = new GraphFilter();
    private boolean executed = false;

    private final ThreadFactory threadFactoryBoss = new BasicThreadFactory.Builder().
            namingPattern(DistributedGraphComputer.class.getSimpleName() + "-boss").build();
    private final ExecutorService computerService = Executors.newSingleThreadExecutor(threadFactoryBoss);
    static AtomicBoolean isCancelled = new AtomicBoolean(false);

    static {
        // todo: fix FireflyGraphFilterStrategy and remove GraphFilterStrategy
        TraversalStrategies.GlobalCache.registerStrategies(DistributedGraphComputer.class,
                TraversalStrategies.GlobalCache.getStrategies(GraphComputer.class).clone()
                        .removeStrategies(MessagePassingReductionStrategy.class)
                        .addStrategies(FireflyComputerVerificationStrategy.instance(),
                                SparkOptimizationStrategy.instance()));
    }

    public DistributedGraphComputer(final FireflyGraph graph, final Object sparkSession) {
        isCancelled.set(false);
        this.graph = graph;
        this.graph.getServiceRegistry().registerService(new JobCancellationService());
        this.graph.getServiceRegistry().registerService(new JobListService());
        this.graph.getServiceRegistry().registerService(new JobClearService());

        if (sparkSession != null) {
            this.spark = (SparkSession) sparkSession;
            workers = spark.sparkContext().getExecutorMemoryStatus().size();
        } else {
            final int processors = Runtime.getRuntime().availableProcessors();
            workers = Math.max(1, processors - 1);
            this.spark = buildSparkSession(workers);
        }
    }

    private DistributedConfigHelper generateConfigHelper() {
        // Get config from Firefly.
        final Map<String, Object> config = new HashMap<>();
        final Iterator<String> keys = graph.configuration().getKeys();
        while (keys.hasNext()) {
            final String key = keys.next();
            config.put(key, graph.configuration().getString(key));
        }

        // Get config from traversal.
        final Map<String, Object> traversalOptions = new HashMap<>();
        this.vertexProgram.getTraversal().get().getStrategies().
                getStrategy(OptionsStrategy.class).ifPresent(
                        optionsStrategy -> traversalOptions.putAll(optionsStrategy.getOptions()));

        return new DistributedConfigHelper(config, traversalOptions);
    }

    private static SparkSession buildSparkSession(final int workers) {
        // TODO: Remove null support and replace commandline with configs or something.
        final SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");

        final Runtime runtime = Runtime.getRuntime();
        final long maxMemoryGb = runtime.maxMemory() / (1024 * 1024 * 1024);
        // Leave room for master + some buffer.
        final long workerMemoryGb = Math.max(1, maxMemoryGb / (workers + 1) - 1);

        System.out.println("Creating spark session with " + workers + " workers and " + workerMemoryGb + "GB memory each.");

        conf.setAppName("aerospike-graph-olap")
                .set("spark.driver.allowMultipleContexts", "false")
                .set("spark.ui.enabled", "true")
                .set("spark.executor.memory", workerMemoryGb + "g")
                .set("spark.executor.cores", "1")
                .set("spark.task.cpus", "1")
                .set("spark.executor.instances", String.valueOf(workers))
                .set("spark.speculation", "false")
                .set("spark.scheduler.revive.interval", "500ms")
                .set("spark.dynamicAllocation.enabled", "true")
                .set("spark.dynamicAllocation.minExecutors", String.valueOf(workers))
                .set("spark.dynamicAllocation.maxExecutors", String.valueOf(workers))
                .set("spark.dynamicAllocation.initialExecutors", String.valueOf(workers))
                .set("spark.scheduler.minRegisteredResourcesRatio", "1.0")
                .set("mapreduce.fileoutputcommitter.algorithm.version", "2")
                .set("spark.executor.extraJavaOptions", "-Dlog4j.logger.org.apache.spark.serializer=DEBUG -Dlog4j.logger.org.apache.spark.util.ClosureCleaner=DEBUG");

        final SparkSession.Builder builder = SparkSession.builder().config(conf);
        builder.config("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .config("google.cloud.auth.service.account.enable", true);

        return builder.getOrCreate();
    }

    ////
    // Boilerplate.
    ////

    @Override
    public GraphComputer result(final ResultGraph resultGraph) {
        this.resultGraph = resultGraph;
        return this;
    }

    @Override
    public GraphComputer persist(final Persist persist) {
        this.persist = persist;
        return this;
    }

    @Override
    public GraphComputer program(final VertexProgram vertexProgram) {
        this.vertexProgram = createVertexProgram(vertexProgram, graph);

        return this;
    }

    @Override
    public GraphComputer mapReduce(final MapReduce mapReduce) {
        this.mapReducers.add(mapReduce);
        return this;
    }

    @Override
    public GraphComputer workers(final int workers) {
        this.workers = workers;
        return this;
    }

    @Override
    public GraphComputer vertices(final Traversal<Vertex, Vertex> vertexFilter) {
        this.graphFilter.setVertexFilter(vertexFilter);
        return this;
    }


    @Override
    public GraphComputer edges(final Traversal<Vertex, Edge> edgeFilter) {
        this.graphFilter.setEdgeFilter(edgeFilter);
        return this;
    }

    @Override
    public GraphComputer vertexProperties(Traversal<Vertex, ? extends Property<?>> vertexPropertyFilter) {
        this.graphFilter.setVertexPropertyFilter(vertexPropertyFilter);
        return this;
    }

    @Override
    public Features features() {
        return new Features() {
            @Override
            public boolean supportsResultGraphPersistCombination(final ResultGraph resultGraph, final Persist persist) {
                return persist == Persist.NOTHING || resultGraph == ResultGraph.ORIGINAL;
            }

            @Override
            public int getMaxWorkers() {
                return Integer.MAX_VALUE;
            }

            @Override
            public boolean supportsVertexAddition() {
                return false;
            }

            @Override
            public boolean supportsVertexRemoval() {
                return false;
            }

            @Override
            public boolean supportsVertexPropertyRemoval() {
                return false;
            }

            @Override
            public boolean supportsEdgeAddition() {
                return false;
            }

            @Override
            public boolean supportsEdgeRemoval() {
                return false;
            }

            @Override
            public boolean supportsEdgePropertyAddition() {
                return false;
            }

            @Override
            public boolean supportsEdgePropertyRemoval() {
                return false;
            }
        };
    }

    @Override
    public Future<ComputerResult> submit() {
        // TODO: Might be able to mess w/ this later.
        LOGGER.info("GRAPH COMPUTER FILTER STRATEGY CONFIGURATION:\n" +
                        "\tVertexProgram to execute: {}\n" +
                        "\tNumber of workers available: {}\n" +
                        "\tGraphComputer strategies applied: {}\n" +
                        "\tGraph filters computed:\n" +
                        "\t\tvertices: {}\n" +
                        "\t\tedges: {}",
                null == this.vertexProgram ? "N/A" : this.vertexProgram.toString(),
                this.workers,
                TraversalStrategies.GlobalCache.getStrategies(DistributedGraphComputer.class).toList().toString(),
                this.graphFilter.getVertexFilter(),
                this.graphFilter.getEdgeFilter());

        // A graph computer can only be executed once.
        if (this.executed) {
            throw Exceptions.computerHasAlreadyBeenSubmittedAVertexProgram();
        }
        this.executed = true;

        // It is not possible execute a computer if it has no vertex program nor MapReducers.
        if (null == this.vertexProgram && this.mapReducers.isEmpty())
            throw GraphComputer.Exceptions.computerHasNoVertexProgramNorMapReducers();

        // It is possible to run MapReducers without a vertex program.
        if (null != this.vertexProgram) {
            GraphComputerHelper.validateProgramOnComputer(this, this.vertexProgram);
            this.mapReducers.addAll(this.vertexProgram.getMapReducers());
        }

        // Get the result graph and persist state to use for the computation.
        this.resultGraph = GraphComputerHelper.getResultGraphState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.resultGraph));
        this.persist = GraphComputerHelper.getPersistState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.persist));

        // TODO: Maybe smarter check.
        // Ensure requested workers are not larger than supported workers.
        if (workers == -1)
            this.workers = spark.sparkContext().getExecutorMemoryStatus().size();
        System.out.println("Workers: " + this.workers);
        System.out.println("Memory: " + spark.sparkContext().getExecutorMemoryStatus());
        if (this.workers > this.features().getMaxWorkers())
            throw GraphComputer.Exceptions.computerRequiresMoreWorkersThanSupported(this.workers, this.features().getMaxWorkers());

        this.resultGraph = GraphComputerHelper.getResultGraphState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.resultGraph));
        this.persist = GraphComputerHelper.getPersistState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.persist));

        // allow cancellation
        final Future<ComputerResult> result = new DistributedFuture(computerService.submit(this::submitJob));
        this.computerService.shutdown();
        return result;
    }

    class DistributedFuture implements Future<ComputerResult> {
        final Future<ComputerResult> future;

        DistributedFuture(final Future<ComputerResult> future) {
            this.future = future;
        }

        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            isCancelled.set(true);
            boolean cancel = future.cancel(mayInterruptIfRunning);
            spark.sparkContext().cancelAllJobs();
            return cancel;
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public ComputerResult get() throws InterruptedException, ExecutionException {
            return future.get();
        }

        @Override
        public ComputerResult get(final long timeout, @NotNull final TimeUnit unit)
                throws ExecutionException, InterruptedException, TimeoutException {
            return future.get(timeout, unit);
        }
    }

    ////
    // Some hardcore stuff.
    ////
    private ComputerResult submitJob() {
        final DistributedAerospikeConnection db = new DistributedAerospikeConnection(graph.getBaseGraph(), 0, 0);
        final String jobId = UUID.randomUUID().toString();

        try {
            final DistributedConfigHelper configHelper = generateConfigHelper();
            System.out.println("Configuration: " + Arrays.toString(spark.sparkContext().getConf().getAll()));

            final PureTraversal<?, ?> traversal = vertexProgram.getTraversal().clone();

            // TODO Configurable page size w/ ConfigurationHelper.Keys.PAGINATION_PAGE_SIZE
            // TODO: Ultimately reworking this logic so that we can distribute the partition to the spark workers to run a sindex again
            // would probably be the best way to go

            // TODO https://aerospike.com/docs/server/reference/configuration#namespace__background-query-max-rps - We might want to jack this up for OLAP.
            // TODO: Partitions pull data directly.

            // Get traversal and apply strategies.
            final Traversal pureTraversal = traversal.getPure().asAdmin().clone();
            pureTraversal.asAdmin().applyStrategies();

            final Step<?, ?> firstStep = pureTraversal.asAdmin().getStartStep();
            if (firstStep instanceof CallStep) {
                return DistributedMasterExecutor.execute(this.vertexProgram, this.graph, this.spark, db, jobId);
            }

            if (!(firstStep instanceof FireflyGraphStep)) {
                throw new RuntimeException("OLAP only supports starting on GraphStep, please contact support.");
            }
            final FireflyGraphStep graphStep = (FireflyGraphStep) firstStep;
            if (graphStep.returnsEdge()) {
                LOGGER.warn("Edges do not support secondary indexes, you may experience poor performance.");
            }
            System.out.println("===== " + graphStep + " " + graphStep.returnsVertex() + " ===== " + pureTraversal.asAdmin().getSteps());

            // truncate only if valid query
            db.truncateOlapSet();

            final Codec codec = vertexProgram.getCodec();

            // Create basic schema.
            final StructType schema = codec.getSchema();

            // Create necessary things for execution (Memory, ResultGraph, Config, etc.)
            this.resultGraph = GraphComputerHelper.getResultGraphState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.resultGraph));
            final DistributedMemory memory = new DistributedMemory(this.vertexProgram, this.mapReducers, new JavaSparkContext(spark.sparkContext()));
            memory.setGraph(graph);
            final DistributedConfiguration vertexProgramConfiguration = new DistributedConfiguration();
            this.vertexProgram.storeState(vertexProgramConfiguration);
            this.vertexProgram.setup(memory);

            db.writeJob(new Job(jobId, traversal.toString()));

            // Broadcast spark context.
            memory.broadcastMemory(new JavaSparkContext(spark.sparkContext()));
            memory.incrIteration();

            final int maxParallelSindexes = AerospikeConnection.InfoOps.getMaxParallelSindexes(this.graph.getBaseGraph(), this.graph.getBaseGraph().namespace) - 4; // Leave some room.

            Dataset<Row> df = null;
            Dataset<Row> results = null;
            int iterationCount = 0;
            do {
                iterationCount++;
                final Job job = db.setJobIteration(jobId, memory.getIteration());
                if (job == null || job.getState() == Job.State.CANCELLED) {
                   throw new TraversalInterruptedException();
                }

                // Set inExecute to true, execute the vertex program, and set inExecute to false.
                memory.setInExecute(true);
                final Dataset<Row> nextDf = DistributedWorkerExecutor.execute(
                        spark,
                        graph,
                        configHelper,
                        graphStep.getHasContainers(),
                        graphStep.getIds(),
                        traversal.get(),
                        maxParallelSindexes,
                        iterationCount == 1,
                        df,
                        memory,
                        vertexProgramConfiguration,
                        schema,
                        Math.max(1, workers - 1));
                // add iteration column if AlgorithmProgram
                df = magicSwap(
                        this.vertexProgram instanceof AlgorithmProgram ? nextDf.withColumn(Codec.ITERATION, lit(memory.getIteration())) : nextDf,
                        true,
                        configHelper.getStorageLevel());

                if (configHelper.isDebugDf()) {
                    df.show();
                    System.out.println(memory.getIteration() + " ==================> TOTAL COUNT: " + df.count());
                }

                memory.setInExecute(false);

                // we can use native spark sort and continue execution locally, so data in dataframe become obsolete.
                boolean skipResults = false;
                final Pair<Boolean, Dataset<Row>> nativeResult = DistributedWorkerExecutor.executeNative(traversal.get(), df, memory, codec);
                if (nativeResult.getValue0()) {
                    if (nativeResult.getValue1() == null) {
                        skipResults = true;
                    }
                    df = magicSwap(nativeResult.getValue1());
                }

                if (!skipResults) {
                    final Dataset<Row> resultsTemp = magicSwap(
                            df.filter(org.apache.spark.sql.functions.col(HALTED_COL).equalTo(true)));

                    // Filter out halted vertices.
                    if (results == null) {
                        results = resultsTemp;
                    } else {
                        results = magicSwap(results.union(resultsTemp));
                    }
                }

                // Filter out vertices that are not halted.
                df = magicSwap(df.filter(org.apache.spark.sql.functions.col(HALTED_COL).equalTo(false)));

                // TODO: Ultimately probably don't want to do isEmpty() check here b/c we could have a query that pulls more data from graph later and
                // we could screw it up.
                if (this.vertexProgram.terminate(memory) || df.limit(1).isEmpty()) {
                    // Need to be very careful with this stuff. Spark is LAZY. It doesn't execute unless forced, so if we incr at the wrong time there is problems.
                    memory.incrIteration();

                    if (!(this.vertexProgram instanceof TraversalProgram)) {
                        results = df;
                    }

                    break;
                } else {
                    memory.incrIteration();
                }
            } while (df.count() != 0);

            // try to execute some steps natively on spark dataframe
            final Pair<Boolean, Dataset<Row>> postProcessResult = vertexProgram.postProcessResults(results, memory);
            if (postProcessResult.getValue0()) {
                results = magicSwap(postProcessResult.getValue1());
            }

            final TraverserSet traversers = new TraverserSet();
            try {
                TraverserSet memoryTraversers = memory.get(HALTED_TRAVERSERS);
                traversers.addAll(memoryTraversers);
            } catch (IllegalArgumentException e) {
                // No data in memory.
            }
            System.out.println("Memory traversers: " + traversers.size() + "; bulkSize: " + traversers.bulkSize());

            if (results != null) {
                // Collect results.
                final List<Row> rows = results.collectAsList();

                // Create traversers.
                rows.stream().forEach(row -> traversers.add(codec.decode(row).asAdmin()));
                System.out.println("Results: " + rows.size());
            }

            if (configHelper.isDebugDf()) {
                System.out.println("Traversers: " + traversers);
            }
            AttachmentHelper.makeDetachedElements(graph, traversers);
            //  remove temporary compute properties and execute native steps if necessary
            vertexProgram.postProcessResults(traversers, memory);

            // Set all traversers as halted and complete memory.
            memory.set(HALTED_TRAVERSERS, traversers);
            memory.complete();

            db.finishJob(jobId, traversers.size());

            // Generate view and process result graph.
            final LocalGraphComputerView view = FireflyHelper.createGraphComputerView(this.graph,
                    this.graphFilter,
                    null != this.vertexProgram ? this.vertexProgram.getVertexComputeKeys() : Collections.emptySet());
            final Graph resultGraph = view.processResultGraphPersist(this.resultGraph, this.persist);

            // Send result and memory to computer result.
            return new DefaultComputerResult(resultGraph, memory);
        } catch (final Exception e) {
            db.setJobError(jobId, e.getMessage());

            if (e instanceof TraversalInterruptedException || e instanceof InterruptedException) {
                spark.sparkContext().cancelAllJobs();
                LOGGER.error("Query timeout or cancellation is called. Consider raising the evaluation timeout.", e);
                throw e;
            }
            // Maybe remove this for L2.
            //spark.close();
            //spark = null;

            LOGGER.error("A global error occurred. Shutting down {}: {}", this, e.getMessage(), e);
            e.printStackTrace();
            throw new RuntimeException("Global error '" + e.getMessage() + "' occurred during OLAP traversal.", e);
        } finally {
            // memory.complete ?
            FireflyHelper.dropGraphComputerView(this.graph);
            db.truncateOlapSet();
        }
    }

    public static Dataset<Row> magicSwap(final Dataset<Row> transform) {
        // Persist false so storage level is not used.
        return magicSwap(transform, false, null);
    }

    public static Dataset<Row> magicSwap(final Dataset<Row> transform,
                                         final boolean persist,
                                         final StorageLevel storageLevel) {
        if (isCancelled.get()) {
            throw new TraversalInterruptedException();
        }
        final Dataset<Row> output = persist ? transform.persist(storageLevel) : transform;
        try {
            output.count();
        } catch (Exception e) {
            // Do nothing.
            // Failed to count in 1 second. Who cares.
        }
        if (isCancelled.get()) {
            throw new TraversalInterruptedException();
        }
        return output;
    }
}
