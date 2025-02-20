package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.config.DistributedConfigHelper;
import com.aerospike.firefly.olap.config.DistributedConfiguration;
import com.aerospike.firefly.olap.helper.AttachmentHelper;
import com.aerospike.firefly.olap.process.TraversalProgram;
import com.aerospike.firefly.process.computer.local.LocalGraphComputerView;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.FireflyHelper;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.DefaultTraverserGeneratorFactory;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static com.aerospike.firefly.olap.codec.RowCodec.HALTED_COL;
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

    private VertexProgram<?> vertexProgram;
    private final FireflyGraph graph;
    private final Set<MapReduce> mapReducers = new HashSet<>();
    private int workers;
    private final GraphFilter graphFilter = new GraphFilter();
    private boolean executed = false;

    public DistributedGraphComputer(final FireflyGraph graph, final Object sparkSession) {
        this.graph = graph;
        if (sparkSession != null) {
            this.spark = (SparkSession) sparkSession;
        } else {
            if (this.spark == null)
                this.spark = buildSparkSession();
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
        config.put(ConfigurationHelper.Keys.OLAP_ENABLED.toLowerCase(), true);
        config.put(ConfigurationHelper.Keys.AUTO_PRE_HEAT.toLowerCase(), "false");
        config.put(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");

        // Get config from traversal.
        final Map<String, Object> traversalOptions = new HashMap<>();
        ((TraversalProgram) this.vertexProgram).getTraversal().get().getStrategies().
                getStrategy(OptionsStrategy.class).ifPresent(
                        optionsStrategy -> traversalOptions.putAll(optionsStrategy.getOptions()));

        return new DistributedConfigHelper(config, traversalOptions);
    }

    private static SparkSession buildSparkSession() {
        // TODO: Remove null support and replace commandline with configs or something.
        System.out.println("Building spark session in DistributedGraphComputer.");
        final SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");

        conf.setAppName("aerospike-graph-olap")
                .set("spark.driver.allowMultipleContexts", "false")
                .set("spark.ui.enabled", "true")
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
        this.vertexProgram = new TraversalProgram((TraversalVertexProgram) vertexProgram);
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

    ////
    // Some hardcore stuff.
    ////

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
        this.workers = spark.sparkContext().getExecutorMemoryStatus().size();
        System.out.println("Workers: " + this.workers);
        System.out.println("Memory: " + spark.sparkContext().getExecutorMemoryStatus());
        if (this.workers > this.features().getMaxWorkers())
            throw GraphComputer.Exceptions.computerRequiresMoreWorkersThanSupported(this.workers, this.features().getMaxWorkers());

        // Initialize the memory.
        // this.memory = new LocalMemory(this.vertexProgram, this.mapReducers);
        this.resultGraph = GraphComputerHelper.getResultGraphState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.resultGraph));
        this.persist = GraphComputerHelper.getPersistState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.persist));
        int i = 0;
        final DistributedConfigHelper configHelper = generateConfigHelper();
        System.out.println("Configuration: "  + Arrays.toString(spark.sparkContext().getConf().getAll()));
        try {
            final PureTraversal<?, ?> traversal = ((TraversalProgram) vertexProgram).getTraversal().clone();

            // TODO Configurable page size w/ ConfigurationHelper.Keys.PAGINATION_PAGE_SIZE
            // TODO: Ultimately reworking this logic so that we can distribute the partition to the spark workers to run a sindex again
            // would probably be the best way to go

            // TODO https://aerospike.com/docs/server/reference/configuration#namespace__background-query-max-rps - We might want to jack this up for OLAP.
            // TODO: Partitions pull data directly.


            // Get traversal and apply strategies.
            final Traversal pureTraversal = traversal.getPure().asAdmin().clone();
            pureTraversal.asAdmin().applyStrategies();

            // Get TraverseRequirements and TraverserGenerator, this will be useful later when we go to traverser based approach.
            final Set<TraverserRequirement> traverserRequirements = pureTraversal.asAdmin().getTraverserRequirements();
            final TraverserGenerator traverserGenerator = DefaultTraverserGeneratorFactory.instance().getTraverserGenerator(traverserRequirements);

            final Step<?, ?> firstStep = pureTraversal.asAdmin().getStartStep();
            if (!(firstStep instanceof FireflyGraphStep)) {
                throw new RuntimeException("OLAP only supports starting on GraphStep, please contact support.");
            }
            final FireflyGraphStep graphStep = (FireflyGraphStep) firstStep;
            if (graphStep.returnsEdge()) {
                LOGGER.warn("Edges do not support secondary indexes, you may experience poor performance.");
            }
            System.out.println("===== " + graphStep + " " + graphStep.returnsVertex() + " ===== " + pureTraversal.asAdmin().getSteps());

            final Codec codec = new Codec(pureTraversal);

            // Create basic schema.
            final StructType schema = codec.getSchema();

            // Create necessary things for execution (Memory, ResultGraph, Config, etc.)
            this.resultGraph = GraphComputerHelper.getResultGraphState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.resultGraph));
            final DistributedMemory memory = new DistributedMemory(this.vertexProgram, this.mapReducers, new JavaSparkContext(spark.sparkContext()));
            final DistributedConfiguration vertexProgramConfiguration = new DistributedConfiguration();
            this.vertexProgram.storeState(vertexProgramConfiguration);
            this.vertexProgram.setup(memory);

            // Broadcast spark context.
            memory.broadcastMemory(new JavaSparkContext(spark.sparkContext()));

            memory.incrIteration();
            final int maxParallelSindexes = AerospikeConnection.InfoOps.getMaxParallelSindexes(this.graph.getBaseGraph(), this.graph.getBaseGraph().namespace) - 4; // Leave some room.

            Dataset<Row> df = null;
            Dataset<Row> results = null;
            int iterationCount = 0;
            do {
                iterationCount++;
                if (Thread.interrupted()) {
                    // If query is cancelled, cancel all spark jobs and throw an exception.
                    spark.sparkContext().cancelAllJobs();
                    throw new TraversalInterruptedException();
                }

                // Set inExecute to true, execute the vertex program, and set inExecute to false.
                memory.setInExecute(true);
                df = magicSwap(DistributedWorkerExecutor.execute(
                        spark,
                        graph,
                        configHelper,
                        graphStep.getHasContainers(),
                        graphStep.getIds(),
                        traversal.get(),
                        schema,
                        maxParallelSindexes,
                        iterationCount == 1,
                        df,
                        memory,
                        vertexProgramConfiguration,
                        schema,
                        Math.max(1, workers - 1)));

                if (configHelper.isDebugDf()) {
                    df.show();
                }
                System.out.println("==================> TOTAL COUNT: " + df.count());
                memory.setInExecute(false);

                final Dataset<Row> resultsTemp = magicSwap(
                        df.filter(org.apache.spark.sql.functions.col(HALTED_COL).equalTo(true)));

                // Filter out halted vertices.
                if (results == null) {
                    results = resultsTemp;
                } else {
                    results = magicSwap(results.union(resultsTemp));

                }

                //if (LOGGER.isDebugEnabled())
                //System.out.println("====================== Results ======================");
                //results.show();

                // Filter out vertices that are not halted.
                df = magicSwap(df.filter(org.apache.spark.sql.functions.col(HALTED_COL).equalTo(false)));

                //System.out.println("====================== DF2 ======================");
                //df.show();

                // TODO: Ultimately probably don't want to do isEmpty() check here b/c we could have a query that pulls more data from graph later and
                // we could screw it up.
                System.out.println("Loop " + i++);
                if (this.vertexProgram.terminate(memory) || df.limit(1).isEmpty()) {
                    // Need to be very careful with this stuff. Spark is LAZY. It doesn't execute unless forced, so if we incr at the wrong time there is problems.
                    memory.incrIteration();
                    break;
                } else {
                    memory.incrIteration();
                }
            } while (df.count() != 0);

            final TraverserSet traversers = new TraverserSet();
            try {
                TraverserSet memoryTraversers = memory.get(HALTED_TRAVERSERS);
                traversers.addAll(memoryTraversers);
            } catch (IllegalArgumentException e) {
                // No data in memory.
            }
            System.out.println("Memory traversers: " + traversers.size());

            // Collect results.
            final List<Row> rows = results.collectAsList();

            // Create traversers.
            final TraversalMatrix traversalMatrix = new TraversalMatrix<>(pureTraversal.asAdmin());
            rows.stream().forEach(row -> {
                traversers.add(codec.decode(row, traverserGenerator, traversalMatrix).asAdmin());
            });
            System.out.println("Results: " + rows.size());

            System.out.println("Traversers: " + traversers);
            AttachmentHelper.makeDetachedElements((FireflyGraph) traversalMatrix.getTraversal().getGraph().get(), traversers);

            // Set all traversers as halted and complete memory.
            memory.set(HALTED_TRAVERSERS, traversers);
            memory.complete();

            // Generate view and process result graph.
            final LocalGraphComputerView view = FireflyHelper.createGraphComputerView(this.graph,
                    this.graphFilter,
                    null != this.vertexProgram ? this.vertexProgram.getVertexComputeKeys() : Collections.emptySet());
            final Graph resultGraph = view.processResultGraphPersist(this.resultGraph, this.persist);

            // Send result and memory to computer result.
            return CompletableFuture.completedFuture(new DefaultComputerResult(resultGraph, memory));
        } catch (final Exception e) {
            // Maybe remove this for L2.
            //spark.close();
            //spark = null;

            LOGGER.error("A global error occurred. Shutting down {}: {}", this, e.getMessage(), e);
            e.printStackTrace();
            throw new RuntimeException("Global error '" + e.getMessage() + "' occurred during OLAP traversal.", e);
        } finally {
            // memory.complete ?
            FireflyHelper.dropGraphComputerView(this.graph);
        }
    }

    public static Dataset<Row> magicSwap(final Dataset<Row> transform) {
        final Dataset<Row> output = transform.persist(StorageLevel.MEMORY_AND_DISK());
        try {
            output.count();
        } catch (Exception e) {
            // Do nothing.
            // Failed to count in 1 second. Who cares.
        }
        return output;
    }
}
