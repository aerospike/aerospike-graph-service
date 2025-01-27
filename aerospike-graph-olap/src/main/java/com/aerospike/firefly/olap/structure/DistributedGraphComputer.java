package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.io.aerospike.query.paged.PartitionIterator;
import com.aerospike.firefly.olap.config.DistributedConfigHelper;
import com.aerospike.firefly.olap.config.DistributedConfiguration;
import com.aerospike.firefly.process.computer.local.LocalGraphComputerView;
import com.aerospike.firefly.process.computer.util.ComputerHelper;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.FireflyHelper;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_NL_O_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_NL_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_NL_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_NL_O_OB_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_NL_O_OB_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.NL_O_OB_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.O_OB_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.DefaultTraverserGeneratorFactory;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.IndexedTraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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

import static com.aerospike.firefly.olap.structure.DistributedElement.HALTED_COL;
import static com.aerospike.firefly.olap.structure.DistributedElement.ID_COL;
import static com.aerospike.firefly.olap.structure.DistributedElement.ID_TYPEHINT_COL;
import static com.aerospike.firefly.olap.structure.DistributedElement.IN_COL;
import static com.aerospike.firefly.olap.structure.DistributedElement.LABEL_COL;
import static com.aerospike.firefly.olap.structure.DistributedElement.OUT_COL;
import static com.aerospike.firefly.olap.structure.DistributedElement.PROPERTIES_COL;
import static com.aerospike.firefly.olap.structure.DistributedElement.REF_COL;
import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.ACTIVE_TRAVERSERS;
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
    private final DistributedConfigHelper configHelper;

    public DistributedGraphComputer(final FireflyGraph graph) {
        this.graph = graph;
        final Map<String, Object> config = new HashMap<>();
        final Iterator<String> keys = graph.configuration().getKeys();
        while (keys.hasNext()) {
            final String key = keys.next();
            config.put(key, graph.configuration().getString(key));
        }
        configHelper = new DistributedConfigHelper(config);

        this.spark = buildSparkSession();
    }

    private static SparkSession buildSparkSession() {
        // TODO: Remove null support and replace commandline with configs or something.
        final SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");

        conf.setAppName("aerospike-graph-olap")
                .set("spark.driver.allowMultipleContexts", "false")
                .set("spark.ui.enabled", "true")
                .set("mapreduce.fileoutputcommitter.algorithm.version", "2");

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
        this.vertexProgram = vertexProgram;
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
        if (this.workers > this.features().getMaxWorkers())
            throw GraphComputer.Exceptions.computerRequiresMoreWorkersThanSupported(this.workers, this.features().getMaxWorkers());

        // Initialize the memory.
        // this.memory = new LocalMemory(this.vertexProgram, this.mapReducers);
        this.resultGraph = GraphComputerHelper.getResultGraphState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.resultGraph));
        this.persist = GraphComputerHelper.getPersistState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.persist));
        try {
            final PureTraversal<?, ?> traversal = ((TraversalVertexProgram) vertexProgram).getTraversal().clone();
            final List<HasContainer> initialHasContainers = ComputerHelper.getInitialHasContainers(traversal.get());

            // TODO Configurable page size w/ ConfigurationHelper.Keys.PAGINATION_PAGE_SIZE
            // TODO: Ultimately reworking this logic so that we can distribute the partition to the spark workers to run a sindex again
            // would probably be the best way to go
            final PartitionIterator.Builder builder = PartitionIterator.
                    build(this.graph).
                    containers(initialHasContainers).
                    partitionSize(10000);
            final List<Row> vertices = new ArrayList<>();

            // Get traversal and apply strategies.
            final Traversal pureTraversal = traversal.getPure().asAdmin().clone();
            pureTraversal.asAdmin().applyStrategies();

            // Get TraverseRequirements and TraverserGenerator, this will be useful later when we go to traverser based approach.
            final Set<TraverserRequirement> traverserRequirements = pureTraversal.asAdmin().getTraverserRequirements();
            final TraverserGenerator traverserGenerator = DefaultTraverserGeneratorFactory.instance().getTraverserGenerator(traverserRequirements);

            // If we give partition info to workers, this can go away.
            try (final PartitionIterator partitionIterator = builder.create()) {
                while (partitionIterator.hasNext()) {
                    final Optional<CloseableIterator<FireflyVertex>> optional = partitionIterator.next();
                    if (optional.isEmpty()) {
                        break;
                    } else {
                        try (final CloseableIterator<FireflyVertex> vertexIterator = optional.get()) {
                            while (vertexIterator.hasNext()) {
                                final FireflyVertex vertex = vertexIterator.next();
                                vertices.add(createRow(vertex, traverserGenerator, null));
                            }
                        }
                    }
                }
            }

            // Create basic schema.
            final StructType schema = new StructType()
                    .add(ID_COL, DataTypes.StringType, false)
                    .add(ID_TYPEHINT_COL, DataTypes.IntegerType, false)
                    .add(LABEL_COL, DataTypes.StringType, true)
                    .add(PROPERTIES_COL, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true))
                    .add(IN_COL, DataTypes.createMapType(DataTypes.StringType,
                            DataTypes.createArrayType(DataTypes.BinaryType)), true)
                    .add(OUT_COL, DataTypes.createMapType(DataTypes.StringType,
                            DataTypes.createArrayType(DataTypes.BinaryType)), true)
                    .add(HALTED_COL, DataTypes.BooleanType, false)
                    .add(REF_COL, DataTypes.BooleanType, false);

            // Generate Dataset.
            Dataset<Row> df = spark.createDataFrame(vertices, schema);

            // Create necessary things for execution (Memory, ResultGraph, Config, etc.)
            this.resultGraph = GraphComputerHelper.getResultGraphState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.resultGraph));
            final DistributedMemory memory = new DistributedMemory(this.vertexProgram, this.mapReducers, new JavaSparkContext(spark.sparkContext()));
            final DistributedConfiguration vertexProgramConfiguration = new DistributedConfiguration();
            this.vertexProgram.storeState(vertexProgramConfiguration);
            this.vertexProgram.setup(memory);

            // Broadcast spark context.
            memory.broadcastMemory(new JavaSparkContext(spark.sparkContext()));

            // Set results to initially empty.
            Dataset<Row> results = spark.emptyDataset(RowEncoder.apply(schema));
            while (true) {
                if (Thread.interrupted()) {
                    // If query is cancelled, cancel all spark jobs and throw an exception.
                    spark.sparkContext().cancelAllJobs();
                    throw new TraversalInterruptedException();
                }

                // Set inExecute to true, execute the vertex program, and set inExecute to false.
                memory.setInExecute(true);
                df = DistributedExecutor.execute(df, memory, configHelper, vertexProgramConfiguration, pureTraversal, schema);
                memory.setInExecute(false);

                // Filter out halted vertices.
                Dataset<Row> halted = df.filter(org.apache.spark.sql.functions.col(HALTED_COL).equalTo(true));

                // Filter out vertices that are not halted.
                df = df.filter(org.apache.spark.sql.functions.col(HALTED_COL).equalTo(false));

                // Create new dataframe with schema (without reapplying schema there are issues).
                df = spark.createDataFrame(df.rdd(), schema);
                if (!halted.isEmpty()) {
                    // Apply schema to halted vertices and union, then apply schema to results.
                    halted = spark.createDataFrame(halted.rdd(), schema);
                    results = results.union(halted);
                    results = spark.createDataFrame(results.rdd(), schema);
                }

                // Persist results.
                results.persist();
                // This is a workaround for the fact spark is lazy. This forces evaluation, without it none of the
                // above actions would have actually executed yet and the below vertexProgram.terminate check will be
                // erroneous.
                results.count();

                memory.set("gremlin.traversalVertexProgram.voteToHalt", true);
                memory.set(ACTIVE_TRAVERSERS, new IndexedTraverserSet.VertexIndexedTraverserSet());
                if (this.vertexProgram.terminate(memory)) {
                    // Need to be very careful with this stuff. Spark is LAZY. It doesn't execute unless forced, so if we incr at the wrong time there is problems.
                    memory.incrIteration();
                    break;
                } else {
                    memory.incrIteration();
                }
                df.show(false);
            }

            // Collect results.
            final List<Row> rows = results.collectAsList();

            // Create traversers.
            final TraverserSet traversers = new TraverserSet();
            rows.stream().forEach(row -> {
                final DistributedVertex vertex = new DistributedVertex(row, graph);
                traversers.add(new B_O_Traverser<>(vertex, 1L));
            });

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
            LOGGER.error("A global error occurred. Shutting down {}: {}", this, e.getMessage(), e);
            return new CompletableFuture<>();
        }
    }

    public static DistributedElement.ID_TYPE getIdType(final Object id) {
        if (id instanceof Long) {
            return DistributedElement.ID_TYPE.LONG;
        } else if (id instanceof Integer) {
            return DistributedElement.ID_TYPE.INTEGER;
        } else if (id instanceof  String) {
            return DistributedElement.ID_TYPE.STRING;
        } else {
            // TODO.
            throw new IllegalArgumentException("Only Long string and integer types can be serialized at this time.");
        }
    }

    public static Row createRow(final FireflyVertex vertex,
                                 final TraverserGenerator generator,
                                 final GraphStep<Vertex, Vertex> step) {
        //final TraverserGenerator generator = traversal.asAdmin().getTraverserGenerator();
        // TODO: Make a better format, stringifying these is going to be slow.

        return RowFactory.create(
                vertex.id().toString(),
                getIdType(vertex.id()).ordinal(),
                vertex.label(),
                vertex.getRawVertexStringPropertyValues(),
                vertex.getCachedIdMap(Direction.IN),
                vertex.getCachedIdMap(Direction.OUT),
                false,
                false);
    }


    public TraverserGenerator getTraverserGenerator(final Set<TraverserRequirement> requirements) {
        if (requirements.contains(TraverserRequirement.ONE_BULK)) {
            if (O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return O_OB_S_SE_SL_TraverserGenerator.instance();

            if (NL_O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return NL_O_OB_S_SE_SL_TraverserGenerator.instance();

            if (LP_O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return LP_O_OB_S_SE_SL_TraverserGenerator.instance();

            if (LP_NL_O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return LP_NL_O_OB_S_SE_SL_TraverserGenerator.instance();

            if (LP_O_OB_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return LP_O_OB_P_S_SE_SL_TraverserGenerator.instance();

            if (LP_NL_O_OB_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return LP_NL_O_OB_P_S_SE_SL_TraverserGenerator.instance();
        } else {
            if (B_O_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_O_TraverserGenerator.instance();

            if (B_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_O_S_SE_SL_TraverserGenerator.instance();

            if (B_NL_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_NL_O_S_SE_SL_TraverserGenerator.instance();

            if (B_LP_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_LP_O_S_SE_SL_TraverserGenerator.instance();

            if (B_LP_NL_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_LP_NL_O_S_SE_SL_TraverserGenerator.instance();

            if (B_LP_O_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_LP_O_P_S_SE_SL_TraverserGenerator.instance();

            if (B_LP_NL_O_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return B_LP_NL_O_P_S_SE_SL_TraverserGenerator.instance();
        }

        throw new IllegalStateException("The provided traverser generator factory does not support the requirements of the traversal: " + this.getClass().getCanonicalName() + requirements);
    }
}
