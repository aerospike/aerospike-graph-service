package com.aerospike.firefly.olap;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.spark.sql.SparkSession;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class DistributedGraphComputer implements GraphComputer {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedGraphComputer.class);
    private ResultGraph resultGraph = null;
    private Persist persist = null;
    private SparkSession spark = null;

    private VertexProgram<?> vertexProgram;
    private final FireflyGraph graph;
    private LocalMemory memory;
    private final LocalMessageBoard messageBoard = new LocalMessageBoard();
    private final Set<MapReduce> mapReducers = new HashSet<>();
    private int workers;
    private final GraphFilter graphFilter = new GraphFilter();
    private boolean executed = false;

    DistributedGraphComputer(final FireflyGraph graph) {
        this.graph = graph;
        this.spark = buildSparkSession(null);
    }

    private static SparkSession buildSparkSession(final CommandLine cmd) {
        // TODO: Remove null support and replace commandline with configs or something.
        SparkConf conf = new SparkConf();
        if (cmd == null || cmd.hasOption(BulkLoaderConfigHelper.LOCAL_MODE)) {
            conf.setMaster("local[*]");
        }

        conf.setAppName("aerospike-graph-olap")
                .set("spark.driver.allowMultipleContexts", "false")
                .set("spark.ui.enabled", "true")
                .set("mapreduce.fileoutputcommitter.algorithm.version", "2");

        final SparkSession.Builder builder = SparkSession.builder().config(conf);
        builder.config("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .config("google.cloud.auth.service.account.enable", true);

        // Internal use configurations
        if (cmd != null && cmd.hasOption("s3e")) {
            builder.config("fs.s3a.endpoint", cmd.getOptionValue("s3e")).config("fs.s3a.connection.ssl.enabled", "false");
        }

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
        LOG.info("GRAPH COMPUTER FILTER STRATEGY CONFIGURATION:\n" +
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
        this.memory = new LocalMemory(this.vertexProgram, this.mapReducers);
        final Gson gson = new Gson();
        try {
            final PureTraversal<?, ?> traversal = ((TraversalVertexProgram) vertexProgram).getTraversal().clone();
            final List<HasContainer> initialHasContainers = ComputerHelper.getInitialHasContainers(traversal.get());

            // TODO Configurable page size w/ ConfigurationHelper.Keys.PAGINATION_PAGE_SIZE
            final PartitionIterator.Builder builder = PartitionIterator.build(this.graph).partitionSize(10000);
            // TODO: Try w/ resources.
            final List<Row> vertices = new ArrayList<>();
            try (PartitionIterator partitionIterator = builder.create()) {
                while (partitionIterator.hasNext()) {
                    final Optional<CloseableIterator<FireflyVertex>> optional = partitionIterator.next();
                    if (optional.isEmpty()) {
                        break;
                    } else {
                        try (final CloseableIterator<FireflyVertex> vertexIterator = optional.get()) {
                            while (vertexIterator.hasNext()) {
                                final FireflyVertex vertex = vertexIterator.next();
                                vertices.add(RowFactory.create(vertex.id(), vertex.label(), gson.toJson(vertex.getRawVertexPropertyValues())));
                            }
                        }
                    }
                }
            }

            StructType schema = new StructType()
                    .add("~id", DataTypes.StringType, false)
                    .add("~label", DataTypes.StringType, false)
                    .add("~properties", DataTypes.StringType, false);
            //.add("inEdgeIds", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true)
            //.add("outEdgeIds", DataTypes.createArrayType(DataTypes.StringType, DataTypes.StringType), true)

            final Dataset<Row> df = spark.createDataFrame(vertices, schema);
            df.show();
            return null;
        } catch (final Exception e) {
            LOG.error("A global error occurred. Shutting down {}: {}", this, e.getMessage());
            return new CompletableFuture<>();
        }
    }
}
