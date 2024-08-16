package com.aerospike.firefly.process.computer.local;

import com.aerospike.firefly.io.aerospike.query.paged.PartitionIterator;
import com.aerospike.firefly.process.traversal.step.computer.PrecomputableComputerStep;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGraphFilterStrategy;
import com.aerospike.firefly.process.traversal.strategy.verification.FireflyComputerVerificationStrategy;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.FireflyHelper;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.GraphFilterStrategy;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.IndexedTraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.HALTED_TRAVERSERS;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LocalGraphComputer implements GraphComputer {

    private static final Logger LOG = LoggerFactory.getLogger(LocalGraphComputer.class);
    private ResultGraph resultGraph = null;
    private Persist persist = null;

    private VertexProgram<?> vertexProgram;
    private final FireflyGraph graph;
    private LocalMemory memory;
    private final LocalMessageBoard messageBoard = new LocalMessageBoard();
    private boolean executed = false;
    private final Set<MapReduce> mapReducers = new HashSet<>();
    private int workers = Runtime.getRuntime().availableProcessors();
    private final GraphFilter graphFilter = new GraphFilter();

    private final int previousPartitionSize;

    private final ThreadFactory threadFactoryBoss = new BasicThreadFactory.Builder().namingPattern(LocalGraphComputer.class.getSimpleName() + "-boss").build();
    private final ExecutorService computerService = Executors.newSingleThreadExecutor(threadFactoryBoss);

    static {
        TraversalStrategies.GlobalCache.registerStrategies(LocalGraphComputer.class,
                TraversalStrategies.GlobalCache.getStrategies(GraphComputer.class).clone()
                        .removeStrategies(new Class[]{GraphFilterStrategy.class})
                        .addStrategies(FireflyGraphFilterStrategy.instance(), FireflyComputerVerificationStrategy.instance()));
    }

    public LocalGraphComputer(final FireflyGraph graph) {
        this.graph = graph;
        this.previousPartitionSize = ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_PAGE_SIZE, this.graph.configuration());
    }

    public GraphComputer partitionSize(final int partitionSize) {
        this.graph.configuration().setProperty(ConfigurationHelper.Keys.PAGINATION_PAGE_SIZE, partitionSize);
        return this;
    }

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
    public Future<ComputerResult> submit() {
        LOG.warn("GRAPH COMPUTER FILTER STRATEGY CONFIGURATION:\n" +
                        "\tVertexProgram to execute: {}\n" +
                        "\tNumber of workers available: {}\n" +
                        "\tGraphComputer strategies applied: {}\n" +
                        "\tGraph filters computed:\n" +
                        "\t\tvertices: {}\n" +
                        "\t\tedges: {}",
                null == this.vertexProgram ? "N/A" : this.vertexProgram.toString(),
                this.workers,
                TraversalStrategies.GlobalCache.getStrategies(LocalGraphComputer.class).toList().toString(),
                this.graphFilter.getVertexFilter(),
                this.graphFilter.getEdgeFilter());
        // a graph computer can only be executed once
        if (this.executed)
            throw Exceptions.computerHasAlreadyBeenSubmittedAVertexProgram();
        else
            this.executed = true;
        // it is not possible execute a computer if it has no vertex program nor mapreducers
        if (null == this.vertexProgram && this.mapReducers.isEmpty())
            throw GraphComputer.Exceptions.computerHasNoVertexProgramNorMapReducers();
        // it is possible to run mapreducers without a vertex program
        if (null != this.vertexProgram) {
            GraphComputerHelper.validateProgramOnComputer(this, this.vertexProgram);
            this.mapReducers.addAll(this.vertexProgram.getMapReducers());
        }
        // get the result graph and persist state to use for the computation
        this.resultGraph = GraphComputerHelper.getResultGraphState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.resultGraph));
        this.persist = GraphComputerHelper.getPersistState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.persist));
        // if (!this.features().supportsResultGraphPersistCombination(this.resultGraph, this.persist))
        // throw GraphComputer.Exceptions.resultGraphPersistCombinationNotSupported(this.resultGraph, this.persist);
        // ensure requested workers are not larger than supported workers
        if (this.workers > this.features().getMaxWorkers())
            throw GraphComputer.Exceptions.computerRequiresMoreWorkersThanSupported(this.workers, this.features().getMaxWorkers());
        // initialize the memory
        this.memory = new LocalMemory(this.vertexProgram, this.mapReducers);
        try {
            final Future<ComputerResult> result = computerService.submit(() -> {
                final long time = System.currentTimeMillis();
                // create logical view over graph maintaining graph computer global state data
                final LocalGraphComputerView view = FireflyHelper.createGraphComputerView(this.graph, this.graphFilter, null != this.vertexProgram ? this.vertexProgram.getVertexComputeKeys() : Collections.emptySet());

                // create thread pool of workers (single machine)
                final LocalWorkerPool workers = new LocalWorkerPool(this.graph, this.memory, this.workers);

                try {
                    final AtomicLong vertexCount = new AtomicLong(-1); // stores final iteration vertex count (used for mapreduce parittion size calculation)
                    if (null != this.vertexProgram) {
                        // execute the vertex program
                        this.vertexProgram.setup(this.memory);
                        while (true) {
                            vertexCount.set(0L);
                            if (Thread.interrupted()) throw new TraversalInterruptedException();
                            this.memory.completeSubRound();
                            workers.setVertexProgram(this.vertexProgram);
                            workers.executeVertexProgram((vertices, vertexProgram, workerMemory) -> {
                                final PureTraversal<?, ?> traversal = ((TraversalVertexProgram) vertexProgram).getTraversal().clone();
                                if (!traversal.get().isLocked())
                                    traversal.get().applyStrategies();
                                final TraversalMatrix<?, ?> traversalMatrix = new TraversalMatrix<>(traversal.get());
                                long counter = 0;
                                vertexProgram.workerIterationStart(workerMemory.asImmutable());
                                Pair<Iterator<FireflyVertex>, PrecomputableComputerStep> output = preComputeVertices(traversalMatrix, vertices, (TraversalVertexProgram) vertexProgram, workerMemory);
                                vertices = output.getLeft();
                                while (vertices.hasNext()) {
                                    final Vertex vertex = vertices.next();
                                    System.out.println(vertex.id());
                                    counter++;
                                    if (Thread.interrupted()) throw new TraversalInterruptedException();
                                    try {
                                        vertexProgram.execute(
                                                ComputerGraph.vertexProgram(vertex, vertexProgram),
                                                new LocalMessenger<>(vertex, this.messageBoard, vertexProgram.getMessageCombiner()),
                                                workerMemory);
                                    } catch (final Exception e) {
                                        LOG.error("Worker failed evaluating vertex {}", vertex.id(), e);
                                    }
                                }
                                vertexProgram.workerIterationEnd(workerMemory.asImmutable());
                                workerMemory.complete();
                                vertexCount.getAndAdd(counter);
                                if (output.getRight() != null) {
                                    output.getRight().release();
                                }
                                return counter;
                            }, this.graphFilter);
                            this.messageBoard.completeIteration();
                            this.memory.completeSubRound();
                            if (this.vertexProgram.terminate(this.memory)) {
                                this.memory.incrIteration();
                                break;
                            } else {
                                this.memory.incrIteration();
                            }
                        }
                        view.complete(); // drop all transient vertex compute keys (i.e. drop global state)
                    }

                    // execute mapreduce jobs
                    final int mapPartitionSize = Math.max(
                            (vertexCount.get() > 0 ? ((int) Math.ceil((double) vertexCount.get() / (double) this.workers)) : this.previousPartitionSize),
                            ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_PAGE_SIZE, this.graph.configuration()));
                    LOG.warn("MAPREDUCE STAGE PARTITION CONFIGURATION:\n\t" +
                            "Vertices in final vertex program iteration: {}\n\t" +
                            "Number of available workers: {}\n\t" +
                            "Computed partition size: {}", vertexCount.get(), this.workers, mapPartitionSize);
                    for (final MapReduce mapReduce : mapReducers) {
                        final LocalMapEmitter<?, ?> mapEmitter = new LocalMapEmitter<>(mapReduce.doStage(MapReduce.Stage.REDUCE));
                        workers.setMapReduce(mapReduce);
                        workers.executeMapReduce(workerMapReduce -> {
                            workerMapReduce.workerStart(MapReduce.Stage.MAP);
                            try (final PartitionIterator partitions = PartitionIterator.build(this.graph)
                                    .filters(this.graphFilter)
                                    .partitionSize(mapPartitionSize)
                                    .create()) {
                                while (partitions.hasNext()) {
                                    final Optional<CloseableIterator<FireflyVertex>> optional = partitions.next();
                                    if (optional.isEmpty())
                                        break;
                                    else {
                                        try (final CloseableIterator<FireflyVertex> itty = optional.get()) {
                                            while (itty.hasNext()) {
                                                if (Thread.interrupted()) throw new TraversalInterruptedException();
                                                final Vertex vertex = itty.next();
                                                workerMapReduce.map(ComputerGraph.mapReduce(vertex), mapEmitter);
                                            }
                                        }
                                    }

                                }
                                workerMapReduce.workerEnd(MapReduce.Stage.MAP);
                            }
                        });

                        // sort results if a map output sort is defined
                        mapEmitter.complete(mapReduce);

                        // no need to run combiners as this is single machine
                        if (mapReduce.doStage(MapReduce.Stage.REDUCE)) {
                            final LocalReduceEmitter<?, ?> reduceEmitter = new LocalReduceEmitter<>();
                            try (final SynchronizedIterator<Map.Entry<?, Queue<?>>> keyValues = new SynchronizedIterator<Map.Entry<?, Queue<?>>>((Iterator) mapEmitter.reduceMap.entrySet().iterator())) {
                                workers.executeMapReduce(workerMapReduce -> {
                                    workerMapReduce.workerStart(MapReduce.Stage.REDUCE);
                                    while (true) {
                                        if (Thread.interrupted()) throw new TraversalInterruptedException();
                                        final Map.Entry<?, Queue<?>> entry = keyValues.next();
                                        if (null == entry) break;
                                        workerMapReduce.reduce(entry.getKey(), entry.getValue().iterator(), reduceEmitter);
                                    }
                                    workerMapReduce.workerEnd(MapReduce.Stage.REDUCE);
                                });
                            }
                            reduceEmitter.complete(mapReduce); // sort results if a reduce output sort is defined
                            mapReduce.addResultToMemory(this.memory, reduceEmitter.reduceQueue.iterator());
                        } else {
                            mapReduce.addResultToMemory(this.memory, mapEmitter.mapQueue.iterator());
                        }

                    }
                    // update runtime and return the newly computed graph
                    this.memory.setRuntime(System.currentTimeMillis() - time);
                    this.memory.complete(); // drop all transient properties and set iteration
                    // determine the resultant graph based on the result graph/persist state
                    final Graph resultGraph = view.processResultGraphPersist(this.resultGraph, this.persist);
                    FireflyHelper.dropGraphComputerView(this.graph); // drop the view from the original source graph
                    return new DefaultComputerResult(resultGraph, this.memory.asImmutable());
                } catch (final InterruptedException ie) {
                    workers.closeNow();
                    throw new TraversalInterruptedException();
                } catch (final Exception ex) {
                    workers.closeNow();
                    throw new RuntimeException(ex);
                } finally {
                    workers.close();
                    this.computerService.shutdown();
                    this.graph.configuration().setProperty(ConfigurationHelper.Keys.PAGINATION_PAGE_SIZE, this.previousPartitionSize);
                }
            });
            return result;
        } catch (final Exception e) {
            LOG.error("A global error occurred. Shutting down {}: {}", this, e.getMessage());
            return new CompletableFuture<>();
        }
    }

    private PrecomputableComputerStep updatePrecompute(FireflyVertex vertex, PrecomputableComputerStep precomputableComputerStep, TraversalMatrix<?, ?> traversalMatrix, final Traverser.Admin<?> traverser) {
        final Step<Object, Object> currentStep = traversalMatrix.getStepById(traverser.getStepId());
        if (currentStep instanceof PrecomputableComputerStep) {
            if (precomputableComputerStep == null) {
                precomputableComputerStep = (PrecomputableComputerStep) currentStep;
            }
            precomputableComputerStep.add(traverser, vertex);
        } else if (currentStep instanceof GraphStep) {
            System.out.println("GraphSTep");
            final Step<?, ?> nextStep = currentStep.getNextStep();
            if (nextStep instanceof PrecomputableComputerStep) {
                if (precomputableComputerStep == null) {
                    precomputableComputerStep = (PrecomputableComputerStep) nextStep;
                }
                precomputableComputerStep.add(traverser, vertex);
            }
        }

        return precomputableComputerStep;
    }

    private Pair<Iterator<FireflyVertex>, PrecomputableComputerStep> preComputeVertices(final TraversalMatrix<?, ?> traversalMatrix,
                                                                                        final Iterator<FireflyVertex> vertices,
                                                                                        final TraversalVertexProgram vertexProgram,
                                                                                        final LocalWorkerMemory memory) {
        if (memory.isInitialIteration()) {
            final List<FireflyVertex> outputVertices = new ArrayList<>();
            final PrecomputableComputerStep[] precomputableComputerStep = {null};
            for (final FireflyVertex vertex : IteratorUtils.list(vertices)) {
                outputVertices.add(vertex);
                final TraverserSet<Object> activeTraversers = new TraverserSet<>();
                final VertexProperty<TraverserSet<Object>> property = vertex.property(HALTED_TRAVERSERS);
                final TraverserSet<Object> haltedTraversers;
                if (property.isPresent()) {
                    haltedTraversers = property.value();
                } else {
                    haltedTraversers = new TraverserSet<>();
                }
                haltedTraversers.stream().iterator().forEachRemaining(activeTraversers::add);

                if (vertexProgram.getTraversal().get().getStartStep() instanceof GraphStep) {
                    final GraphStep<Element, Element> graphStep = (GraphStep<Element, Element>) vertexProgram.getTraversal().get().getStartStep();
                    graphStep.reset();
                    activeTraversers.forEach(traverser -> graphStep.addStart((Traverser.Admin) traverser));
                    activeTraversers.clear();
                    if (graphStep.returnsVertex())
                        graphStep.setIteratorSupplier(() -> ElementHelper.idExists(vertex.id(), graphStep.getIds()) ? (Iterator) IteratorUtils.of(vertex) : EmptyIterator.instance());
                    else
                        graphStep.setIteratorSupplier(() -> (Iterator) IteratorUtils.filter(vertex.edges(Direction.OUT), edge -> ElementHelper.idExists(edge.id(), graphStep.getIds())));
                    graphStep.forEachRemaining(traverser -> {
                        if (!traverser.isHalted()) {
                            activeTraversers.add((Traverser.Admin) traverser);
                        }
                    });
                    if (graphStep.getNextStep() instanceof PrecomputableComputerStep) {

                    }
                }

                activeTraversers.forEach(traverser -> {
                    precomputableComputerStep[0] = updatePrecompute(vertex, precomputableComputerStep[0], traversalMatrix, traverser);
                    if (precomputableComputerStep[0] != null) {
                        System.out.println("activeTraversers");
                    }
                });
            }
            if (precomputableComputerStep[0] != null) {
                precomputableComputerStep[0].precompute();
            }

            return new Pair<>() {
                final Iterator<FireflyVertex> ffv = outputVertices.iterator();
                final PrecomputableComputerStep pccs = precomputableComputerStep[0];

                @Override
                public Iterator<FireflyVertex> getLeft() {
                    return ffv;
                }

                @Override
                public PrecomputableComputerStep getRight() {
                    return pccs;
                }

                @Override
                public PrecomputableComputerStep setValue(final PrecomputableComputerStep value) {
                    return null;
                }
            };
        } else {
            final IndexedTraverserSet<Object, Vertex> maybeActiveTraversers = memory.get(TraversalVertexProgram.ACTIVE_TRAVERSERS);
            final PrecomputableComputerStep[] precomputableComputerStep = {null};
            final List<FireflyVertex> outputVertices = new ArrayList<>();
            while (vertices.hasNext()) {
                final FireflyVertex vertex = vertices.next();
                outputVertices.add(vertex);
                synchronized (maybeActiveTraversers) {
                    if (!maybeActiveTraversers.isEmpty()) {
                        final Collection<Traverser.Admin<Object>> traversers = maybeActiveTraversers.get(vertex);
                        if (traversers == null) {
                            continue;
                        }
                        traversers.forEach(traverser -> {
                            precomputableComputerStep[0] = updatePrecompute(vertex, precomputableComputerStep[0], traversalMatrix, traverser);
                            if (precomputableComputerStep[0] != null) {
                                System.out.println("maybeActiveTraversers");
                            }
                        });
                    }

                    vertex.<TraverserSet<Object>>property(TraversalVertexProgram.ACTIVE_TRAVERSERS).ifPresent(previousActiveTraversers -> {
                        previousActiveTraversers.stream().forEach(traverser -> {
                            precomputableComputerStep[0] = updatePrecompute(vertex, precomputableComputerStep[0], traversalMatrix, traverser);
                            if (precomputableComputerStep[0] != null) {
                                System.out.println("previousActiveTraversers");
                            }
                        });
                    });

                    LocalMessenger messenger = new LocalMessenger<>(vertex, this.messageBoard, vertexProgram.getMessageCombiner());
                    Iterator<TraverserSet<Object>> messages = messenger.receiveMessages();
                    final AtomicInteger msgCount1 = new AtomicInteger();
                    while (messages.hasNext()) {
                        final TraverserSet<Object> traversers = messages.next();
                        traversers.forEach(traverser -> {
                            if (!traverser.isHalted()) {
                                msgCount1.getAndIncrement();
                                precomputableComputerStep[0] = updatePrecompute(vertex, precomputableComputerStep[0], traversalMatrix, traverser);
                                if (precomputableComputerStep[0] != null) {
                                    System.out.println("Messageboard");
                                }
                            }
                        });
                    }
                }
            }
            if (precomputableComputerStep[0] != null) {
                precomputableComputerStep[0].precompute();
            }

            return new Pair<>() {
                final Iterator<FireflyVertex> ffv = outputVertices.iterator();
                final PrecomputableComputerStep pccs = precomputableComputerStep[0];

                @Override
                public Iterator<FireflyVertex> getLeft() {
                    return ffv;
                }

                @Override
                public PrecomputableComputerStep getRight() {
                    return pccs;
                }

                @Override
                public PrecomputableComputerStep setValue(final PrecomputableComputerStep value) {
                    return null;
                }
            };
        }
    }

    @Override
    public String toString() {
        return StringFactory.graphComputerString(this);
    }

    public static class SynchronizedIterator<V> implements CloseableIterator<V> {

        private final Iterator<V> iterator;

        public SynchronizedIterator(final Iterator<V> iterator) {
            this.iterator = iterator;
        }

        public boolean hasNext() throws UnsupportedOperationException {
            throw new UnsupportedOperationException("Use " + this.getClass().getName() + ".next() and check if the returned element is null");
        }

        public synchronized V next() {
            if (this.iterator.hasNext())
                return this.iterator.next();
            else {
                this.close();
                return null;
            }
        }

        @Override
        public void close() {
            if (this.iterator instanceof CloseableIterator)
                ((CloseableIterator<V>) this.iterator).close();
        }

    }

    @Override
    public Features features() {
        return new Features() {
            @Override
            public boolean supportsResultGraphPersistCombination(final ResultGraph resultGraph, final Persist persist) {
                return persist == Persist.NOTHING || resultGraph == ResultGraph.ORIGINAL;// true;// persist == Persist.NOTHING;// persist != Persist.EDGES && persist != Persist.VERTEX_PROPERTIES && resultGraph != ResultGraph.NEW;
            }

            @Override
            public int getMaxWorkers() {
                return Runtime.getRuntime().availableProcessors();
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
}
