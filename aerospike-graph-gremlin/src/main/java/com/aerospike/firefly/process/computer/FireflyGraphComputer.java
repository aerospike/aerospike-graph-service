package com.aerospike.firefly.process.computer;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.FireflyHelper;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FireflyGraphComputer implements GraphComputer {

    private static final Logger LOG = LoggerFactory.getLogger(FireflyGraphComputer.class);
    private ResultGraph resultGraph = null;
    private Persist persist = null;

    private VertexProgram<?> vertexProgram;
    private final FireflyGraph graph;
    private FireflyMemory memory;
    private final FireflyMessageBoard messageBoard = new FireflyMessageBoard();
    private boolean executed = false;
    private final Set<MapReduce> mapReducers = new HashSet<>();
    private int workers = Runtime.getRuntime().availableProcessors();
    private final GraphFilter graphFilter = new GraphFilter();

    private final ThreadFactory threadFactoryBoss = new BasicThreadFactory.Builder().namingPattern(FireflyGraphComputer.class.getSimpleName() + "-boss").build();
    private final ExecutorService computerService = Executors.newSingleThreadExecutor(threadFactoryBoss);

    public FireflyGraphComputer(final FireflyGraph graph) {
        this.graph = graph;

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
        LOG.warn("{} graph computer workers executing {}", this.workers, this.vertexProgram.toString());
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
        if (!this.features().supportsResultGraphPersistCombination(this.resultGraph, this.persist))
            throw GraphComputer.Exceptions.resultGraphPersistCombinationNotSupported(this.resultGraph, this.persist);
        // ensure requested workers are not larger than supported workers
        if (this.workers > this.features().getMaxWorkers())
            throw GraphComputer.Exceptions.computerRequiresMoreWorkersThanSupported(this.workers, this.features().getMaxWorkers());

        // initialize the memory
        this.memory = new FireflyMemory(this.vertexProgram, this.mapReducers);
        final Future<ComputerResult> result = computerService.submit(() -> {
            final long time = System.currentTimeMillis();
            final FireflyGraphComputerView view = FireflyHelper.createGraphComputerView(this.graph, this.graphFilter, null != this.vertexProgram ? this.vertexProgram.getVertexComputeKeys() : Collections.emptySet());
            final FireflyWorkerPool workers = new FireflyWorkerPool(this.graph, this.memory, this.workers);


            try {
                if (null != this.vertexProgram) {
                    // execute the vertex program
                    this.vertexProgram.setup(this.memory);
                    while (true) {
                        if (Thread.interrupted()) throw new TraversalInterruptedException();
                        this.memory.completeSubRound();
                        workers.setVertexProgram(this.vertexProgram);
                        workers.executeVertexProgram((vertices, vertexProgram, workerMemory) -> {
                            vertexProgram.workerIterationStart(workerMemory.asImmutable());
                            try {
                                while (true) {
                                    final Vertex vertex = vertices.next();
                                    if (null == vertex)
                                        break;
                                    if (Thread.interrupted()) throw new TraversalInterruptedException();
                                    vertexProgram.execute(
                                            ComputerGraph.vertexProgram(vertex, vertexProgram),
                                            new FireflyMessenger<>(vertex, this.messageBoard, vertexProgram.getMessageCombiner()),
                                            workerMemory);
                                }
                            } catch (NoSuchElementException ignored) {
                            }
                            vertexProgram.workerIterationEnd(workerMemory.asImmutable());
                            workerMemory.complete();
                        });
                        this.messageBoard.completeIteration();
                        this.memory.completeSubRound();
                        if (this.vertexProgram.terminate(this.memory)) {
                            this.memory.incrIteration();
                            break;
                        } else {
                            this.memory.incrIteration();
                        }
                    }
                    view.complete(); // drop all transient vertex compute keys
                }

                // execute mapreduce jobs
                for (final MapReduce mapReduce : mapReducers) {
                    final FireflyMapEmitter<?, ?> mapEmitter = new FireflyMapEmitter<>(mapReduce.doStage(MapReduce.Stage.REDUCE));
                    final SynchronizedIterator<Vertex> vertices = new SynchronizedIterator<>(this.graph.vertices());
                    workers.setMapReduce(mapReduce);
                    workers.executeMapReduce(workerMapReduce -> {
                        workerMapReduce.workerStart(MapReduce.Stage.MAP);
                        while (true) {
                            if (Thread.interrupted()) throw new TraversalInterruptedException();
                            final Vertex vertex = vertices.next();
                            if (null == vertex) break;
                            workerMapReduce.map(ComputerGraph.mapReduce(vertex), mapEmitter);
                        }
                        workerMapReduce.workerEnd(MapReduce.Stage.MAP);
                    });

                    // sort results if a map output sort is defined
                    mapEmitter.complete(mapReduce);

                    // no need to run combiners as this is single machine
                    if (mapReduce.doStage(MapReduce.Stage.REDUCE)) {
                        final FireflyReduceEmitter<?, ?> reduceEmitter = new FireflyReduceEmitter<>();
                        final SynchronizedIterator<Map.Entry<?, Queue<?>>> keyValues = new SynchronizedIterator((Iterator) mapEmitter.reduceMap.entrySet().iterator());
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
            } catch (InterruptedException ie) {
                workers.closeNow();
                throw new TraversalInterruptedException();
            } catch (Exception ex) {
                workers.closeNow();
                throw new RuntimeException(ex);
            } finally {
                workers.close();
            }
        });
        this.computerService.shutdown();
        return result;
    }

    @Override
    public String toString() {

        return StringFactory.graphComputerString(this);
    }

    public static class SynchronizedIterator<V> implements Iterator<V> {

        private final Iterator<V> iterator;

        public SynchronizedIterator(final Iterator<V> iterator) {
            this.iterator = iterator;
        }

        public boolean hasNext() throws UnsupportedOperationException {
            throw new UnsupportedOperationException("Only use next() and check if the returned element is null");
        }

        public synchronized V next() {
            return this.iterator.hasNext() ? this.iterator.next() : null;
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
