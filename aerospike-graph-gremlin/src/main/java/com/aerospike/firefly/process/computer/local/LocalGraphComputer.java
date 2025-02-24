package com.aerospike.firefly.process.computer.local;

import com.aerospike.firefly.io.aerospike.query.paged.PartitionIterator;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGraphFilterStrategy;
import com.aerospike.firefly.process.traversal.strategy.verification.FireflyComputerVerificationStrategy;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.FireflyHelper;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
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
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.HALTED_TRAVERSERS;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LocalGraphComputer implements GraphComputer {

    private static final Logger LOG = LoggerFactory.getLogger(LocalGraphComputer.class);
    private ResultGraph resultGraph = null;
    private Persist persist = null;

    private final FireflyGraph graph;
    private LocalMemory memory;
    private final LocalMessageBoard messageBoard = new LocalMessageBoard();
    private boolean executed = false;
    private final Set<MapReduce> mapReducers = new HashSet<>();
    private int workers;
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
        this.workers = graph.getBaseGraph().OLAP_WORKERS;
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

    class ExecuteVertexProgram {
        final LocalMessageBoard messageBoard;

        // Iterator<FireflyVertex>, VertexProgram, LocalWorkerMemory, Pair<Long, List<Element>>
        public ExecuteVertexProgram(final LocalMessageBoard messageBoard, final PureTraversal<?, ?> traversal) {
            this.messageBoard = messageBoard;
            Traversal<?, ?> traversal1 = traversal.get().clone();
            if (!traversal1.asAdmin().isLocked())
                traversal1.asAdmin().applyStrategies();
        }

        public List<Element> execute(final Object vertexProgram,
                                     final LocalWorkerMemory workerMemory,
                                     final Predicate workerIdFilter) throws Exception {
            return messageBoard.getVerticesWithTraversers();
        }
    }

    private static List<HasContainer> getInitialHasContainers(final Traversal.Admin<?, ?> traversal) {
        final List<HasContainer> hasContainers = new ArrayList<>();
        if (traversal.getStartStep() instanceof GraphStep && ((GraphStep<Vertex, Vertex>) traversal.getStartStep()).returnsVertex()) {
            if (Stream.of(((GraphStep) traversal.getStartStep()).getIds()).count() > 0)
                hasContainers.add(new HasContainer(T.id.getAccessor(), P.eq(P.within(((GraphStep) traversal.getStartStep()).getIds()))));
            for (Step<?, ?> currentStep = ((GraphStep) traversal.getStartStep()).getNextStep();
                 currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep || currentStep instanceof ProfileStep;
                 currentStep = currentStep.getNextStep()) {
                if (currentStep instanceof HasStep) {
                    if (((HasStep) currentStep).getHasContainers().stream().filter(it -> (((HasContainer) it).getKey() == null)).findAny().isPresent()) {
                        for (HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                            hasContainers.add(hasContainer);
                        }
                    } else {
                        for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers().stream()
                                .filter(h -> h.getKey().equals(T.id.getAccessor()) || h.getValue() instanceof Number || h.getValue() instanceof Number ||
                                        (h.getPredicate().getPredicateName().equals(P.eq(1).getPredicateName()))).collect(Collectors.toList())) {
                            hasContainers.add(hasContainer);
                        }
                    }
                }
            }
        }
        return hasContainers;
    }

    @Override
    public Future<ComputerResult> submit() {
        return null;
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
}
