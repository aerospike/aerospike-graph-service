package com.aerospike.firefly.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer.Persist;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.GraphFilterStrategy;
import org.apache.tinkerpop.gremlin.process.computer.util.EmptyMemory;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class FireflyGraphFilterStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {
    private static final FireflyGraphFilterStrategy INSTANCE = new FireflyGraphFilterStrategy();
    GraphFilterStrategy graphFilterStrategy;

    private FireflyGraphFilterStrategy() {
    }

    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.getStepsOfAssignableClass(VertexProgramStep.class, traversal).size() <= 1) {
            final Graph graph = traversal.getGraph().orElse(EmptyGraph.instance());

            for (final TraversalVertexProgramStep step : TraversalHelper.getStepsOfClass(TraversalVertexProgramStep.class, traversal)) {
                final Traversal.Admin<?, ?> computerTraversal = step.generateProgram(graph, EmptyMemory.instance()).getTraversal().get().clone();
                if (!computerTraversal.isLocked()) {
                    computerTraversal.applyStrategies();
                }
                Computer computer = step.getComputer();
                if (null == computer.getEdges() && !Persist.EDGES.equals(computer.getPersist())) {
                    Traversal.Admin<Vertex, Edge> edgeFilter = getEdgeFilter(computerTraversal);
                    if (edgeFilter != null)
                        computer = computer.edges(edgeFilter);
                }
                if (null == computer.getVertices()) {
                    Traversal.Admin<Vertex, Vertex> vertexFilter = getVertexFilter(computerTraversal);
                    if (vertexFilter != null)
                        computer = computer.vertices(vertexFilter);
                }
                step.setComputer(computer);
            }

        }
    }

    private static Traversal.Admin<Vertex, Vertex> getVertexFilter(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.hasStepOfAssignableClassRecursively(VertexStep.class, traversal))
            return null;
        GraphTraversal.Admin<Vertex, Vertex> hasTraversal = new DefaultGraphTraversal<>();
        if (traversal.getStartStep() instanceof GraphStep && ((GraphStep<Vertex, Vertex>) traversal.getStartStep()).returnsVertex()) {
            if (Stream.of(((GraphStep) traversal.getStartStep()).getIds()).count() > 0)
                hasTraversal = hasTraversal.has(T.id, P.eq(P.within(((GraphStep) traversal.getStartStep()).getIds()))).asAdmin();
            for (Step<?, ?> currentStep = ((GraphStep) traversal.getStartStep()).getNextStep();
                 currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep || currentStep instanceof ProfileStep;
                 currentStep = currentStep.getNextStep()) {
                if (currentStep instanceof HasStep) {
                    for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers().stream()
                            .filter(h -> h.getKey().equals(T.id.getAccessor()) || h.getValue() instanceof Number ||h.getValue() instanceof Number ||
                                    (h.getPredicate().getPredicateName().equals(P.eq(1).getPredicateName()))).collect(Collectors.toList())) {
                        hasTraversal = hasTraversal.has(hasContainer.getKey(), hasContainer.getPredicate()).asAdmin();
                    }
                }
            }
        }
        return hasTraversal.getSteps().isEmpty() ? null : hasTraversal;
    }

    /*
     * Using reflection on GraphFilterStrategy to access protected getEdgeFilter()
     */
    private static Traversal.Admin<Vertex, Edge> getEdgeFilter(final Traversal.Admin<?, ?> traversal) {
        try {
            return (Traversal.Admin<Vertex, Edge>) Stream.concat(Arrays.stream(GraphFilterStrategy.class.getDeclaredMethods()), Arrays.stream(GraphFilterStrategy.class.getMethods()))
                    .filter(m -> Modifier.isStatic(m.getModifiers()))
                    .map(m -> {
                        m.setAccessible(true);
                        return m;
                    })
                    .filter(m -> m.getName().equals("getEdgeFilter")).findFirst().get().invoke(null, traversal);
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static FireflyGraphFilterStrategy instance() {
        return INSTANCE;
    }
}
