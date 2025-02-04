package com.aerospike.firefly.process.computer.util;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.ProjectedTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ComputerHelper {
    public static boolean onGraphComputer(Traversal.Admin<?, ?> traversal) {
        while (!(traversal.isRoot())) {
            if (traversal.getParent() instanceof TraversalVertexProgramStep)
                return true;
            traversal = traversal.getParent().asStep().getTraversal();
        }
        if (!traversal.getSteps().isEmpty()) {
            return traversal.getSteps().get(0) instanceof TraversalVertexProgramStep;
        } else {
            return false;
        }
    }

    public static List<HasContainer> getInitialHasContainers(final Traversal.Admin<?, ?> traversal) {
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

    private static void collectIds(final Traverser traverser,
                                   final Set<Object> vertexIds,
                                   final Set<Object> edgeIds) {
        if (traverser.get() instanceof Vertex)
            vertexIds.add(((Vertex) traverser.get()).id());
        else if (traverser.get() instanceof Edge)
            edgeIds.add(((Edge) traverser.get()).id());

        if (traverser instanceof ProjectedTraverser) {
            for (final Object projection : ((ProjectedTraverser) traverser).getProjections())
                if (projection instanceof Vertex)
                    vertexIds.add(((Vertex) projection).id());
                else if (projection instanceof Edge)
                    edgeIds.add(((Edge) projection).id());
        }
    }

    // todo: refactor
    public static void bulkAttach(final FireflyGraph graph,
                                  final TraversalSideEffects traversalSideEffects,
                                  final List<Object> elements) {
        final Set<Object> vertexIds = new HashSet<>();
        final Set<Object> edgeIds = new HashSet<>();
        elements.forEach(element -> {
            if (element instanceof Vertex) {
                vertexIds.add(((Vertex) element).id());
            } else if (element instanceof Edge) {
                edgeIds.add(((Edge) element).id());
            }
        });

        final Map<Object, Element> vertexCache = new HashMap<>();
        if (!vertexIds.isEmpty())
            graph.vertices(vertexIds.toArray(new Object[vertexIds.size()])).forEachRemaining(vertex -> vertexCache.put(vertex.id(), vertex));

        final Map<Object, Element> edgeCache = new HashMap<>();
        if (!edgeIds.isEmpty())
            graph.edges(edgeIds.toArray(new Object[edgeIds.size()])).forEachRemaining(edge -> edgeCache.put(edge.id(), edge));

        final List copy = new ArrayList<>(elements);
        for (int i = 0; i < copy.size(); i++) {
            if (copy.get(i) instanceof Vertex && vertexCache.containsKey(((Vertex) copy.get(i)).id())) {
                elements.remove(i);
                elements.add(i, vertexCache.get(((Vertex) copy.get(i)).id()));
            } else if (copy.get(i) instanceof Edge && edgeCache.containsKey(((Edge) copy.get(i)).id())) {
                elements.remove(i);
                elements.add(i, edgeCache.get(((Edge) copy.get(i)).id()));
            }
        }
    }

    public static void bulkAttach(final FireflyGraph graph,
                                  final TraversalSideEffects traversalSideEffects,
                                  final TraverserSet<Object> traversers) {
        final Set<Object> vertexIds = new HashSet<>();
        final Set<Object> edgeIds = new HashSet<>();
        traversers.forEach(traverser -> collectIds(traverser, vertexIds, edgeIds));

        final Map<Object, Element> vertexCache = new HashMap<>();
        if (!vertexIds.isEmpty())
            graph.vertices(vertexIds.toArray(new Object[vertexIds.size()])).forEachRemaining(vertex -> vertexCache.put(vertex.id(), vertex));

        final Map<Object, Element> edgeCache = new HashMap<>();
        if (!edgeIds.isEmpty())
            graph.edges(edgeIds.toArray(new Object[edgeIds.size()])).forEachRemaining(edge -> edgeCache.put(edge.id(), edge));

        traversers.forEach(traverser -> {
            if (traverser.get() instanceof Vertex && vertexCache.containsKey(((Vertex) traverser.get()).id())) {
                final Vertex vertex = (Vertex) vertexCache.get(((Vertex) traverser.get()).id());
                traverser.attach(Attachable.Method.get(vertex));
                traverser.setSideEffects(traversalSideEffects);
            } else if (traverser.get() instanceof Edge && edgeCache.containsKey(((Edge) traverser.get()).id())) {
                final Edge edge = (Edge) edgeCache.get(((Edge) traverser.get()).id());
                // todo: better attachment way for edges
                traverser.attach(Attachable.Method.get(edge.outVertex()));
                traverser.setSideEffects(traversalSideEffects);
            }

            if (traverser instanceof ProjectedTraverser) {
                final List original = ((ProjectedTraverser) traverser).getProjections();
                final List copy = new ArrayList<>(original);
                for (int i = 0; i < copy.size(); i++) {
                    final Object projection = copy.get(i);
                    if (projection instanceof Vertex && vertexCache.containsKey(((Vertex) projection).id())) {
                        original.remove(i);
                        original.add(i, vertexCache.get(((Vertex) projection).id()));
                    } else if (projection instanceof Edge && edgeCache.containsKey(((Edge) projection).id())) {
                        original.remove(i);
                        original.add(i, edgeCache.get(((Edge) projection).id()));
                    }
                }
            }
        });
    }

    // some types can't be handled by
    public static void prepareForDistributedMemory(final Traverser traverser) {
        if (traverser.get() instanceof Map.Entry) {
            final Map.Entry entry = (Map.Entry) traverser.get();
            traverser.asAdmin().set(new AbstractMap.SimpleEntry(entry.getKey(), entry.getValue()));
        }
    }
}
