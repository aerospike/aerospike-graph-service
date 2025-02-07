package com.aerospike.firefly.process.computer.util;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ReflectionHelper;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.ProjectedTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferencePath;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;
import scala.xml.Elem;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

        if (traversal.getStartStep() instanceof GraphStep) {
            if (Stream.of(((GraphStep) traversal.getStartStep()).getIds()).count() > 0) {
                ((GraphStep) traversal.getStartStep()).getIds();
                hasContainers.add(new HasContainer(T.id.getAccessor(), P.eq(P.within(((GraphStep) traversal.getStartStep()).getIds()))));
            } else if (traversal.getStartStep().getNextStep() instanceof IdStep) {
                Step currentStep = traversal.getStartStep().getNextStep().getNextStep();
                while (currentStep instanceof HasStep) {
                    HasStep currentHasStep = (HasStep) currentStep;
                    final List<HasContainer> allHasContainers = currentHasStep.getHasContainers();
                    final List<HasContainer> nonIdContainers = allHasContainers.stream().filter(c -> !c.getKey().equals(T.id.getAccessor())).collect(Collectors.toList());
                    final List<HasContainer> idContainers = allHasContainers.stream().filter(c -> c.getKey().equals(T.id.getAccessor())).collect(Collectors.toList());
                    hasContainers.addAll(nonIdContainers);
                    if (!idContainers.isEmpty()) {
                        final List<Object> ids = new ArrayList<>();
                        for (final HasContainer idContainer : idContainers) {
                            ids.add(idContainer.getPredicate().getValue());
                        }
                        hasContainers.add(new HasContainer(T.id.getAccessor(), P.eq(P.within(ids))));
                    }
                    currentStep = currentStep.getNextStep();
                }
            }
            for (Step<?, ?> currentStep = ((GraphStep) traversal.getStartStep()).getNextStep();
                 currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep || currentStep instanceof ProfileStep;
                 currentStep = currentStep.getNextStep()) {
                if (currentStep instanceof HasStep) {
                    if (((HasStep) currentStep).getHasContainers().stream().filter(it -> (((HasContainer) it).getKey() == null)).findAny().isPresent()) {
                        for (HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                            hasContainers.add(hasContainer);
                        }
                    } else {
                        for (final HasContainer container : (((HasContainerHolder) currentStep).getHasContainers())) {
                            hasContainers.add(container);
                        }
                    }
                }
            }
        }
        return hasContainers;
    }

    private static void collectIds(final Object object,
                                   final Set<Object> vertexIds,
                                   final Set<Object> edgeIds) {
        if (object == null)
            return;

        if (object instanceof Vertex)
            vertexIds.add(((Vertex) object).id());
        else if (object instanceof Edge)
            edgeIds.add(((Edge) object).id());
        else if (object instanceof VertexProperty)
            vertexIds.add(((VertexProperty) object).element().id());
        else if (object instanceof ReferenceProperty) {
            collectIds(((ReferenceProperty) object).element(), vertexIds, edgeIds);
        } else if (object instanceof ReferencePath) {
            ((ReferencePath) object).forEach((obj, labels) -> collectIds(obj, vertexIds, edgeIds));
        } else if (object instanceof Collection) {
            for (final Object nested : (Collection) object)
                collectIds(nested, vertexIds, edgeIds);
        }
    }

    private static void collectIds(final Traverser traverser,
                                   final Set<Object> vertexIds,
                                   final Set<Object> edgeIds) {
        collectIds(traverser.get(), vertexIds, edgeIds);

        if (traverser instanceof ProjectedTraverser) {
            for (final Object projection : ((ProjectedTraverser) traverser).getProjections())
                collectIds(projection, vertexIds, edgeIds);
        }

        collectIds(traverser.path(), vertexIds, edgeIds);
    }

    private static Object getFromCache(final Object object,
                                       final Map<Object, Element> vertexCache,
                                       final Map<Object, Element> edgeCache) {
        if (object instanceof Vertex && vertexCache.containsKey(((Vertex) object).id())) {
            return vertexCache.get(((Vertex) object).id());
        } else if (object instanceof Edge && edgeCache.containsKey(((Edge) object).id())) {
            return edgeCache.get(((Edge) object).id());
        } else if (object instanceof VertexProperty && vertexCache.containsKey(((VertexProperty) object).element().id())) {
            final Vertex vertex = (Vertex) vertexCache.get(((VertexProperty) object).element().id());
            final Iterator<VertexProperty<Object>> itty = vertex.properties();
            // todo: verify is firefly caches vertex properties
            while (itty.hasNext()) {
                // vertex property attachment require key, so shortcut here
                final VertexProperty vp = itty.next();
                if (vp.id().equals(((VertexProperty<?>) object).id())) {
                    return vp;
                }
            }
            return object;
        } else if (object instanceof ReferenceProperty && edgeCache.containsKey(((ReferenceProperty) object).element().id())) {
            final Edge edge = (Edge) edgeCache.get(((ReferenceProperty) object).element().id());
            final Iterator<Property<Object>> itty = edge.properties();
            while (itty.hasNext()) {
                // vertex property attachment require key, so shortcut here
                final Property p = itty.next();
                if (p.key().equals(((Property<?>) object).key())) {
                    return p;
                }
            }
        }

        return object;
    }

    // todo: refactor
    public static void bulkAttach(final FireflyGraph graph,
                                  final TraversalSideEffects traversalSideEffects,
                                  final List<Object> elements) {
        final Set<Object> vertexIds = new HashSet<>();
        final Set<Object> edgeIds = new HashSet<>();
        elements.forEach(element -> collectIds(element, vertexIds, edgeIds));

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
                                  final Map elements) {
        final Set<Object> vertexIds = new HashSet<>();
        final Set<Object> edgeIds = new HashSet<>();
        elements.forEach((k, v) -> {
            collectIds(k, vertexIds, edgeIds);
            if (v instanceof Traverser)
                collectIds((Traverser) v, vertexIds, edgeIds);
            else if (v instanceof List) {
                // group by can have list of elements
                ((List) v).forEach(i -> collectIds(i, vertexIds, edgeIds));
            }
        });

        final Map<Object, Element> vertexCache = new HashMap<>();
        if (!vertexIds.isEmpty())
            graph.vertices(vertexIds.toArray(new Object[vertexIds.size()])).forEachRemaining(vertex -> vertexCache.put(vertex.id(), vertex));

        final Map<Object, Element> edgeCache = new HashMap<>();
        if (!edgeIds.isEmpty())
            graph.edges(edgeIds.toArray(new Object[edgeIds.size()])).forEachRemaining(edge -> edgeCache.put(edge.id(), edge));

        final Map<?, ?> copy = new HashMap<>(elements);
        for (final Map.Entry entry : copy.entrySet()) {
            final Object newKey = getFromCache(entry.getKey(), vertexCache, edgeCache);
            Object newValue = entry.getValue();
            if (newValue instanceof Traverser) {
                ((Traverser) newValue).asAdmin().set(getFromCache(((Traverser) newValue).get(), vertexCache, edgeCache));
                ((Traverser) newValue).asAdmin().setSideEffects(traversalSideEffects);
            } else if (newValue instanceof List) {
                ((List) newValue).replaceAll(object -> getFromCache(object, vertexCache, edgeCache));
            }
            elements.remove(entry.getKey());
            elements.put(newKey, newValue);
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
            if (traverser.get() instanceof Element || traverser.get() instanceof Property) {
                final Object newValue = getFromCache(traverser.get(), vertexCache, edgeCache);
                traverser.set(newValue);
                traverser.setSideEffects(traversalSideEffects);
            } else if (traverser.get() instanceof BulkSet) {
                final BulkSet attached = new BulkSet();
                ((BulkSet) traverser.get()).forEach((element, bulk) ->
                        attached.add(getFromCache(element, vertexCache, edgeCache), (long) bulk)
                );
                traverser.set(attached);
                traverser.setSideEffects(traversalSideEffects);
            }

            attachPath(traverser, vertexCache, edgeCache);

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

    private static void attachPath(final Traverser traverser,
                                   final Map<Object, Element> vertexCache,
                                   final Map<Object, Element> edgeCache) {
        final Path path = traverser.path();
        if (path == null || path.isEmpty())
            return;

        final Path attached = MutablePath.make();
        path.forEach((obj, labels) ->
                attached.extend(getFromCache(obj, vertexCache, edgeCache), labels)
        );

        final Traverser.Admin t = ProjectedTraverser.tryUnwrap(traverser.asAdmin());
        ReflectionHelper.setFieldValue(t, "path", attached);
    }

    public static void prepareEdgesForFeatureTests(final FireflyGraph graph,
                                                   final TraverserSet<Object> traversers) {
        final Set<Object> edgeIds = new HashSet<>();
        traversers.forEach(traverser -> {
            // we care only about ReferenceEdge for now
            if (traverser.get() instanceof ReferenceEdge) {
                edgeIds.add(((ReferenceEdge) traverser.get()).id());
            }
        });

        final Map<Object, Edge> edgeCache = new HashMap<>();
        if (!edgeIds.isEmpty())
            graph.edges(edgeIds.toArray(new Object[edgeIds.size()])).forEachRemaining(edge -> edgeCache.put(edge.id(), edge));

        traversers.forEach(traverser -> {
            if (traverser.get() instanceof ReferenceEdge && edgeCache.containsKey(((ReferenceEdge) traverser.get()).id())) {
                final Edge edge = edgeCache.get(((ReferenceEdge) traverser.get()).id());
                traverser.asAdmin().set(ReferenceFactory.detach(edge));
            }
        });
    }

    public static Object detach(final Object barrier) {
        // not handled by ReferenceFactory
        if (barrier instanceof TraverserSet) {
            final TraverserSet original = (TraverserSet) barrier;
            // iterate over copy to be able to add/remove items
            for (final Object t : new HashSet<>(original)) {
                if (t instanceof ProjectedTraverser) {
                    original.remove(t);
                    original.add(new ProjectedTraverser((ProjectedTraverser.tryUnwrap((ProjectedTraverser) t)).detach(),
                            ReferenceFactory.detach(((ProjectedTraverser) t).getProjections())));
                } else if (t instanceof Traverser.Admin) {
                    original.remove(t);
                    original.add(((Traverser.Admin<?>) t).detach());
                }
            }
        }
        return ReferenceFactory.detach(barrier);
    }

    // some types can't be handled by
    public static void prepareForDistributedMemory(final Traverser traverser) {
        if (traverser.get() instanceof Map.Entry) {
            final Map.Entry entry = (Map.Entry) traverser.get();
            traverser.asAdmin().set(new AbstractMap.SimpleEntry(entry.getKey(), entry.getValue()));
        }
    }
}
