package com.aerospike.firefly.olap.helper;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ReflectionHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.ProjectedTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversalSideEffects;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferencePath;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class AttachmentHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(AttachmentHelper.class);


    private static void collectIds(final Object object,
                                   final Set<Object> vertexIds,
                                   final Set<Object> edgeIds) {
        if (object == null)
            return;

        if (object instanceof Traverser)
            collectIds((Traverser) object, vertexIds, edgeIds);
        if (object instanceof ReferenceVertex || object instanceof DetachedVertex)
            vertexIds.add(((Vertex) object).id());
        else if (object instanceof ReferenceEdge || object instanceof DetachedEdge)
            edgeIds.add(((Edge) object).id());
        else if (object instanceof ReferenceVertexProperty || object instanceof DetachedVertexProperty)
            vertexIds.add(((VertexProperty) object).element().id());
        else if (object instanceof ReferenceProperty || object instanceof DetachedProperty) {
            collectIds(((Property) object).element(), vertexIds, edgeIds);
        } else if (object instanceof ReferencePath) {
            ((ReferencePath) object).forEach((obj, labels) -> collectIds(obj, vertexIds, edgeIds));
        } else if (object instanceof Map) {
            ((Map) object).forEach((k, v) -> {
                collectIds(k, vertexIds, edgeIds);
                collectIds(v, vertexIds, edgeIds);
            });
        } else if (object instanceof Collection) {
            ((Collection) object).forEach(i -> collectIds(i, vertexIds, edgeIds));
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
            while (itty.hasNext()) {
                // vertex property attachment require key, so shortcut here
                final VertexProperty vp = itty.next();
                if (vp.id().equals(((VertexProperty<?>) object).id())) {
                    return vp;
                }
            }
            return object;
        } else if (object instanceof Property && edgeCache.containsKey(((Property) object).element().id())) {
            final Edge edge = (Edge) edgeCache.get(((Property) object).element().id());
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
        graph.logMessage("Bulk attach element list " + elements.size(), LOGGER);
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
        graph.logMessage("Bulk attach element Map " + elements.size(), LOGGER);
        final Set<Object> vertexIds = new HashSet<>();
        final Set<Object> edgeIds = new HashSet<>();
        collectIds(elements, vertexIds, edgeIds);

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
            } else if (newValue instanceof Set) {
                // need to transform incoming Set to TraverserSet
                // todo: attachment
                final TraverserSet temp = new TraverserSet<>();
                ((Set) entry.getValue()).forEach(t -> {
                    if (t instanceof Traverser.Admin)
                        temp.add((Traverser.Admin) t);
                });
                if (!temp.isEmpty() && temp.size() == ((Set<?>) newValue).size())
                    newValue = temp;
            }
            elements.remove(entry.getKey());
            elements.put(newKey, newValue);
        }
    }

    public static void bulkAttach(final FireflyGraph graph,
                                  final TraversalSideEffects traversalSideEffects,
                                  final TraverserSet<Object> traversers) {
        graph.logMessage("Bulk attach TraverserSet " + traversers.size(), LOGGER);
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
            } else if (traverser.get() instanceof BulkSet) {
                final BulkSet attached = new BulkSet();
                ((BulkSet) traverser.get()).forEach((element, bulk) ->
                        attached.add(getFromCache(element, vertexCache, edgeCache), (long) bulk)
                );
                traverser.set(attached);
            } else if (traverser.get() instanceof Map) {
                final HashMap attached = new HashMap();
                ((HashMap) traverser.get()).forEach((key, value) ->
                        attached.put(getFromCache(key, vertexCache, edgeCache), getFromCache(value, vertexCache, edgeCache))
                );
                traverser.set(attached);
            } else if (traverser.get() instanceof Path) {
                final Path attached = buildPath((Path) traverser.get(), vertexCache, edgeCache);
                traverser.set(attached);
            }

            traverser.setSideEffects(traversalSideEffects);

            attachPath(traverser, vertexCache, edgeCache);

            if (traverser instanceof ProjectedTraverser) {
                final List original = ((ProjectedTraverser) traverser).getProjections();
                final List copy = new ArrayList<>(original);
                for (int i = 0; i < copy.size(); i++) {
                    final Object newValue = getFromCache(copy.get(i), vertexCache, edgeCache);
                    if (copy.get(i) != newValue) {
                        original.remove(i);
                        original.add(i, newValue);
                    }
                }
            }
        });
    }

    private static Path buildPath(final Path path,
                                   final Map<Object, Element> vertexCache,
                                   final Map<Object, Element> edgeCache) {
        if (path == null || path.isEmpty())
            return path;

        final Path attached = MutablePath.make();
        path.forEach((obj, labels) ->
                attached.extend(getFromCache(obj, vertexCache, edgeCache), labels)
        );

        return attached;
    }

    private static void attachPath(final Traverser traverser,
                                   final Map<Object, Element> vertexCache,
                                   final Map<Object, Element> edgeCache) {
        final Path attached = buildPath(traverser.path(), vertexCache, edgeCache);
        if (attached != traverser.path()) {
            final Traverser.Admin t = ProjectedTraverser.tryUnwrap(traverser.asAdmin());
            ReflectionHelper.setFieldValue(t, "path", attached);
        }
    }

    public static void makeDetachedElements(final FireflyGraph graph,
                                            final TraverserSet<Object> traversers) {
        bulkAttach(graph,  EmptyTraversalSideEffects.instance(), traversers);
        // for TraverserSet detach modify incoming object
        detach(traversers, false);
    }

    public static Object detach(final Object value, final Boolean useReference) {
        final Function<Object, Object> detacher = useReference
                ? ReferenceFactory::detach
                : (o) -> DetachedFactory.detach(o, true);
        return detach(value, detacher);
    }

    private static Object detach(final Object value, Function<Object, Object> detacher) {
        // not handled by ReferenceFactory
        if (value instanceof TraverserSet) {
            for (final Object t : (TraverserSet) value) {
                if (t instanceof ProjectedTraverser) {
                    (ProjectedTraverser.tryUnwrap((ProjectedTraverser) t)).detach();
                    ReflectionHelper.setFieldValue(ProjectedTraverser.class, t, "projections",
                            detacher.apply(((ProjectedTraverser) t).getProjections()));
                } else if (t instanceof Traverser) {
                    final Traverser.Admin traverser = ((Traverser) t).asAdmin();
                    if (traverser.get() instanceof Map.Entry) {
                        final Map.Entry entry = (Map.Entry) traverser.get();
                        traverser.set(new AbstractMap.SimpleEntry(entry.getKey(), entry.getValue()));
                    }

                    // we need only traversal value to be detached, path can be reference
                    final Object detached = detacher.apply(traverser.get());
                    traverser.detach();
                    traverser.set(detached);
                }
            }

            return value;
        }

        // ReferenceFactory don't detach traversers
        if (value instanceof Map) {
            final Map<Object, Object> map = (Map<Object, Object>) value;
            for (final Map.Entry<Object, Object> entry : map.entrySet()) {
                if (entry.getValue() instanceof Map)
                    map.put(entry.getKey(), detach(entry.getValue(), detacher));
                else if (entry.getValue() instanceof Traverser) {
                    final Traverser.Admin traverser = ((Traverser) entry.getValue()).asAdmin();
                    // used only by barriers, so it's ok to always use default traverser.detach here
                    // final Object detached = detacher.apply(traverser.get());
                    traverser.detach();
                    // traverser.set(detached);
                }
            }
        }

        return detacher.apply(value);
    }
}
