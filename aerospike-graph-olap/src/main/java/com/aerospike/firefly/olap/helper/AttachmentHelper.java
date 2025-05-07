package com.aerospike.firefly.olap.helper;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ReflectionHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.ProjectedTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversalSideEffects;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class AttachmentHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(AttachmentHelper.class);

    public static void bulkAttach(final FireflyGraph graph,
                                  final Collection<Object> elements) {
        graph.logMessage("Bulk attach element Collection " + elements.size(), LOGGER);
        final AttachmentCache cache = new AttachmentCache(graph);
        cache.collectIds(elements);

        if (!cache.needAttachment())
            return;

        cache.fill();

        final Collection copy = new ArrayList<>(elements);
        elements.clear();
        for (final Object o : copy) {
            elements.add(cache.get(o));
        }
    }

    public static void bulkAttach(final FireflyGraph graph,
                                  final TraversalSideEffects traversalSideEffects,
                                  final Map elements) {
        graph.logMessage("Bulk attach element Map " + elements.size(), LOGGER);
        final AttachmentCache cache = new AttachmentCache(graph);
        cache.collectIds(elements);

        if (!cache.needAttachment())
            return;

        cache.fill();

        final Map<?, ?> copy = new HashMap<>(elements);
        for (final Map.Entry entry : copy.entrySet()) {
            final Object newKey = cache.get(entry.getKey());
            Object newValue = entry.getValue();
            if (newValue instanceof Traverser) {
                ((Traverser) newValue).asAdmin().set(cache.get(((Traverser) newValue).get()));
                ((Traverser) newValue).asAdmin().setSideEffects(traversalSideEffects);
            } else if (newValue instanceof List) {
                ((List) newValue).replaceAll(object -> cache.get(object));
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
        final AttachmentCache cache = new AttachmentCache(graph);

        traversers.forEach(traverser -> cache.collectIds(traverser));

        if (!cache.needAttachment())
            return;

        cache.fill();

        traversers.forEach(traverser -> {
            if (traverser.get() instanceof Element || traverser.get() instanceof Property) {
                final Object newValue = cache.get(traverser.get());
                traverser.set(newValue);
            } else if (traverser.get() instanceof BulkSet) {
                final BulkSet attached = new BulkSet();
                ((BulkSet) traverser.get()).forEach((element, bulk) ->
                        attached.add(cache.get(element), (long) bulk)
                );
                traverser.set(attached);
            } else if (traverser.get() instanceof Map) {
                final HashMap attached = new HashMap();
                ((HashMap) traverser.get()).forEach((key, value) ->
                        attached.put(cache.get(key), cache.get(value))
                );
                traverser.set(attached);
            } else if (traverser.get() instanceof Path) {
                final Path attached = cache.buildPath((Path) traverser.get());
                traverser.set(attached);
            }

            traverser.setSideEffects(traversalSideEffects);

            cache.attachPath(traverser);

            if (traverser instanceof ProjectedTraverser) {
                final List original = ((ProjectedTraverser) traverser).getProjections();
                final List copy = new ArrayList<>(original);
                for (int i = 0; i < copy.size(); i++) {
                    final Object newValue = cache.get(copy.get(i));
                    if (copy.get(i) != newValue) {
                        original.remove(i);
                        original.add(i, newValue);
                    }
                }
            }
        });
    }

    public static void makeDetachedElements(final FireflyGraph graph,
                                            final TraverserSet<Object> traversers) {
        // no need to make detached elements if already
        boolean allDetached = true;
        for (final Traverser element : traversers) {
            if (!(element.get() instanceof DetachedVertex)) {
                allDetached = false;
                break;
            }
        }

        if (allDetached) return;

        bulkAttach(graph, EmptyTraversalSideEffects.instance(), traversers);
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
            final Map<Object, Object> map = new HashMap<>();
            for (final Map.Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
                Object entryValue = entry.getValue();
                if (entryValue instanceof Map || entryValue instanceof TraverserSet) {
                    entryValue = detach(entry.getValue(), detacher);
                } else if (entry.getValue() instanceof Traverser) {
                    final Traverser.Admin traverser = ((Traverser) entry.getValue()).asAdmin();
                    // used only by barriers, so it's ok to always use default traverser.detach here
                    // final Object detached = detacher.apply(traverser.get());
                    traverser.detach();
                    // traverser.set(detached);
                } else
                    entryValue = detacher.apply(entryValue);

                map.put(detacher.apply(entry.getKey()), entryValue);
            }
            return map;
        }

        // TraverserSet detached as HashSet, so need to avoid this
        return detacher.apply(value);
    }
}
