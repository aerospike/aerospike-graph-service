package com.aerospike.firefly.process.traversal.step.util;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinTypeErrorException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.translator.AnonymizingTypeTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.translator.GroovyTranslator;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;

public class TraversalUtil {
    private static final Logger LOG = LoggerFactory.getLogger(TraversalUtil.class);
    private static final LRUTraversalLogCache TRAVERSAL_LOG_CACHE = new LRUTraversalLogCache();

    static public String toStringScript(final Traversal.Admin traversal, final boolean redactLiterals) {
        if (redactLiterals) {
            return GroovyTranslator.of("g", new AnonymizingTypeTranslator()).translate(traversal.getBytecode()).getScript();
        } else {
            return GroovyTranslator.of("g").translate(traversal.getBytecode()).getScript();
        }
    }

    static public void supernodeTraversalWarning(final FireflyGraph graph, final Traversal.Admin traversal,
                                                 final FireflyVertex vertex) {
        if (graph.getBaseGraph().SUPERNODE_COUNTER_ENABLED && vertex.isEdgeCacheOverflowed()) {
            graph.incrementSupernodesTraversed();
        }
        if (graph.getBaseGraph().SUPERNODE_TRAVERSAL_LOG_WARNING && vertex.isEdgeCacheOverflowed()) {
            final String traversalString = toStringScript(traversal, graph.getBaseGraph().REDACT_SCRIPT_LITERALS_ENABLED);
            if (!TRAVERSAL_LOG_CACHE.contains(traversalString)) {
                final String message = "The traversal, \"" + traversalString +
                        "\", walks over the Edges of an existing supernode in the Graph which may cause unexpected performance.\n" +
                        "Consider adjusting the traversal to filter out supernode Vertices or adding filters to the Edges if required.\n" +
                        "ID of first supernode Vertex encountered by this traversal: " + vertex.id();
                LOG.warn(message);
            }
        }
    }

    /**
     * Custom override of HasContainer.TestAll to fix it throwing GremlinTypeErrorException instead of returning false
     * in the case of multi-properties.
     * @param element       Element to test HasContainer list against
     * @param hasContainers List of HasContainer
     * @return              True if Element satisfies all HasContainer conditions
     * @param <S>           Type of Element
     */
    static public <S> boolean fireflyTestAll(final S element, final List<HasContainer> hasContainers) {
        final boolean isProperty = element instanceof Property;
        for (final HasContainer hasContainer : hasContainers) {
            if (isProperty) {
                if (!hasContainer.test((Property) element)) {
                    return false;
                }
            } else {
                if (!fireflyHasContainerTest(hasContainer, (Element) element)) {
                    return false;
                }
            }
        }
        return true;
    }

    static private boolean fireflyHasContainerTest(final HasContainer hasContainer, final Element element) {
        final String key = hasContainer.getKey();
        if (key != null) {
            if (key.equals(T.id.getAccessor()) || key.equals(T.label.getAccessor())) {
                return hasContainer.test(element);
            }
        }

        // it is OK to evaluate equality of ids via toString(), given that the test suite enforces the value of
        // id().toString() to be a first class representation of the identifier. a string test is only executed
        // if the predicate value is a String.  this allows stuff like: g.V().has(id,lt(10)) to work properly
        final Iterator<? extends Property> itty = element.properties(key);
        try {
            while (itty.hasNext()) {
                final Property property = itty.next();
                final Predicate<Object> predicate = (Predicate<Object>) hasContainer.getPredicate();
                try {
                    if (predicate.test(property.value())) {
                        return true;
                    }
                } catch (final GremlinTypeErrorException ignored) {
                    // If the type mismatches, then just move on to the next property to compare the value.
                }
            }
        } finally {
            CloseableIterator.closeIterator(itty);
        }
        return false;
    }

    private static class LRUTraversalLogCache {
        /**
         * A minimalist LRU cache for traversal logging used to reduce repeat messages.
         */
        private static final int SIZE = 10;
        private final LinkedList<String> cacheList = new LinkedList<>();

        synchronized public boolean contains(final String traversal) {
            if (this.cacheList.contains(traversal)) {
                this.cacheList.remove(traversal);
                this.cacheList.addFirst(traversal);
                return true;
            } else {
                this.cacheList.addFirst(traversal);
                if (this.cacheList.size() > SIZE) {
                    this.cacheList.removeLast();
                }
                return false;
            }
        }
    }
}
