package com.aerospike.firefly.process.traversal.step.util;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.translator.AnonymizingTypeTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.translator.GroovyTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

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
