package com.aerospike.firefly.process.traversal.step.util;

import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.translator.GroovyTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraversalUtil {
    private static final Logger LOG = LoggerFactory.getLogger(TraversalUtil.class);
    static public String toStringScript(final Traversal.Admin traversal) {
        return GroovyTranslator.of("g").translate(traversal.getBytecode()).getScript();
    }

    static public void supernodeTraversalWarning(final Traversal.Admin traversal, final FireflyVertex vertex) {
        if (vertex.isEdgeCacheOverflowed()) {
            final String message = "The Edges of a supernode Vertex with ID " + vertex.id() +
                    " are being traversed and may cause unexpected performance for the traversal: \n" +
                    toStringScript(traversal) + " \nConsider adjusting the traversal to filter out supernode Vertices or adding filters to the Edges if required.";
            LOG.warn(message);
        }
    }
}
