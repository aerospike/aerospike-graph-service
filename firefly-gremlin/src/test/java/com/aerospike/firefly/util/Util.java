package com.aerospike.firefly.util;

import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Util {
    private static final Logger LOG = LoggerFactory.getLogger(Util.class);

    public static void clearGraph(final FireflyGraph graph) {
        graph.traversal().V().drop().iterate();
        final long vertexCount = graph.traversal().V().count().next();
        if (vertexCount > 0) {
            final var vertices = graph.traversal().V();
            while (true) {
                try {
                    LOG.error("Found non-dropped vertex: " + vertices.next().toString());
                } catch (final NoSuchElementException e) {
                    LOG.error("Done listing non-dropped vertices");
                    break;
                }
            }
            throw new RuntimeException("Non-zero vertex count after drop operation: " + vertexCount);
        }
        final long edgeCount = graph.traversal().E().count().next();
        if (edgeCount > 0) {
            final var edges = graph.traversal().E();
            while (true) {
                try {
                    LOG.error("Found non-dropped edge: " + edges.next().toString());
                } catch (final NoSuchElementException e) {
                    LOG.error("Done listing non-dropped edges");
                    break;
                }
            }
            throw new RuntimeException("Non-zero edge count after drop operation: " + edgeCount);
        }
    }
}
