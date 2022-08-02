package com.aerospike.firefly.util;

import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.LoggerFactory;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Util {
    public static void clearGraph(FireflyGraph graph) {
        if (graph.traversal().V().count().next() > 0 || graph.traversal().E().count().next() > 0)
            LoggerFactory.getLogger("clearGraph").warn("nonzero vertex or edge count at start of test");
        graph.traversal().V().drop().iterate();
//        if (graph.traversal().V().count().next() > 0 || graph.traversal().E().count().next() > 0)
//            throw new RuntimeException("nonzero vertex or edge count after drop operation");
    }
}
