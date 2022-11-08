package com.aerospike.firefly.util;

import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.io.Util.verifyClean;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Util {
    private static final Logger LOG = LoggerFactory.getLogger(Util.class);

    public static void clearGraph(final FireflyGraph graph) {
        graph.traversal().V().drop().iterate();
        verifyClean(graph);
    }
}
