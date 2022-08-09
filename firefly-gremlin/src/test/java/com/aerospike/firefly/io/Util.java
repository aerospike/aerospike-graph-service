package com.aerospike.firefly.io;

import com.aerospike.firefly.structure.FireflyGraph;

import static org.junit.Assert.fail;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Util {
    public static void verifyClean(final FireflyGraph graph){
        if (graph.traversal().V().count().next() > 0 )
            fail("nonzero vertex count after drop operation");
        if (graph.traversal().E().count().next() > 0)
            fail("nonzero edge count after drop operation");
    }
}
