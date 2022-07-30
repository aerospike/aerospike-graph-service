package com.aerospike.firefly.io;

import com.aerospike.firefly.structure.FireflyGraph;

import static org.junit.Assert.fail;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Util {
    public static void verifyClean(FireflyGraph graph){
        AerospikeConnection db = graph.getBaseGraph();
        if (graph.traversal().V().count().next() > 0 )
            fail("nonzero vertex count after drop operation");
        if (graph.traversal().E().count().next() > 0)
            fail("nonzero edge count after drop operation");
        if(db.scanAllRecordsInSet(db.VERTEX_AERO_SET).hasNext())
            fail("nonzero vertex aero set after drop");
        if(db.scanAllRecordsInSet(db.EDGE_AERO_SET).hasNext())
            fail("nonzero vertex aero set after drop");
        if(db.scanAllRecordsInSet(db.VERTEX_PROPERTY_AERO_SET).hasNext())
            fail("nonzero vertex aero set after drop");
        if(db.scanAllRecordsInSet(db.VERTEX_EDGELIST_AERO_SET).hasNext())
            fail("nonzero vertex aero set after drop");
    }
}
