package com.aerospike.firefly.io.cache;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

public class TestSupernodeFlagCacheDisable extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testSupernodeFlagSetAfterVertexCreation() {
        final GraphTraversalSource g = graph.traversal();

        // Create vertex.
        FireflyVertex v = (FireflyVertex) g.addV("test").next();

        // Validate vertex supernode flag is not set.
        final Key key = FireflyRecord.getKey(graph.getBaseGraph(), graph.getBaseGraph().VERTEX_AERO_SET, v.id);
        Record r = graph.getBaseGraph().read(key, null);
        Assert.assertFalse(v.isEdgeCacheOverflowed());
        Assert.assertFalse(r.getBoolean(db.EDGE_CACHE_DISABLED_BIN));

        // Set supernode flag and grab vertex.
        v = (FireflyVertex) g.V(v.id()).property("~supernode", true).next();

        // Validate cache overflowed flag is set.
        Assert.assertTrue(v.isEdgeCacheOverflowed());
        r = graph.getBaseGraph().read(key, null);
        Assert.assertTrue(r.getBoolean(db.EDGE_CACHE_DISABLED_BIN));

        // Grab vertex through id.
        v = (FireflyVertex) g.V(v.id()).next();

        // Validate cache overflowed flag is set.
        Assert.assertTrue(v.isEdgeCacheOverflowed());
        r = graph.getBaseGraph().read(key, null);
        Assert.assertTrue(r.getBoolean(db.EDGE_CACHE_DISABLED_BIN));
    }

    @Test
    public void testSupernodeFlagSetOnVertexCreation() {
        final GraphTraversalSource g = graph.traversal();

        // Create vertex.
        FireflyVertex v = (FireflyVertex) g.addV("test").property("~supernode", true).next();

        // Validate vertex supernode flag is set.
        final Key key = FireflyRecord.getKey(graph.getBaseGraph(), graph.getBaseGraph().VERTEX_AERO_SET, v.id);
        Record r = graph.getBaseGraph().read(key, null);
        Assert.assertTrue(v.isEdgeCacheOverflowed());
        Assert.assertTrue(r.getBoolean(db.EDGE_CACHE_DISABLED_BIN));

        v = (FireflyVertex) g.V().hasLabel("test").next();

        // Validate vertex supernode flag is set.
        r = graph.getBaseGraph().read(key, null);
        Assert.assertTrue(v.isEdgeCacheOverflowed());
        Assert.assertTrue(r.getBoolean(db.EDGE_CACHE_DISABLED_BIN));
    }
}
