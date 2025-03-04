package com.aerospike.firefly;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.aerospike.ReadThroughRecordCache;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

public class CacheInterationTest extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testTransactionCacheStep() {
        GraphTraversalSource g = graph.traversal();

        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        // This test tests the interaction between the cache step and the aerospike hasContainer pushdown.
        // If the aerospike hasContainers are pushed down but the read through cache gets read from first
        // and the hasContainers are not applied since the cache is read instead, then the results will
        // be incorrect.
        //
        // This is because has('name', 'vadas') is applied at the end of the traversal after the step out,
        // so it should be pushed to aerospike, however since we previously went out and in, the data needed
        // is cached, so we don't to aerospike for it.
        final List<Vertex> vertexList = g.V().has("name", "marko").out().in().has("name", "marko").out().toList();
        Assert.assertEquals(9, vertexList.size());
        final List<Vertex> vertexList2 = g.V().has("name", "marko").out().in().has("name", "marko").out().has("name", "vadas").toList();
        Assert.assertEquals(3, vertexList2.size());
    }

    @Test
    public void testCacheHitMissManual() {
        final UUID uuid = UUID.randomUUID();
        final Key key = new Key("test", "test", uuid.toString());
        final Bin bin = new Bin("test", "test");
        db.checkedPut(null, key, bin);
        final FireflyCache cache = new ReadThroughRecordCache(db, uuid);
        final Record miss = cache.read(null, key);
        Assert.assertNotNull(miss);
        Assert.assertEquals("test", miss.getString("test"));
        Assert.assertEquals(0, cache.getHitCount());
        Assert.assertEquals(1, cache.getMissCount());
        final Record hit = cache.read(null, key);
        Assert.assertNotNull(hit);
        Assert.assertEquals("test", hit.getString("test"));
        Assert.assertEquals(1, cache.getHitCount());
        Assert.assertEquals(1, cache.getMissCount());
        Assert.assertEquals(miss, hit);
    }

    @Test
    public void testCacheIntegration() {
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);
        graph.traversal().V().has("name", "marko").out().in().toList();
        Assert.assertEquals(1L, graph.getBaseGraph().transactionCache.get().getHitCount());
        Assert.assertEquals(5L, graph.getBaseGraph().transactionCache.get().getMissCount());
        graph.traversal().V().has("name", "marko").out().in().toList();
        Assert.assertEquals(1L, graph.getBaseGraph().transactionCache.get().getHitCount());
        Assert.assertEquals(5L, graph.getBaseGraph().transactionCache.get().getMissCount());
        graph.traversal().V().has("name", "marko").out().in().toList();
        Assert.assertEquals(1L, graph.getBaseGraph().transactionCache.get().getHitCount());
        Assert.assertEquals(5L, graph.getBaseGraph().transactionCache.get().getMissCount());
        graph.traversal().V().has("name", "marko").out().in().toList();
        Assert.assertEquals(1L, graph.getBaseGraph().transactionCache.get().getHitCount());
        Assert.assertEquals(5L, graph.getBaseGraph().transactionCache.get().getMissCount());
    }
}
