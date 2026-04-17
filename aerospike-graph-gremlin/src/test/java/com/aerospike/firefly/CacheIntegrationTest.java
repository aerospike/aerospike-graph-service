/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.aerospike.CacheManager.CacheMode;
import com.aerospike.firefly.io.aerospike.ReadThroughRecordCache;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

public class CacheIntegrationTest extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @After
    public void resetCacheMode() {
        // Ensure cache mode is reset to TRANSACTIONAL after each test
        if (graph.getBaseGraph().cacheManager.getCacheMode() != CacheMode.TRANSACTIONAL) {
            graph.getBaseGraph().cacheManager.setCacheMode(db, CacheMode.TRANSACTIONAL);
        }
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
    public void testCacheBatchHitMissManual() {
        final UUID uuid = UUID.randomUUID();
        final UUID uuid2 = UUID.randomUUID();
        final Key key = new Key("test", "test", uuid.toString());
        final Bin bin = new Bin("test", "test");
        final Key key2 = new Key("test", "test", uuid2.toString());
        final Bin bin2 = new Bin("test", "test2");
        db.checkedPut(null, key, bin);
        db.checkedPut(null, key2, bin2);
        Key[] keys = new Key[2];
        keys[0] = key;
        keys[1] = key2;
        final FireflyCache cache = new ReadThroughRecordCache(db, uuid);
        final Record[] misses = cache.read(keys, null);
        Assert.assertNotNull(misses);
        Assert.assertTrue(
                (misses[0].getString("test").equals("test") || misses[0].getString("test").equals("test2")) &&
                        (misses[1].getString("test").equals("test") || misses[1].getString("test").equals("test2"))
        );
        Assert.assertEquals(0, cache.getHitCount());
        Assert.assertEquals(2, cache.getMissCount());
        final Record[] hits = cache.read(keys, null);
        Assert.assertNotNull(hits);
        Assert.assertTrue(
                (hits[0].getString("test").equals("test") || hits[0].getString("test").equals("test2")) &&
                        (hits[1].getString("test").equals("test") || hits[1].getString("test").equals("test2"))
        );
        Assert.assertEquals(2, cache.getHitCount());
        Assert.assertEquals(2, cache.getMissCount());
        Assert.assertArrayEquals(misses, hits);
    }

    @Test
    public void testCacheIntegration() {
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);
        // Start on marko, received via scan (so not cached, also not a hit or miss).
        // out to lop/josh/vadas (3 misses).
        // in to marko x3, josh, peter (miss on peter/marko (x3 will be optimized to read once), hit on josh).
        // Total: 5 misses, 1 hit.
        // out().count() to force reading vertices with in/out edges
        graph.traversal().V().has("name", "marko").out().in().local(__.out().count()).toList();
        Assert.assertEquals(1L, graph.getBaseGraph().cacheManager.getTransactionCache().getHitCount());
        Assert.assertEquals(5L, graph.getBaseGraph().cacheManager.getTransactionCache().getMissCount());
        graph.traversal().V().has("name", "marko").out().in().out().local(__.out().count()).toList();
        // out to vadas, lop, josh, ripple, (3 hits, 1 miss on ripple).
        // Total 6 misses, 4 hits.
        Assert.assertEquals(4L, graph.getBaseGraph().cacheManager.getTransactionCache().getHitCount());
        Assert.assertEquals(6L, graph.getBaseGraph().cacheManager.getTransactionCache().getMissCount());
    }

    @Test
    public void testGlobalCacheHitMissManual() {
        // Switch to global cache mode
        graph.getBaseGraph().cacheManager.setCacheMode(db, CacheMode.GLOBAL);
        Assert.assertEquals(CacheMode.GLOBAL, graph.getBaseGraph().cacheManager.getCacheMode());

        final UUID uuid = UUID.randomUUID();
        final Key key = new Key("test", "test", uuid.toString());
        final Bin bin = new Bin("test", "test");
        db.checkedPut(null, key, bin);

        final FireflyCache cache = graph.getBaseGraph().cacheManager.getTransactionCache();
        Assert.assertNotNull(cache);

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
    public void testGlobalCacheNotResetBetweenTraversals() {
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        // Switch to global cache mode
        graph.getBaseGraph().cacheManager.setCacheMode(db, CacheMode.GLOBAL);
        Assert.assertEquals(CacheMode.GLOBAL, graph.getBaseGraph().cacheManager.getCacheMode());

        // First traversal - same as testCacheIntegration first traversal
        // Start on marko (via scan, not cached).
        // out to lop/josh/vadas (3 misses).
        // in to marko x3, josh, peter (miss on peter/marko, hit on josh).
        // Total: 5 misses, 1 hit.
        graph.traversal().V().has("name", "marko").out().in().local(__.out().count()).toList();
        Assert.assertEquals(1L, graph.getBaseGraph().cacheManager.getTransactionCache().getHitCount());
        Assert.assertEquals(5L, graph.getBaseGraph().cacheManager.getTransactionCache().getMissCount());

        // Second traversal - global cache is NOT reset, so hits accumulate from previous traversal
        // Vertices cached from first traversal: marko, vadas, lop, josh, peter
        // Second traversal accesses same + ripple (1 new miss)
        // out to vadas, lop, josh (all cached - 3 hits), ripple (1 miss)
        // Total accumulated: more hits, 6 misses (5 + 1 new)
        graph.traversal().V().has("name", "marko").out().in().out().local(__.out().count()).toList();
        Assert.assertEquals(6L, graph.getBaseGraph().cacheManager.getTransactionCache().getMissCount());
        // Hits accumulate (previous 1 + new hits from cached vertices)
        Assert.assertTrue("Hits should be > 1 (accumulated)",
                graph.getBaseGraph().cacheManager.getTransactionCache().getHitCount() > 1L);
    }

    @Test
    public void testSwitchFromTransactionalToGlobal() {
        // Verify starting in TRANSACTIONAL mode
        Assert.assertEquals(CacheMode.TRANSACTIONAL, graph.getBaseGraph().cacheManager.getCacheMode());

        // Global cache should be null initially
        graph.getBaseGraph().cacheManager.setCacheMode(db, CacheMode.GLOBAL);

        // After switching to GLOBAL, caches should be initialized
        Assert.assertEquals(CacheMode.GLOBAL, graph.getBaseGraph().cacheManager.getCacheMode());
        Assert.assertNotNull(graph.getBaseGraph().cacheManager.getTransactionCache());
        Assert.assertNotNull(graph.getBaseGraph().cacheManager.getEmptyPropsCache());
    }

    @Test
    public void testSwitchFromGlobalToTransactional() {
        // First switch to GLOBAL mode
        graph.getBaseGraph().cacheManager.setCacheMode(db, CacheMode.GLOBAL);
        Assert.assertEquals(CacheMode.GLOBAL, graph.getBaseGraph().cacheManager.getCacheMode());

        // Populate global cache with some data
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);
        graph.traversal().V().has("name", "marko").out().toList();

        final FireflyCache globalCacheBefore = graph.getBaseGraph().cacheManager.getTransactionCache();
        Assert.assertNotNull(globalCacheBefore);
        Assert.assertTrue("Global cache should have some misses", globalCacheBefore.getMissCount() > 0);

        // Switch back to TRANSACTIONAL - global cache should be cleared
        graph.getBaseGraph().cacheManager.setCacheMode(db, CacheMode.TRANSACTIONAL);
        Assert.assertEquals(CacheMode.TRANSACTIONAL, graph.getBaseGraph().cacheManager.getCacheMode());

        // After switching, transactional cache should be null (not yet initialized)
        Assert.assertNull(graph.getBaseGraph().cacheManager.getTransactionCache());
    }

    @Test
    public void testGlobalCacheSharedAcrossTraversals() {
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        // Switch to global cache mode
        graph.getBaseGraph().cacheManager.setCacheMode(db, CacheMode.GLOBAL);

        // First traversal: out from marko to vadas, lop, josh (3 misses, 0 hits)
        graph.traversal().V().has("name", "marko").out().local(__.out().count()).toList();
        final FireflyCache cacheAfterFirst = graph.getBaseGraph().cacheManager.getTransactionCache();
        Assert.assertEquals(0L, cacheAfterFirst.getHitCount());
        Assert.assertEquals(3L, cacheAfterFirst.getMissCount());

        // Second identical traversal: same 3 vertices are now cached (3 hits, 0 new misses)
        graph.traversal().V().has("name", "marko").out().local(__.out().count()).toList();
        final FireflyCache cacheAfterSecond = graph.getBaseGraph().cacheManager.getTransactionCache();

        // Should be the same cache instance in global mode
        Assert.assertSame("Global cache should be the same instance", cacheAfterFirst, cacheAfterSecond);

        // After second traversal: 3 hits (from cached vadas, lop, josh), still 3 total misses
        Assert.assertEquals(3L, cacheAfterSecond.getHitCount());
        Assert.assertEquals(3L, cacheAfterSecond.getMissCount());
    }

    @Test
    public void testCacheSizeManual() {
        final UUID uuid = UUID.randomUUID();
        final ReadThroughRecordCache cache = new ReadThroughRecordCache(db, uuid);

        // Initially cache should be empty
        Assert.assertEquals(0L, cache.getEstimatedEntryCount());

        // Add first record
        final Key key1 = new Key("test", "test", UUID.randomUUID().toString());
        final Bin bin1 = new Bin("test", "value1");
        db.checkedPut(null, key1, bin1);
        cache.read(null, key1);

        // Cache should have 1 entry
        Assert.assertEquals(1L, cache.getEstimatedEntryCount());

        // Add second record
        final Key key2 = new Key("test", "test", UUID.randomUUID().toString());
        final Bin bin2 = new Bin("test", "value2");
        db.checkedPut(null, key2, bin2);
        cache.read(null, key2);

        // Cache should have 2 entries
        Assert.assertEquals(2L, cache.getEstimatedEntryCount());

        // Verify hit/miss counts are correct
        Assert.assertEquals(0L, cache.getHitCount());
        Assert.assertEquals(2L, cache.getMissCount());
    }

    @Test
    public void testCacheSizeWithGraphTraversal() {
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        // Switch to global cache mode for easier testing (cache persists)
        graph.getBaseGraph().cacheManager.setCacheMode(db, CacheMode.GLOBAL);

        // Initially cache should be empty
        Assert.assertEquals(0L, graph.getBaseGraph().cacheManager.getTotalEstimatedEntryCount());
        Assert.assertEquals(0L, graph.getBaseGraph().cacheManager.getTotalWeightedSize());

        // Execute traversal to populate cache
        graph.traversal().V().has("name", "marko").out().local(__.out().count()).toList();

        // Cache should now have entries (3 vertices: vadas, lop, josh)
        final long entryCount = graph.getBaseGraph().cacheManager.getTotalEstimatedEntryCount();
        final long weightedSize = graph.getBaseGraph().cacheManager.getTotalWeightedSize();
        final long memoryBytes = graph.getBaseGraph().cacheManager.getTotalEstimatedMemoryUsageBytes();
        final String memoryFormatted = graph.getBaseGraph().cacheManager.getTotalEstimatedMemoryUsageFormatted();

        Assert.assertTrue("Entry count should be >= 3", entryCount >= 3);
        Assert.assertTrue("Weighted size should be > 0", weightedSize > 0);
        Assert.assertTrue("Memory usage should be > 0", memoryBytes > 0);
        Assert.assertNotNull("Formatted memory should not be null", memoryFormatted);
        Assert.assertFalse("Formatted memory should not be empty", memoryFormatted.isEmpty());

        // Execute another traversal to add more entries
        graph.traversal().V().has("name", "marko").out().in().local(__.out().count()).toList();

        // Cache should have more entries now
        Assert.assertTrue("Entry count should increase after more traversals",
                graph.getBaseGraph().cacheManager.getTotalEstimatedEntryCount() > entryCount);
        Assert.assertTrue("Weighted size should increase",
                graph.getBaseGraph().cacheManager.getTotalWeightedSize() > weightedSize);
    }

    @Test
    public void testCacheSizeAfterInvalidation() {
        final UUID uuid = UUID.randomUUID();
        final ReadThroughRecordCache cache = new ReadThroughRecordCache(db, uuid);

        // Add records
        final Key key1 = new Key("test", "test", UUID.randomUUID().toString());
        final Key key2 = new Key("test", "test", UUID.randomUUID().toString());
        db.checkedPut(null, key1, new Bin("test", "value1"));
        db.checkedPut(null, key2, new Bin("test", "value2"));
        cache.read(null, key1);
        cache.read(null, key2);

        Assert.assertEquals(2L, cache.getEstimatedEntryCount());

        // Invalidate one key
        cache.invalidate(key1);

        // Entry count should decrease
        Assert.assertEquals(1L, cache.getEstimatedEntryCount());

        // Invalidate all
        cache.invalidateAll();
        Assert.assertEquals(0L, cache.getEstimatedEntryCount());
    }

    @Test
    public void testMemoryUsageFormattedOutput() {
        // Test the formatted output produces reasonable strings
        graph.getBaseGraph().cacheManager.setCacheMode(db, CacheMode.GLOBAL);

        // With empty cache
        final String emptyFormatted = graph.getBaseGraph().cacheManager.getTotalEstimatedMemoryUsageFormatted();
        Assert.assertEquals("0 B", emptyFormatted);

        // Populate with some data
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);
        graph.traversal().V().toList();

        final String populatedFormatted = graph.getBaseGraph().cacheManager.getTotalEstimatedMemoryUsageFormatted();
        Assert.assertNotNull(populatedFormatted);
        // Should contain a unit (B, KB, MB, or GB)
        Assert.assertTrue("Formatted string should contain a unit",
                populatedFormatted.contains("B") || populatedFormatted.contains("KB") ||
                populatedFormatted.contains("MB") || populatedFormatted.contains("GB"));
    }
}
