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

package com.aerospike.firefly.process.call.cache;

import com.aerospike.firefly.io.aerospike.CacheManager.CacheMode;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * Integration tests for cache management services.
 */
public class CacheServiceTest extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Before
    public void resetConfigBeforeTest() {
        // Reset persisted configuration and cache mode to TRANSACTIONAL before each test
        graph.getBaseGraph().resetConfiguration();
    }

    @After
    public void resetCacheMode() {
        // Reset persisted configuration and cache mode to TRANSACTIONAL after each test
        graph.getBaseGraph().resetConfiguration();
    }

    @Test
    public void testCacheServiceStatusEmpty() {
        final GraphTraversalSource g = graph.traversal();

        // Get cache status when cache is empty
        final Map<String, Object> status = (Map<String, Object>) g.call("aerospike.graph.admin.cache.status").next();

        Assert.assertNotNull(status);
        Assert.assertEquals("TRANSACTIONAL", status.get("mode"));
        Assert.assertEquals(1_000_000L, status.get("cache_weight"));
        Assert.assertEquals(0L, status.get("estimated_entry_count"));
        Assert.assertEquals(0L, status.get("weighted_size"));
        Assert.assertEquals(0L, status.get("estimated_memory_bytes"));
        Assert.assertEquals("0 B", status.get("estimated_memory_formatted"));
    }

    @Test
    public void testCacheServiceStatusAfterTraversal() {
        final GraphTraversalSource g = graph.traversal();

        // Use GLOBAL mode so cache persists between traversals
        g.call("aerospike.graph.admin.cache.set-mode").with("mode", "GLOBAL").next();

        // Populate data and execute traversal
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);
        g.V().has("name", "marko").out().local(__.out().count()).toList();

        // Get cache status after traversal
        final Map<String, Object> status = (Map<String, Object>) g.call("aerospike.graph.admin.cache.status").next();

        Assert.assertNotNull(status);
        Assert.assertEquals("GLOBAL", status.get("mode"));
        final long entryCount = (Long) status.get("estimated_entry_count");
        Assert.assertTrue("Entry count should be > 0", entryCount > 0);
    }

    @Test
    public void testCacheServiceSetModeToGlobal() {
        final GraphTraversalSource g = graph.traversal();

        // Set mode to GLOBAL
        final Map<String, Object> result = (Map<String, Object>) g.call("aerospike.graph.admin.cache.set-mode")
                .with("mode", "GLOBAL")
                .next();

        Assert.assertNotNull(result);
        Assert.assertEquals("success", result.get("status"));
        Assert.assertEquals("TRANSACTIONAL", result.get("previous_mode"));
        Assert.assertEquals("GLOBAL", result.get("current_mode"));
        Assert.assertEquals(20_000_000L, result.get("cache_weight")); // Default for GLOBAL

        // Verify via status
        final Map<String, Object> status = (Map<String, Object>) g.call("aerospike.graph.admin.cache.status").next();
        Assert.assertEquals("GLOBAL", status.get("mode"));
        Assert.assertEquals(20_000_000L, status.get("cache_weight"));
    }

    @Test
    public void testCacheServiceSetModeToTransactional() {
        final GraphTraversalSource g = graph.traversal();

        // First switch to GLOBAL
        g.call("aerospike.graph.admin.cache.set-mode").with("mode", "GLOBAL").next();

        // Then switch back to TRANSACTIONAL
        final Map<String, Object> result = (Map<String, Object>) g.call("aerospike.graph.admin.cache.set-mode")
                .with("mode", "TRANSACTIONAL")
                .next();

        Assert.assertNotNull(result);
        Assert.assertEquals("success", result.get("status"));
        Assert.assertEquals("GLOBAL", result.get("previous_mode"));
        Assert.assertEquals("TRANSACTIONAL", result.get("current_mode"));
        Assert.assertEquals(1_000_000L, result.get("cache_weight")); // Default for TRANSACTIONAL
    }

    @Test
    public void testCacheServiceSetModeWithCustomWeight() {
        final GraphTraversalSource g = graph.traversal();

        // Set mode to GLOBAL with custom weight (50 million)
        final Map<String, Object> result = (Map<String, Object>) g.call("aerospike.graph.admin.cache.set-mode")
                .with("mode", "GLOBAL")
                .with("cache_weight", "50000000")
                .next();

        Assert.assertNotNull(result);
        Assert.assertEquals("success", result.get("status"));
        Assert.assertEquals("GLOBAL", result.get("current_mode"));
        Assert.assertEquals(50_000_000L, result.get("cache_weight"));

        // Verify via status
        final Map<String, Object> status = (Map<String, Object>) g.call("aerospike.graph.admin.cache.status").next();
        Assert.assertEquals("GLOBAL", status.get("mode"));
        Assert.assertEquals(50_000_000L, status.get("cache_weight"));
    }

    @Test
    public void testCacheServiceSetModeCaseInsensitive() {
        final GraphTraversalSource g = graph.traversal();

        // Test lowercase
        Map<String, Object> result = (Map<String, Object>) g.call("aerospike.graph.admin.cache.set-mode")
                .with("mode", "global")
                .next();
        Assert.assertEquals("GLOBAL", result.get("current_mode"));

        // Test mixed case
        result = (Map<String, Object>) g.call("aerospike.graph.admin.cache.set-mode")
                .with("mode", "Transactional")
                .next();
        Assert.assertEquals("TRANSACTIONAL", result.get("current_mode"));
    }

    @Test
    public void testCacheModeChangeDoesNotAffectOtherGraphs() {
        final GraphTraversalSource g1 = graph.traversal();

        g1.call("aerospike.graph.admin.cache.set-mode").with("mode", "GLOBAL").next();

        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);
        g1.V().toList();

        final Map<String, Object> statusBefore = (Map<String, Object>) g1.call("aerospike.graph.admin.cache.status").next();
        Assert.assertEquals("GLOBAL", statusBefore.get("mode"));
        Assert.assertTrue("Global cache should have entries", (Long) statusBefore.get("estimated_entry_count") > 0);

        try (FireflyGraph graph2 = FireflyGraph.open(config)) {
            final GraphTraversalSource g2 = graph2.traversal();
            g2.call("aerospike.graph.admin.cache.set-mode").with("mode", "TRANSACTIONAL").next();

            final Map<String, Object> statusAfter = (Map<String, Object>) g1.call("aerospike.graph.admin.cache.status").next();
            Assert.assertEquals("GLOBAL", statusAfter.get("mode"));
            Assert.assertTrue("Global cache should remain populated", (Long) statusAfter.get("estimated_entry_count") > 0);
        }
    }

    @Test
    public void testCacheServiceResetTransactional() {
        final GraphTraversalSource g = graph.traversal();

        // In TRANSACTIONAL mode, each call() resets the cache, so the reset service
        // simply confirms it can reset/reinitialize the cache.
        // Reset cache
        final Map<String, Object> result = (Map<String, Object>) g.call("aerospike.graph.admin.cache.reset").next();

        Assert.assertNotNull(result);
        Assert.assertEquals("success", result.get("status"));
        Assert.assertEquals("TRANSACTIONAL", result.get("mode"));
        // In TRANSACTIONAL mode, cache was already reset by the call() itself
        Assert.assertEquals(0L, result.get("current_entry_count"));
        Assert.assertEquals(0L, result.get("current_weighted_size"));
    }

    @Test
    public void testCacheServiceResetGlobal() {
        final GraphTraversalSource g = graph.traversal();

        // Switch to GLOBAL mode
        g.call("aerospike.graph.admin.cache.set-mode").with("mode", "GLOBAL").next();

        // Populate cache
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);
        g.V().has("name", "marko").out().local(__.out().count()).toList();

        // Verify cache is populated
        Map<String, Object> status = (Map<String, Object>) g.call("aerospike.graph.admin.cache.status").next();
        final long populatedCount = (Long) status.get("estimated_entry_count");
        Assert.assertTrue("Cache should have entries", populatedCount > 0);

        // Reset cache
        final Map<String, Object> result = (Map<String, Object>) g.call("aerospike.graph.admin.cache.reset").next();

        Assert.assertNotNull(result);
        Assert.assertEquals("success", result.get("status"));
        Assert.assertEquals("GLOBAL", result.get("mode"));
        Assert.assertEquals(populatedCount, result.get("previous_entry_count"));
        Assert.assertEquals(0L, result.get("current_entry_count"));
    }

    @Test
    public void testCacheServiceStatusWithGlobalCache() {
        final GraphTraversalSource g = graph.traversal();

        // Switch to GLOBAL mode
        g.call("aerospike.graph.admin.cache.set-mode").with("mode", "GLOBAL").next();

        // Get status
        Map<String, Object> status = (Map<String, Object>) g.call("aerospike.graph.admin.cache.status").next();
        Assert.assertEquals("GLOBAL", status.get("mode"));
        Assert.assertEquals(20_000_000L, status.get("cache_weight"));
        Assert.assertEquals(0L, status.get("estimated_entry_count"));

        // Populate data and execute traversal
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);
        g.V().has("name", "marko").out().local(__.out().count()).toList();

        // Get status after traversal
        status = (Map<String, Object>) g.call("aerospike.graph.admin.cache.status").next();
        Assert.assertEquals("GLOBAL", status.get("mode"));
        final long entryCount = (Long) status.get("estimated_entry_count");
        Assert.assertTrue("Entry count should be > 0", entryCount > 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCacheServiceSetModeInvalidMode() {
        final GraphTraversalSource g = graph.traversal();
        g.call("aerospike.graph.admin.cache.set-mode")
                .with("mode", "INVALID")
                .next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCacheServiceSetModeMissingParameter() {
        final GraphTraversalSource g = graph.traversal();
        g.call("aerospike.graph.admin.cache.set-mode").next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCacheServiceSetModeInvalidWeight() {
        final GraphTraversalSource g = graph.traversal();
        g.call("aerospike.graph.admin.cache.set-mode")
                .with("mode", "GLOBAL")
                .with("cache_weight", "not_a_number")
                .next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCacheServiceSetModeNegativeWeight() {
        final GraphTraversalSource g = graph.traversal();
        g.call("aerospike.graph.admin.cache.set-mode")
                .with("mode", "GLOBAL")
                .with("cache_weight", "-1")
                .next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCacheServiceStatusWithExtraParameters() {
        final GraphTraversalSource g = graph.traversal();
        g.call("aerospike.graph.admin.cache.status")
                .with("extra", "param")
                .next();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCacheServiceResetWithExtraParameters() {
        final GraphTraversalSource g = graph.traversal();
        g.call("aerospike.graph.admin.cache.reset")
                .with("extra", "param")
                .next();
    }

    @Test
    public void testGlobalCachePersistsBetweenTraversals() {
        final GraphTraversalSource g = graph.traversal();

        // Populate data
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        // Switch to GLOBAL mode
        g.call("aerospike.graph.admin.cache.set-mode").with("mode", "GLOBAL").next();

        // First traversal
        g.V().has("name", "marko").out().local(__.out().count()).toList();
        Map<String, Object> status1 = (Map<String, Object>) g.call("aerospike.graph.admin.cache.status").next();
        final long count1 = (Long) status1.get("estimated_entry_count");

        // Second traversal (same data should still be cached)
        g.V().has("name", "marko").out().local(__.out().count()).toList();
        Map<String, Object> status2 = (Map<String, Object>) g.call("aerospike.graph.admin.cache.status").next();
        final long count2 = (Long) status2.get("estimated_entry_count");

        // Entry count should not decrease (and might not increase much if same data)
        Assert.assertTrue("Entry count should be maintained", count2 >= count1);
    }

    @Test
    public void testSwitchModesClearsCache() {
        final GraphTraversalSource g = graph.traversal();

        // Populate data
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        // Switch to GLOBAL mode and populate cache
        g.call("aerospike.graph.admin.cache.set-mode").with("mode", "GLOBAL").next();
        g.V().has("name", "marko").out().local(__.out().count()).toList();

        Map<String, Object> statusGlobal = (Map<String, Object>) g.call("aerospike.graph.admin.cache.status").next();
        Assert.assertTrue("Global cache should have entries", (Long) statusGlobal.get("estimated_entry_count") > 0);

        // Switch back to TRANSACTIONAL - cache should be cleared
        g.call("aerospike.graph.admin.cache.set-mode").with("mode", "TRANSACTIONAL").next();
        Map<String, Object> statusTxn = (Map<String, Object>) g.call("aerospike.graph.admin.cache.status").next();
        Assert.assertEquals("TRANSACTIONAL", statusTxn.get("mode"));
        // Transactional cache is not initialized yet, so count should be 0
        Assert.assertEquals(0L, statusTxn.get("estimated_entry_count"));
    }

    @Test
    public void testGlobalModeCachesAllVertices() {
        final GraphTraversalSource g = graph.traversal();

        // Populate data - TinkerFactory modern graph has 6 vertices
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        // Switch to GLOBAL mode
        g.call("aerospike.graph.admin.cache.set-mode").with("mode", "GLOBAL").next();

        // Execute traversal that touches all vertices
        // g.V().both().both().path().count() traverses all vertices and produces 30 paths
        final Long pathCount = g.V().both().both().path().count().next();
        Assert.assertEquals("Path count should be 30", 30L, pathCount.longValue());

        // Verify cache contains at least all 6 vertices (may also contain config records)
        final Map<String, Object> status = (Map<String, Object>) g.call("aerospike.graph.admin.cache.status").next();
        Assert.assertEquals("GLOBAL", status.get("mode"));
        final long entryCount = (Long) status.get("estimated_entry_count");
        Assert.assertTrue("Cache should contain at least 6 vertices", entryCount >= 6L);
    }

    @Test
    public void testTransactionalModeCachesAllVertices() {
        final GraphTraversalSource g = graph.traversal();

        // Populate data - TinkerFactory modern graph has 6 vertices
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        // Ensure we're in TRANSACTIONAL mode
        Assert.assertEquals(CacheMode.TRANSACTIONAL, graph.getBaseGraph().cacheManager.getCacheMode());

        // Execute traversal that touches all vertices
        // g.V().both().both().path().count() traverses all vertices and produces 30 paths
        final Long pathCount = g.V().both().both().path().count().next();
        Assert.assertEquals("Path count should be 30", 30L, pathCount.longValue());

        // In TRANSACTIONAL mode, verify cache directly (since call() would reset it)
        final long entryCount = graph.getBaseGraph().cacheManager.getTotalEstimatedEntryCount();
        Assert.assertTrue("Cache should contain at least 6 vertices", entryCount >= 6L);
    }
}
