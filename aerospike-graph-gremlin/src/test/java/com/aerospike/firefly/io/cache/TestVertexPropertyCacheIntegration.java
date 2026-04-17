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

package com.aerospike.firefly.io.cache;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestVertexPropertyCacheIntegration {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;

    @BeforeClass
    public static void beforeAll() {
        SETUP_GRAPH = FireflyGraph.open(CONFIG);
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
    }

    @AfterClass
    public static void afterAll() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
        SETUP_GRAPH.close();
    }

    @Before
    public void beforeEach() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
    }

    @Test
    public void testEdgeCache() {
        try (final FireflyGraph graph = CacheTestsUtils.getCacheDisabledFirefly()) {
            assertAddAndDropVertexProperties(graph);
        }
    }

    @Test
    public void testCacheSizeExceeded() {
        try (final FireflyGraph graph = CacheTestsUtils.getCacheWithSizeFirefly(2)) {
            assertAddAndDropVertexProperties(graph);
        }
    }

    private void assertAddAndDropVertexProperties(final FireflyGraph graph) {
        final GraphTraversalSource g = graph.traversal();

        g.addV("cat").property("name", "Vincent").iterate();
        Assert.assertTrue(g.V().hasLabel("cat").hasNext());

        g.V().hasLabel("cat").property("legs", "four").iterate();
        Assert.assertTrue(g.V().has("legs", "four").hasNext());

        g.V().hasLabel("cat").property("tail", "one").iterate();
        Assert.assertTrue(g.V().has("legs", "four").hasNext());
        Assert.assertTrue(g.V().has("tail", "one").hasNext());

        g.V().hasLabel("cat").property("eyes", "two").iterate();
        Assert.assertTrue(g.V().has("legs", "four").hasNext());
        Assert.assertTrue(g.V().has("tail", "one").hasNext());
        Assert.assertTrue(g.V().has("eyes", "two").hasNext());

        g.V().hasLabel("cat").properties("eyes").drop().iterate();
        Assert.assertTrue(g.V().has("legs", "four").hasNext());
        Assert.assertTrue(g.V().has("tail", "one").hasNext());
        Assert.assertFalse(g.V().has("eyes", "two").hasNext());

        g.V().hasLabel("cat").properties("tail").drop().iterate();
        Assert.assertTrue(g.V().has("legs", "four").hasNext());
        Assert.assertFalse(g.V().has("tail", "one").hasNext());
        Assert.assertFalse(g.V().has("eyes", "two").hasNext());

        g.V().hasLabel("cat").properties("legs").drop().iterate();
        Assert.assertFalse(g.V().has("legs", "four").hasNext());
        Assert.assertFalse(g.V().has("tail", "one").hasNext());
        Assert.assertFalse(g.V().has("eyes", "two").hasNext());
    }
}
