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

package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.process.traversal.step.map.FireflyCountGlobalStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class VertexCountTest {
    private static Configuration config;

    @BeforeClass
    public static void beforeClass() {
        // Remove everything from the graph and load modern dataset.
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            // Drop graph.
            graph.traversal().V().drop().iterate();

            // Load modern dataset.
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
        }
    }

    @AfterClass
    public static void afterClass() {
        // Remove everything from the graph.
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
        }
    }

    public void setFastCountConfig(final boolean labelIndex, final boolean hasIndex1, final String hasKey1,
                                   final boolean hasIndex2, final String hasKey2) {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ENABLE_FAST_COUNT_STRATEGY.toLowerCase(), "true");
        if (labelIndex) {
            config.setProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), "true");
        }
        if (hasIndex1 || hasIndex2) {
            if (!hasIndex2) {
                config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES.toLowerCase(), hasKey1);
            } else if (!hasIndex1) {
                config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES.toLowerCase(), hasKey2);
            } else {
                config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES.toLowerCase(), String.format("%s,%s", hasKey1, hasKey2));
            }
        }
    }

    public void setRegularCountConfig() {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ENABLE_FAST_COUNT_STRATEGY.toLowerCase(), "false");
    }

    private void countVertices(final boolean labelIndex, final String label, final boolean hasIndex1, final String hasKey1,
                               final Object hasValue1, final boolean hasIndex2, final String hasKey2, final Object hasValue2) {
        // Run fast count and get result.
        setFastCountConfig(labelIndex, hasIndex1, hasKey1, hasIndex2, hasKey2);
        final long fastCount;
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            final GraphTraversal<?, ?> t = g.V();
            if (label != null) {
                t.hasLabel(label);
            }
            if (hasKey1 != null) {
                t.has(hasKey1, hasValue1);
            }
            if (hasKey2 != null) {
                t.has(hasKey2, hasValue2);
            }
            fastCount = t.count().next();
            Assert.assertEquals(1, t.asAdmin().getSteps().size());
            Assert.assertTrue(t.asAdmin().getSteps().get(0) instanceof FireflyCountGlobalStep);
        }

        // Run normal count and get result.
        setRegularCountConfig();
        final long regularCount;
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            final GraphTraversal<?, ?> t = g.V();
            if (label != null) {
                t.hasLabel(label);
            }
            if (hasKey1 != null) {
                t.has(hasKey1, hasValue1);
            }
            if (hasKey2 != null) {
                t.has(hasKey2, hasValue2);
            }
            regularCount = t.count().next();
            Assert.assertNotEquals(1, t.asAdmin().getSteps().size());
            Assert.assertFalse(t.asAdmin().getSteps().get(0) instanceof FireflyCountGlobalStep);
        }

        Assert.assertEquals(regularCount, fastCount);
    }

    @Test
    public void countIndexedHasLabelTest() {
        countVertices(true, "person", false, null, null, false, null, null);
    }

    @Test
    public void countUnindexedHasLabelTest() {
        countVertices(false, "person", false, null, null, false, null, null);
    }

    @Test
    public void countIndexedHasLabelIndexedHasTest() {
        countVertices(true, "person", true, "name", "marko", false, null, null);
    }

    @Test
    public void countIndexedHasLabelUnindexedHasTest() {
        countVertices(true, "person", false, "name", "marko", false, null, null);
    }

    @Test
    public void countUnindexedHasLabelIndexedHasTest() {
        countVertices(false, "person", true, "name", "marko", false, null, null);
    }

    @Test
    public void countUnindexedHasLabelUnindexedHasTest() {
        countVertices(false, "person", false, "name", "marko", false, null, null);
    }

    @Test
    public void countIndexedHasTest() {
        countVertices(true, null, true, "name", "marko", false, null, null);
    }

    @Test
    public void countUnindexedHasTest() {
        countVertices(false, null, false, "name", "marko", false, null, null);
    }

    @Test
    public void countIndexedHasIndexedHasTest() {
        countVertices(true, null, true, "name", "marko", true, "age", 29);
    }

    @Test
    public void countIndexedHasUnindexedHasTest() {
        countVertices(true, null, true, "name", "marko", false, "age", 29);
    }

    @Test
    public void countUnindexedHasIndexedHasTest() {
        countVertices(false, null, false, "name", "marko", true, "age", 29);
    }

    @Test
    public void countUnindexedHasUnindexedHasTest() {
        countVertices(false, null, false, "name", "marko", false, "age", 29);
    }
}
