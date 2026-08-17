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

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyQueryTracingStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class FireflyQueryTracingStrategyTest {
    static private final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    static private FireflyGraph SETUP_GRAPH;

    @BeforeClass
    static public void beforeAll() throws Exception {
        SETUP_GRAPH = FireflyGraph.open(CONFIG);
        final Field mrtEnabled = SETUP_GRAPH.getClass().getDeclaredField("queryTracingEnabled");
        mrtEnabled.setAccessible(true);
        mrtEnabled.set(SETUP_GRAPH, true);
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        Vertex v1 = g.addV("v1").next();
        Vertex v2 = g.addV("v2").next();
        Vertex v3 = g.addV("v3").next();
        g.addE("e2x").from(v1).to(v2).iterate();
        g.addE("e3x").from(v1).to(v3).iterate();
        g.addE("e2x").from(v3).to(v2).iterate();
        g.addE("e3x").from(v1).to(v3).iterate();
    }

    @AfterClass
    static public void afterAll() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
        SETUP_GRAPH.close();
    }

    @Test
    public void testStrategyIsApplied() {
        var g = SETUP_GRAPH.traversal();
        var traversal = g.V().hasLabel("v2").bothE();
        final List<Step> steps = traversal.asAdmin().getSteps();
        final int beforeSize = steps.size();
        for (final Step step : steps) {
            if (step instanceof ProfileStep || step instanceof FireflyQueryTracingStep) {
                Assert.fail("Should not have ProfileStep or QueryTracing before strategies are applied.");
            }
        }
        traversal.asAdmin().applyStrategies();
        int finalStepIndex = beforeSize * 2;
        for (int i = 0; i < steps.size(); i++) {
            if (i == finalStepIndex) {
                Assert.assertTrue(steps.get(i) instanceof FireflyQueryTracingStep);
            } else if (i % 2 == 1) { // Every odd index should be a profile step
                Assert.assertTrue(steps.get(i) instanceof ProfileStep);
            }
        }
    }

    @Test
    public void testStrategyIsNotApplied() {
        CONFIG.setProperty(ConfigurationHelper.Keys.QUERY_TRACING_LOG_THRESHOLD, "-1");
        try (final FireflyGraph disabledGraph = FireflyGraph.open(CONFIG)) {
            var g = disabledGraph.traversal();
            var traversal = g.V().hasLabel("v2").bothE();
            final List<Step> steps = traversal.asAdmin().getSteps();
            traversal.asAdmin().applyStrategies();
            for (final Step step : steps) {
                if (step instanceof ProfileStep || step instanceof FireflyQueryTracingStep) {
                    Assert.fail("Should not have ProfileStep or SlowQueryLogStep when the Strategy is disabled.");
                }
            }
        }
    }

    @Test
    public void testStrategyIsNotAppliedWhenProfiling() {
        var g = SETUP_GRAPH.traversal();
        var traversal = g.V().hasLabel("v2").bothE().profile();
        final List<Step> steps = traversal.asAdmin().getSteps();
        for (final Step step : steps) {
            if (step instanceof ProfileStep || step instanceof FireflyQueryTracingStep) {
                Assert.fail("Should not have ProfileStep or SlowQueryLogStep before strategies are applied.");
            }
        }
        traversal.asAdmin().applyStrategies();
        for (int i = 0; i < steps.size(); i++) {
            Assert.assertFalse(steps.get(i) instanceof FireflyQueryTracingStep);
            if (i % 2 == 1 && i != steps.size() - 1) { // Every odd index should be a profile step
                Assert.assertTrue(steps.get(i) instanceof ProfileStep);
            }
        }
    }

    @Test
    public void testStrategyDoesNotAffectTraversalOutcome() {
        var g = SETUP_GRAPH.traversal();
        Assert.assertEquals(3, (long) g.V().count().next());
        Assert.assertEquals(4, (long) g.E().count().next());
        var e2Traversal = g.V().hasLabel("v2").bothE();
        while (e2Traversal.hasNext()) {
            Assert.assertEquals("e2x", e2Traversal.next().label());
        }
        var e3Traversal = g.V().hasLabel("v3").inE();
        while (e3Traversal.hasNext()) {
            Assert.assertEquals("e3x", e3Traversal.next().label());
        }
    }

    @Test
    public void testStrategyDoesNotDupeEdges() {
        // Specific edge-case traversal that causes the issue from GRAPH-1584
        var g = SETUP_GRAPH.traversal();
        Assert.assertEquals(3, (long) g.V().count().next());
        Assert.assertEquals(4, (long) g.E().count().next());
        try {
            g.addV("dupeTestV1").iterate();
            g.addV("dupeTestV2").iterate();
            addEdgeIfNotExist(g);
            Assert.assertEquals(5, (long) g.E().count().next());
            addEdgeIfNotExist(g);
            Assert.assertEquals(5, (long) g.E().count().next());
        } finally {
            g.V().hasLabel("dupeTestV1").drop().iterate();
            g.V().hasLabel("dupeTestV2").drop().iterate();
        }

        Assert.assertEquals(3, (long) g.V().count().next());
        Assert.assertEquals(4, (long) g.E().count().next());
    }

    private void addEdgeIfNotExist(final GraphTraversalSource graphTraversalSource) {
        graphTraversalSource
                .V().hasLabel("dupeTestV1").as("v1")
                .V().hasLabel("dupeTestV2").as("v2")
                .select("v1", "v2")
                .coalesce(
                        __.select("v1").outE("dupeEdgeLabel").where(__.inV().as("v2")),
                        __.addE("dupeEdgeLabel").from("v1").to("v2")
                )
                .iterate();
    }
}
