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

import com.aerospike.firefly.process.traversal.step.map.FireflyAdjacentVertexIdStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class FireflyAdjacentVertexStrategyTest {

    @Test
    public void testStrategyEnabled() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ENABLE_CACHED_ADJACENT_ID_STRATEGY, "true");
        try (FireflyGraph graph = FireflyGraph.open(config)) {
            try {
                final var g = graph.traversal();
                var t = g.V().out().id();
                t.asAdmin().applyStrategies();
                List<Step> steps = t.asAdmin().getSteps();
                Assert.assertTrue(containsCustomStep(steps));

                t = g.V().in().id();
                t.asAdmin().applyStrategies();
                steps = t.asAdmin().getSteps();
                Assert.assertTrue(containsCustomStep(steps));

                t = g.V().out("edgeLabel").id();
                t.asAdmin().applyStrategies();
                steps = t.asAdmin().getSteps();
                Assert.assertTrue(containsCustomStep(steps));

                t = g.V().in("edgeLabel").id();
                t.asAdmin().applyStrategies();
                steps = t.asAdmin().getSteps();
                Assert.assertTrue(containsCustomStep(steps));

                t = g.V().out().has("foo", "bar").id();
                t.asAdmin().applyStrategies();
                steps = t.asAdmin().getSteps();
                Assert.assertFalse(containsCustomStep(steps));

                t = g.V().in().has("foo", "bar").id();
                t.asAdmin().applyStrategies();
                steps = t.asAdmin().getSteps();
                Assert.assertFalse(containsCustomStep(steps));

                t = g.V().both().both().id();
                t.asAdmin().applyStrategies();
                steps = t.asAdmin().getSteps();
                Assert.assertTrue(containsCustomStep(steps));
            } finally {
                graph.getBaseGraph().dropDatabase(graph, true);
            }
        }
    }

    @Test
    public void testStrategyDisabled() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ENABLE_CACHED_ADJACENT_ID_STRATEGY, "false");
        try (FireflyGraph graph = FireflyGraph.open(config)) {
            try {
                final var g = graph.traversal();
                var t = g.V().out().id();
                t.asAdmin().applyStrategies();
                List<Step> steps = t.asAdmin().getSteps();
                Assert.assertFalse(containsCustomStep(steps));

                t = g.V().in().id();
                t.asAdmin().applyStrategies();
                steps = t.asAdmin().getSteps();
                Assert.assertFalse(containsCustomStep(steps));

                t = g.V().out("edgeLabel").id();
                t.asAdmin().applyStrategies();
                steps = t.asAdmin().getSteps();
                Assert.assertFalse(containsCustomStep(steps));

                t = g.V().in("edgeLabel").id();
                t.asAdmin().applyStrategies();
                steps = t.asAdmin().getSteps();
                Assert.assertFalse(containsCustomStep(steps));
                Assert.assertFalse(containsCustomStep(steps));
            } finally {
                graph.getBaseGraph().dropDatabase(graph, true);
            }
        }
    }

    private boolean containsCustomStep(final List<Step> steps) {
        for (final Step step : steps) {
            if (step instanceof FireflyAdjacentVertexIdStep) {
                return true;
            }
        }
        return false;
    }
}
