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

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;

import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

public class FireflyGraphStepStrategyTest {

    @Test
    public void shouldApplyPropertiesReadOptimization() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            // read all properties
            assertReadProperties(g.V(), null);
            assertReadProperties(g.V().elementMap(), null);
            assertReadProperties(g.V().valueMap(), null);
            assertReadProperties(g.V().properties(), null);
            assertReadProperties(g.V().propertyMap(), null);

            // read specific properties
            assertReadProperties(g.V().elementMap("name", "age"), List.of("name", "age"));
            assertReadProperties(g.V().properties("name"), List.of("name"));
            assertReadProperties(g.V().propertyMap("name"), List.of("name"));

            // skip special filters
            assertReadProperties(g.V().hasId(1).elementMap("name"), List.of("name"));
            assertReadProperties(g.V().has(T.id, 1).elementMap("name"), List.of("name"));
            assertReadProperties(g.V().hasLabel("person").elementMap("name"), List.of("name"));

            // filter and properties
            assertReadProperties(g.V().has("name", "marko").properties("age"), List.of("name", "age"));
            assertReadProperties(g.V().has("name", "marko").elementMap("age", "name"), List.of("name", "age"));

            // following element() step, so should not be optimization
            assertReadProperties(g.V().valueMap("name").element(), null);

            // following out() step, so no need to read properties if no label
            assertReadProperties(g.V().out(), List.of());
            assertReadProperties(g.V().out().properties("name"), List.of());

            // can't use optimization with labels
            assertReadProperties(g.V().as("a").out(), null);
            assertReadProperties(g.V().as("a").out().elementMap("name"), null);
        }
    }

    private void assertReadProperties(final GraphTraversal t, final List<String> expected) {
        t.asAdmin().applyStrategies();
        final List<Step> steps = t.asAdmin().getSteps();
        final FireflyGraphStep graphStep = (FireflyGraphStep) steps.get(0);
        assertEquals(expected, graphStep.getProperties());
    }
}
