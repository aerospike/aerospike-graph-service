package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
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

            assertReadProperties(g.V().elementMap("name", "age"), List.of("name", "age"));
            assertReadProperties(g.V().properties("name"), List.of("name"));
            assertReadProperties(g.V().propertyMap("name"), List.of("name"));

            // filter and properties
            assertReadProperties(g.V().has("name", "marko").properties("age"), List.of("name", "age"));

            // following element() step, so should not be optimization
            assertReadProperties(g.V().properties("name").element(), null);

            // following out() step, so no need to read properties if no label
            assertReadProperties(g.V().out(), List.of());
            assertReadProperties(g.V().out().properties("name"), List.of());
            assertReadProperties(g.V().as("a").out().properties("name"), null);
        }
    }

    private void assertReadProperties(final GraphTraversal t, final List<String> expected) {
        t.asAdmin().applyStrategies();
        final List<Step> steps = t.asAdmin().getSteps();
        final FireflyGraphStep graphStep = (FireflyGraphStep) steps.get(0);
        assertEquals(expected, graphStep.getProperties());
    }
}
