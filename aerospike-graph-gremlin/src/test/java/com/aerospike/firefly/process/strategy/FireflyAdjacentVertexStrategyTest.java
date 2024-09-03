package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.process.traversal.step.map.FireflyAdjacentVertexIdStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
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
        final FireflyGraph graph = FireflyGraph.open(config);
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
        } finally {
            graph.getBaseGraph().dropDatabase(graph, true);
            graph.close();
        }
    }

    @Test
    public void testStrategyDisabled() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ENABLE_CACHED_ADJACENT_ID_STRATEGY, "false");
        final FireflyGraph graph = FireflyGraph.open(config);
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
            graph.close();
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
