package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.process.traversal.step.FireflyOtherVBatchReadStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class FireflyBatchOtherVReadStrategyTest {
    private static FireflyGraph graph;

    @BeforeClass
    public static void beforeAll() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ENABLE_BATCH_EDGE_READ_STRATEGY, "true");
        graph = FireflyGraph.open(config);
        graph.getBaseGraph().dropDatabase(graph, false);

        Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);
    }

    @AfterClass
    public static void cleanUp() {
        graph.getBaseGraph().dropDatabase(graph, false);
        graph.close();
    }

    @Test
    public void verifyResult() {
        final var g = graph.traversal();

        List result = g.V().outE().not(__.hasLabel("knows")).otherV().hasLabel("test").toList();
        Assert.assertEquals(0, result.size());

        result = g.V().outE().not(__.hasLabel("knows")).otherV().toList();
        Assert.assertEquals(4, result.size());
    }

    @Test
    public void testStrategyEnabled() {
        final var g = graph.traversal();

        var t = g.V().outE().not(__.hasLabel("knows")).otherV().hasLabel("test");
        t.asAdmin().applyStrategies();
        List<Step> steps = t.asAdmin().getSteps();
        Assert.assertTrue(containsCustomStep(steps));
    }

    private boolean containsCustomStep(final List<Step> steps) {
        for (final Step step : steps) {
            if (step instanceof FireflyOtherVBatchReadStep) {
                return true;
            }
        }
        return false;
    }
}
