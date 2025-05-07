package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.process.traversal.step.FireflyEdgeToVertexBatchReadStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FireflyEdgeToVertexBatchReadStrategyTest {
    private static FireflyGraph graph;

    @BeforeClass
    public static void beforeAll() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ENABLE_BATCH_EDGE_TO_VERTEX_READ_STRATEGY, "true");
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

        // filter out everything
        List result = g.V().outE().not(__.hasLabel("knows")).inV().hasLabel("test").toList();
        assertEquals(0, result.size());

        result = g.V().outE().not(__.hasLabel("knows")).outV().hasLabel("test").toList();
        assertEquals(0, result.size());

        result = g.V().outE().not(__.hasLabel("knows")).bothV().hasLabel("test").toList();
        assertEquals(0, result.size());

        // only part of results is valid
        result = g.V().outE().not(__.hasLabel("knows")).inV().has("name", "ripple").toList();
        assertEquals(1, result.size());

        result = g.V().outE().not(__.hasLabel("created")).outV().has("name", "marko").toList();
        assertEquals(2, result.size());

        result = g.V().outE().not(__.hasLabel("knows")).bothV().has("name", "marko").toList();
        assertEquals(1, result.size());

        // lets inject new vertex and check we captured right `a`
        result = g.V().outE().not(__.hasLabel("knows")).inV().as("a")
                .has("name", "ripple")
                .inject(null)
                .select("a").toList();
        assertEquals(1, result.size());

        result = g.V().outE().not(__.hasLabel("created")).outV()
                .has("name", "marko").as("a")
                .inject(null)
                .select("a").toList();
        assertEquals(2, result.size());

        // no hasContainer
        result = g.V().outE().not(__.hasLabel("knows")).inV().toList();
        assertEquals(4, result.size());

        result = g.V().outE().not(__.hasLabel("knows")).outV().toList();
        assertEquals(4, result.size());

        result = g.V().outE().not(__.hasLabel("knows")).bothV().toList();
        assertEquals(8, result.size());
    }

    @Test
    public void testStrategyEnabled() {
        final var g = graph.traversal();

        var t = g.V().outE().not(__.hasLabel("knows")).inV().hasLabel("test");
        assertTrue(containsCustomStep(t));

        t = g.V().outE().not(__.hasLabel("knows")).outV().hasLabel("test");
        assertTrue(containsCustomStep(t));

        t = g.V().outE().not(__.hasLabel("knows")).bothV().hasLabel("test");
        assertTrue(containsCustomStep(t));
    }

    @Test
    public void testStrategyDisabledInConfiguration() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ENABLE_BATCH_EDGE_TO_VERTEX_READ_STRATEGY, "false");

        final FireflyGraph graph = FireflyGraph.open(config);
        final var g = graph.traversal();

        var t = g.V().outE().not(__.hasLabel("knows")).inV().hasLabel("test");
        assertFalse(containsCustomStep(t));

        t = g.V().outE().not(__.hasLabel("knows")).outV().hasLabel("test");
        assertFalse(containsCustomStep(t));

        t = g.V().outE().not(__.hasLabel("knows")).bothV().hasLabel("test");
        assertFalse(containsCustomStep(t));

        graph.close();
    }

    @Test
    public void testStrategySkipped() {
        final var g = graph.traversal();

        // outE().inV() is same as out(), so replaced with FireflyCompositeIdStep
        var t = g.V().outE().inV().hasLabel("test");

        assertFalse(containsCustomStep(t));
    }

    private boolean containsCustomStep(final GraphTraversal traversal) {
        traversal.asAdmin().applyStrategies();

        List<Step> steps = traversal.asAdmin().getSteps();

        for (final Step step : steps) {
            if (step instanceof FireflyEdgeToVertexBatchReadStep) {
                return true;
            }
        }
        return false;
    }
}
