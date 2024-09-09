package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.process.traversal.step.FireflyOtherVBatchReadStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.*;

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
        assertEquals(0, result.size());

        result = g.V().outE().not(__.hasLabel("knows")).otherV().toList();
        assertEquals(4, result.size());

        // borrowed from TinkerPop Feature tests
        result = g.V().local(__.bothE("created").limit(1)).otherV().values("name").toList();
        assertEquals(5, result.size());

        result = g.V(4).bothE().otherV().toList();
        assertEquals(3, result.size());

        result = g.V(4).bothE().has("weight", P.lt(1.0)).otherV().toList();
        assertEquals(1, result.size());
        assertEquals(3, ((Vertex)result.get(0)).id());
    }

    @Test
    public void testStrategyEnabled() {
        final var g = graph.traversal();

        var t = g.V().outE().not(__.hasLabel("knows")).otherV().hasLabel("test");
        t.asAdmin().applyStrategies();
        List<Step> steps = t.asAdmin().getSteps();
        assertTrue(containsCustomStep(steps));
    }

    @Test
    public void testStrategySkipped() {
        final var g = graph.traversal();

        // outE().otherV() is same as out(), so replaced with FireflyCompositeIdStep
        var t = g.V().outE().otherV().hasLabel("test");
        t.asAdmin().applyStrategies();
        List<Step> steps = t.asAdmin().getSteps();
        assertFalse(containsCustomStep(steps));
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
