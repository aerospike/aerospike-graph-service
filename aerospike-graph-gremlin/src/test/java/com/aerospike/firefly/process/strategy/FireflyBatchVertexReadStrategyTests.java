package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.process.traversal.step.FireflyBatchVertexReadStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class FireflyBatchVertexReadStrategyTests {

    private static FireflyGraph graph;

    @BeforeClass
    public static void setUp() throws Exception {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        graph = FireflyGraph.open(config);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (graph != null) graph.close();
    }

    @Test
    public void shouldApplyPropertiesReadOptimization() {
        final GraphTraversalSource g = graph.traversal();

        // last step, so need to read properties
        assertReadProperties(g.V().both(), null);

        // followed by step required all props
        assertReadProperties(g.V().in().elementMap(), null);
        assertReadProperties(g.V().in().has("name", "marko").valueMap(), null);

        // can skip some properties
        assertReadProperties(g.V().both().valueMap("age"), List.of("age"));
        assertReadProperties(g.V().both().elementMap("name", "age"), List.of("name", "age"));
        assertReadProperties(g.V().both().properties("name"), List.of("name"));
        assertReadProperties(g.V().both().propertyMap("name"), List.of("name"));

        assertReadProperties(g.V().both().hasId(1).valueMap("name"), List.of("name"));
        assertReadProperties(g.V().both().hasLabel("person").valueMap("name"), List.of("name"));
        assertReadProperties(g.V().both().has("name", "marko").valueMap("age"), List.of("name", "age"));

        // label, so need to read props
        assertReadProperties(g.V().both().as("a").propertyMap("name").select("a"), null);
        assertReadProperties(g.V().both().as("a").elementMap("name").out().select("a"), null);

        // read all props because dedup
        assertReadProperties(g.V().both().has(T.label, "software").dedup().by("lang").values("name"), null);

        assertReadProperties(g.V().in().hasId(P.neq(1)), null);
    }

    @Test
    public void multipleSimpleStepTest() {
        final GraphTraversalSource g = graph.traversal();

        final GraphTraversal t = g.V().both().both().propertyMap("name");
        t.asAdmin().applyStrategies();
        final List<Step> steps = t.asAdmin().getSteps();

        // first FireflyBatchVertexReadStep can skip properties, but not. To fix later
        final FireflyBatchVertexReadStep first = (FireflyBatchVertexReadStep) steps.get(1);
        assertNull(first.getProperties());

        // second FireflyBatchVertexReadStep should read only "name"
        final FireflyBatchVertexReadStep second = (FireflyBatchVertexReadStep) steps.get(2);
        assertEquals(List.of("name"), second.getProperties());
    }

    @Test
    public void multipleStepsWithHasTest() {
        final GraphTraversalSource g = graph.traversal();

        final GraphTraversal t = g.V().out().has("name", "marko").in().has("age", 29).valueMap("name");
        t.asAdmin().applyStrategies();
        final List<Step> steps = t.asAdmin().getSteps();

        // first FireflyBatchVertexReadStep can read only "name"
        final FireflyBatchVertexReadStep first = (FireflyBatchVertexReadStep) steps.get(1);
        assertEquals(List.of("name"), first.getProperties());

        // second FireflyBatchVertexReadStep should read only "age", "name"
        final FireflyBatchVertexReadStep second = (FireflyBatchVertexReadStep) steps.get(2);
        assertEquals(List.of("age", "name"), second.getProperties());
    }

    private void assertReadProperties(final GraphTraversal t, final List<String> expected) {
        t.asAdmin().applyStrategies();
        final List<Step> steps = t.asAdmin().getSteps();
        FireflyBatchVertexReadStep step = null;
        for (final Step s : steps) {
            if (s instanceof FireflyBatchVertexReadStep) {
                step = (FireflyBatchVertexReadStep) s;
                break;
            }
        }

        assertNotNull(step);
        assertEquals(expected, step.getProperties());
    }
}
