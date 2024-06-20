package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGraphStepStrategy;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TestGraphStepHasContainers extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Override
    protected boolean runTest() {
        // This test is only valid on one node clusters due to how the calculations for cardinality work.
        return db.getClient().getNodes().length == 1;
    }

    @Before
    public void setup() {
        graph.getBaseGraph().clearNamespace();
    }

    private void loadTestData() {
        final GraphTraversalSource g = graph.traversal();
        g.addV("person").property("isCool", "sometimes").property("firstName", "simon").property("age", 16).next();
        g.addV("person").property("isCool", "sometimes").property("firstName", "simon").property("age", 17).next();
        g.addV("person").property("isCool", "sometimes").property("firstName", "simon").property("age", 18).next();
        g.addV("person").property("isCool", "sometimes").property("firstName", "simon").property("age", 19).next();
        g.addV("person").property("isCool", "sometimes").property("firstName", "simon").property("age", 20).next();
        g.addV("person").property("isCool", "sometimes").property("firstName", "simon").property("age", 21).next();
        g.addV("person").property("isCool", "sometimes").property("firstName", "simon").property("age", 22).next();
        g.addV("person").property("isCool", "exceptionally").property("firstName", "grant").property("age", 23).next();
        g.addV("person").property("isCool", "exceptionally").property("firstName", "grant").property("age", 24).next();
        g.addV("person").property("isCool", "exceptionally").property("firstName", "grant").property("age", 25).next();
        g.addV("person").property("isCool", "exceptionally").property("firstName", "grant").property("age", 26).next();
        g.addV("person").property("isCool", "exceptionally").property("firstName", "grant").property("age", 27).next();
        g.addV("person").property("isCool", "exceptionally").property("firstName", "grant").property("age", 28).next();
        g.addV("person").property("isCool", "yes").property("firstName", "lyndon").property("age", 29).next();
        g.addV("person").property("isCool", "yes").property("firstName", "lyndon").property("age", 30).next();
        g.addV("person").property("isCool", "yes").property("firstName", "lyndon").property("age", 31).next();
        g.addV("person").property("isCool", "no").property("firstName", "lyndon").property("age", 32).next();
        g.addV("person").property("isCool", "no").property("firstName", "lyndon").property("age", 33).next();
        g.addV("person").property("isCool", "no").property("firstName", "lyndon").property("age", 34).next();
        g.addV("person").property("isCool", "sometimes").property("firstName", "lyndon").property("age", 35).next();
    }

    private static class KeyValuePair {
        public final String key;
        public final Object value;

        public KeyValuePair(final String key, final Object value) {
            this.key = key;
            this.value = value;
        }
    }

    public GraphTraversal<Vertex, Vertex> getTraversal(final GraphTraversalSource g) {
        return g.V().hasLabel("person").has("isCool", "sometimes").has("firstName", "lyndon").has("age", 35);
    }

    private GraphTraversal<Vertex, Vertex> setupTest(final String indexes, final boolean labelIndex) {
        loadTestData();

        // We insert data then set the cardinality and indexes so that we can force the bvals to be set.
        graph.close();
        config.setProperty("aerospike.graph.index.vertex.properties", indexes);
        config.setProperty("aerospike.graph.index.vertex.label.enabled", labelIndex ? "true" : "false");
        config.setProperty("aerospike.graph.admin.metadata.cardinality.update.frequency", "1");
        config.setProperty("aerospike.graph.admin.metadata.index.update.frequency", "1");
        graph = FireflyGraph.open(config);

        // Give some time for the indexes to be created.
        try {
            Thread.sleep(10);
        } catch (InterruptedException ignored) {
        }
        return getTraversal(graph.traversal());
    }

    @Test
    public void testGraphStepOnlyPropertiesIndexed() {
        final GraphTraversal<Vertex, Vertex> multiHasTraversal = setupTest("isCool,age,firstName", false);

        final List<KeyValuePair> expectedOrder = List.of(
                new KeyValuePair("age", 35),
                new KeyValuePair("isCool", "sometimes"),
                new KeyValuePair("firstName", "lyndon"),
                new KeyValuePair("~label", "person")
        );

        // Test that the order is correct.
        testTraversalAgainstExpected(multiHasTraversal, expectedOrder);
    }

    @Test
    public void testGraphStepOnlyLabelIndexed() throws InterruptedException {
        final GraphTraversal<Vertex, Vertex> multiHasTraversal = setupTest("", true);

        final List<KeyValuePair> expectedOrder = List.of(
                new KeyValuePair("~label", "person")
        );

        // Test that the order is correct.
        testTraversalAgainstExpectedOrderIndeterminant(multiHasTraversal, expectedOrder);
    }

    @Test
    public void testGraphStepfirstNameAgeNoLabelIndexed() {
        final GraphTraversal<Vertex, Vertex> multiHasTraversal = setupTest("firstName,age", false);

        final List<KeyValuePair> expectedOrder = List.of(
                new KeyValuePair("age", 35),
                new KeyValuePair("firstName", "lyndon"),
                new KeyValuePair("isCool", "sometimes"),
                new KeyValuePair("~label", "person")
        );

        // Test that the order is correct.
        testTraversalAgainstExpected(multiHasTraversal, expectedOrder);
    }

    @Test
    public void testGraphStepIsCoolAgeNoLabelIndexed() {
        final GraphTraversal<Vertex, Vertex> multiHasTraversal = setupTest("isCool,age", false);

        final List<KeyValuePair> expectedOrder = List.of(
                new KeyValuePair("age", 35),
                new KeyValuePair("isCool", "sometimes"),
                new KeyValuePair("firstName", "lyndon"),
                new KeyValuePair("~label", "person")
        );

        // Test that the order is correct.
        testTraversalAgainstExpected(multiHasTraversal, expectedOrder);
    }

    @Test
    public void testGraphStepIsCoolfirstNameNoLabelIndexed() {
        final GraphTraversal<Vertex, Vertex> multiHasTraversal = setupTest("isCool,firstName", false);

        final List<KeyValuePair> expectedOrder = List.of(
                new KeyValuePair("isCool", "sometimes"),
                new KeyValuePair("firstName", "lyndon"),
                new KeyValuePair("age", 35),
                new KeyValuePair("~label", "person")
        );

        // Test that the order is correct.
        testTraversalAgainstExpected(multiHasTraversal, expectedOrder);
    }

    @Test
    public void testGraphStepfirstNameAgeLabelIndexed() throws InterruptedException {
        final GraphTraversal<Vertex, Vertex> multiHasTraversal = setupTest("firstName,age", true);

        final List<KeyValuePair> expectedOrder = List.of(
                new KeyValuePair("age", 35),
                new KeyValuePair("firstName", "lyndon"),
                new KeyValuePair("~label", "person"),
                new KeyValuePair("isCool", "sometimes")
        );

        // Test that the order is correct.
        testTraversalAgainstExpected(multiHasTraversal, expectedOrder);
    }

    @Test
    public void testGraphStepIsCoolAgeLabelIndexed() throws InterruptedException {
        final GraphTraversal<Vertex, Vertex> multiHasTraversal = setupTest("isCool,age", true);

        final List<KeyValuePair> expectedOrder = List.of(
                new KeyValuePair("age", 35),
                new KeyValuePair("isCool", "sometimes"),
                new KeyValuePair("~label", "person"),
                new KeyValuePair("firstName", "lyndon")
        );

        // Test that the order is correct.
        testTraversalAgainstExpected(multiHasTraversal, expectedOrder);
    }

    @Test
    public void testGraphStepIsCoolfirstNameLabelIndexed() {
        final GraphTraversal<Vertex, Vertex> multiHasTraversal = setupTest("isCool,firstName", true);

        final List<KeyValuePair> expectedOrder = List.of(
                new KeyValuePair("isCool", "sometimes"),
                new KeyValuePair("firstName", "lyndon"),
                new KeyValuePair("~label", "person"),
                new KeyValuePair("age", 35)
        );

        // Test that the order is correct.
        testTraversalAgainstExpected(multiHasTraversal, expectedOrder);
    }

    @Test
    public void testGraphStepAllIndexed() {
        final GraphTraversal<Vertex, Vertex> multiHasTraversal = setupTest("isCool,firstName,age", true);

        final List<KeyValuePair> expectedOrder = List.of(
                new KeyValuePair("age", 35),
                new KeyValuePair("isCool", "sometimes"),
                new KeyValuePair("firstName", "lyndon"),
                new KeyValuePair("~label", "person")
        );

        // Test that the order is correct.
        testTraversalAgainstExpected(multiHasTraversal, expectedOrder);
    }

    private void testTraversalAgainstExpected(final GraphTraversal<Vertex, Vertex> traversal, final List<KeyValuePair> expectedOrder) {
        // Apply strategy to traversal so we can inspect it.
        final FireflyGraphStepStrategy strategy = new FireflyGraphStepStrategy();
        strategy.apply(traversal.asAdmin());
        final List<Step> steps = traversal.asAdmin().getSteps();

        boolean found = false;
        for (final Step step : steps) {
            if (step instanceof FireflyGraphStep) {
                // Should only be found once.
                Assert.assertFalse(found);

                found = true;
                final FireflyGraphStep fireflyGraphStep = (FireflyGraphStep) step;
                final List<FireflyGraphStep.HasContainerWithCardinality> orderedHasContainers =
                        FireflyBatchReadHelper.getHasContainersWithCardinalityOrder(graph, FireflyVertex.class, fireflyGraphStep.getHasContainers());
                Assert.assertEquals(expectedOrder.size(), orderedHasContainers.size());
                for (int i = 0; i < expectedOrder.size(); i++) {
                    final KeyValuePair expected = expectedOrder.get(i);
                    final FireflyGraphStep.HasContainerWithCardinality actual = orderedHasContainers.get(i);
                    Assert.assertEquals(expected.key, actual.hasContainer.getKey());
                    Assert.assertEquals(expected.value, actual.hasContainer.getValue());
                }
            }
        }

        // Make sure the test ran correctly.
        Assert.assertTrue(found);
    }

    private void testTraversalAgainstExpectedOrderIndeterminant(final GraphTraversal<Vertex, Vertex> traversal, final List<KeyValuePair> expectedOrder) {
        // Apply strategy to traversal so we can inspect it.
        final FireflyGraphStepStrategy strategy = new FireflyGraphStepStrategy();
        strategy.apply(traversal.asAdmin());
        final List<Step> steps = traversal.asAdmin().getSteps();

        boolean found = false;
        for (final Step step : steps) {
            if (step instanceof FireflyGraphStep) {
                // Should only be found once.
                Assert.assertFalse(found);

                found = true;
                final FireflyGraphStep fireflyGraphStep = (FireflyGraphStep) step;
                final List<FireflyGraphStep.HasContainerWithCardinality> orderedHasContainers =
                        FireflyBatchReadHelper.getHasContainersWithCardinalityOrder(graph, FireflyVertex.class, fireflyGraphStep.getHasContainers());
                Assert.assertTrue(expectedOrder.size() <= orderedHasContainers.size());
                for (int i = 0; i < expectedOrder.size(); i++) {
                    final KeyValuePair expected = expectedOrder.get(i);
                    final FireflyGraphStep.HasContainerWithCardinality actual = orderedHasContainers.get(i);
                    Assert.assertEquals(expected.key, actual.hasContainer.getKey());
                    Assert.assertEquals(expected.value, actual.hasContainer.getValue());
                }
            }
        }

        // Make sure the test ran correctly.
        Assert.assertTrue(found);
    }

    @Test
    public void testHasContainerResults() {
        setupTest("isCool,firstName,age", true);
        final GraphTraversalSource g = graph.traversal();
        List<Vertex> v = g.V().hasLabel("person").has("isCool", "sometimes").has("firstName", "lyndon").has("age", 35).toList();
        Assert.assertEquals(1, v.size());
        Assert.assertEquals("lyndon", v.get(0).value("firstName"));
        Assert.assertEquals((Integer) 35, v.get(0).value("age"));
        Assert.assertEquals("sometimes", v.get(0).value("isCool"));

        v = g.V().hasLabel("person").has("isCool", "sometimes").has("firstName", "notlyndon").has("age", 35).toList();
        Assert.assertTrue(v.isEmpty());

        v = g.V().hasLabel("person").has("isCool", "sometimes").has("firstName", "lyndon").has("age", 100).toList();
        Assert.assertTrue(v.isEmpty());

        v = g.V().hasLabel("person").has("isCool", "always").has("firstName", "lyndon").has("age", 35).toList();
        Assert.assertTrue(v.isEmpty());

        v = g.V().hasLabel("pearson").has("isCool", "sometimes").has("firstName", "lyndon").has("age", 35).toList();
        Assert.assertTrue(v.isEmpty());

        v = g.V().hasLabel("person").has("isCool", TextP.endingWith("times")).has("firstName", "lyndon").has("age", 35).toList();
        Assert.assertEquals(1, v.size());
        Assert.assertEquals("lyndon", v.get(0).value("firstName"));
        Assert.assertEquals((Integer) 35, v.get(0).value("age"));
        Assert.assertEquals("sometimes", v.get(0).value("isCool"));
    }
}
