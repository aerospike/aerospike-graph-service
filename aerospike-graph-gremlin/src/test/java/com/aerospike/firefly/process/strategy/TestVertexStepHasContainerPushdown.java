package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeReadStep;
import com.aerospike.firefly.process.traversal.step.FireflyCompositeIdStep;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyContentionHandlingStrategy;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

// TODO GRAPH-401: While we have hacks to get around the fact that we cannot filter our cache with a hasContainer
//  this test is not valid. Remove @Ignore when this is fixed.
@Ignore
public class TestVertexStepHasContainerPushdown extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testCompositeIdVertexStepHasContainerPushdownAerospike() {
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final GraphTraversal<?, ?> traversal = g.V().has("name", "marko").out().has("age", 29).has("foo", "blah");
        FireflyContentionHandlingStrategy strategy = new FireflyContentionHandlingStrategy();
        strategy.apply(traversal.asAdmin());
        final List<Step> steps = traversal.asAdmin().getSteps();

        boolean found = false;
        for (final Step step : steps) {
            if (step instanceof FireflyCompositeIdStep) {
                // Should only be found once.
                Assert.assertFalse(found);

                found = true;
                final FireflyCompositeIdStep compositeIdStep = (FireflyCompositeIdStep) step;
                Assert.assertEquals(2, compositeIdStep.aerospikeHasContainers.size());
                Assert.assertEquals(0, compositeIdStep.fireflyHasContainers.size());
            } else if (found) {
                // Expect all has steps after composite id to be pulled into composite id.
                Assert.assertFalse(step instanceof HasStep);
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testCompositeIdVertexStepHasContainerPushdownFirefly() {
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final GraphTraversal<?, ?> traversal = g.V().has("name", "marko").out().has("age", TextP.endingWith("old"));
        FireflyContentionHandlingStrategy strategy = new FireflyContentionHandlingStrategy();
        strategy.apply(traversal.asAdmin());
        final List<Step> steps = traversal.asAdmin().getSteps();

        boolean found = false;
        for (final Step step : steps) {
            if (step instanceof FireflyCompositeIdStep) {
                // Should only be found once.
                Assert.assertFalse(found);

                found = true;
                final FireflyCompositeIdStep compositeIdStep = (FireflyCompositeIdStep) step;
                Assert.assertEquals(0, compositeIdStep.aerospikeHasContainers.size());
                Assert.assertEquals(1, compositeIdStep.fireflyHasContainers.size());
            } else if (found) {
                // Expect all has steps after composite id to be pulled into composite id.
                Assert.assertFalse(step instanceof HasStep);
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testCompositeIdVertexStepHasContainerPushdownMix() {
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final GraphTraversal<?, ?> traversal = g.V().has("name", "marko").out().has("age", TextP.endingWith("old")).has("foo", P.gt(1));
        FireflyContentionHandlingStrategy strategy = new FireflyContentionHandlingStrategy();
        strategy.apply(traversal.asAdmin());
        final List<Step> steps = traversal.asAdmin().getSteps();

        boolean found = false;
        for (final Step step : steps) {
            if (step instanceof FireflyCompositeIdStep) {
                // Should only be found once.
                Assert.assertFalse(found);

                found = true;
                final FireflyCompositeIdStep compositeIdStep = (FireflyCompositeIdStep) step;
                Assert.assertEquals(1, compositeIdStep.aerospikeHasContainers.size());
                Assert.assertEquals(1, compositeIdStep.fireflyHasContainers.size());
            } else if (found) {
                // Expect all has steps after composite id to be pulled into composite id.
                Assert.assertFalse(step instanceof HasStep);
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testCompositeIdVertexStepHasContainerPushdownMixLabelOmit() {
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final GraphTraversal<?, ?> traversal = g.V().has("name", "marko").out().as("a").has("age", TextP.endingWith("old")).has("foo", P.gt(1));
        FireflyContentionHandlingStrategy strategy = new FireflyContentionHandlingStrategy();
        strategy.apply(traversal.asAdmin());
        final List<Step> steps = traversal.asAdmin().getSteps();

        boolean found = false;
        boolean foundSecond = false;
        for (final Step step : steps) {
            if (step instanceof FireflyCompositeIdStep) {
                // Should only be found once.
                Assert.assertFalse(found);

                found = true;
                final FireflyCompositeIdStep compositeIdStep = (FireflyCompositeIdStep) step;
                Assert.assertEquals(0, compositeIdStep.aerospikeHasContainers.size());
                Assert.assertEquals(0, compositeIdStep.fireflyHasContainers.size());
            } else if (found && !foundSecond) {
                // Expect all has steps after composite id to be pulled into composite id.
                Assert.assertTrue(step instanceof HasStep);
                foundSecond = true;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testBatchEdgeStepHasContainerPushdownAerospike() {
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final GraphTraversal<?, ?> traversal = g.V().has("name", "marko").outE().has("age", 29).has("foo", "blah");
        FireflyContentionHandlingStrategy strategy = new FireflyContentionHandlingStrategy();
        strategy.apply(traversal.asAdmin());
        final List<Step> steps = traversal.asAdmin().getSteps();

        boolean found = false;
        for (final Step step : steps) {
            if (step instanceof FireflyBatchEdgeReadStep) {
                // Should only be found once.
                Assert.assertFalse(found);

                found = true;
                final FireflyBatchEdgeReadStep batchEdgeReadStep = (FireflyBatchEdgeReadStep) step;
                Assert.assertEquals(2, batchEdgeReadStep.aerospikeHasContainers.size());
                Assert.assertEquals(0, batchEdgeReadStep.fireflyHasContainers.size());
            } else if (found) {
                // Expect all has steps after composite id to be pulled into composite id.
                Assert.assertFalse(step instanceof HasStep);
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testBatchEdgeStepHasContainerPushdownFirefly() {
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final GraphTraversal<?, ?> traversal = g.V().has("name", "marko").outE().has("age", TextP.endingWith("old"));
        FireflyContentionHandlingStrategy strategy = new FireflyContentionHandlingStrategy();
        strategy.apply(traversal.asAdmin());
        final List<Step> steps = traversal.asAdmin().getSteps();

        boolean found = false;
        for (final Step step : steps) {
            if (step instanceof FireflyBatchEdgeReadStep) {
                // Should only be found once.
                Assert.assertFalse(found);

                found = true;
                final FireflyBatchEdgeReadStep batchEdgeReadStep = (FireflyBatchEdgeReadStep) step;
                Assert.assertEquals(0, batchEdgeReadStep.aerospikeHasContainers.size());
                Assert.assertEquals(1, batchEdgeReadStep.fireflyHasContainers.size());
            } else if (found) {
                // Expect all has steps after composite id to be pulled into composite id.
                Assert.assertFalse(step instanceof HasStep);
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testBatchEdgeStepHasContainerPushdownMix() {
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final GraphTraversal<?, ?> traversal = g.V().has("name", "marko").outE().has("age", TextP.endingWith("old")).has("foo", P.gt(1));
        FireflyContentionHandlingStrategy strategy = new FireflyContentionHandlingStrategy();
        strategy.apply(traversal.asAdmin());
        final List<Step> steps = traversal.asAdmin().getSteps();

        boolean found = false;
        for (final Step step : steps) {
            if (step instanceof FireflyBatchEdgeReadStep) {
                // Should only be found once.
                Assert.assertFalse(found);

                found = true;
                final FireflyBatchEdgeReadStep batchEdgeReadStep = (FireflyBatchEdgeReadStep) step;
                Assert.assertEquals(1, batchEdgeReadStep.aerospikeHasContainers.size());
                Assert.assertEquals(1, batchEdgeReadStep.fireflyHasContainers.size());
            } else if (found) {
                // Expect all has steps after composite id to be pulled into composite id.
                Assert.assertFalse(step instanceof HasStep);
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testBatchEdgeStepHasContainerPushdownMixLabelOmit() {
        final Graph tg = TinkerFactory.createModern();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final GraphTraversal<?, ?> traversal = g.V().has("name", "marko").outE().as("a").has("age", TextP.endingWith("old")).has("foo", P.gt(1));
        FireflyContentionHandlingStrategy strategy = new FireflyContentionHandlingStrategy();
        strategy.apply(traversal.asAdmin());
        final List<Step> steps = traversal.asAdmin().getSteps();

        boolean found = false;
        boolean foundSecond = false;
        for (final Step step : steps) {
            if (step instanceof FireflyBatchEdgeReadStep) {
                // Should only be found once.
                Assert.assertFalse(found);

                found = true;
                final FireflyBatchEdgeReadStep batchEdgeReadStep = (FireflyBatchEdgeReadStep) step;
                Assert.assertEquals(0, batchEdgeReadStep.aerospikeHasContainers.size());
                Assert.assertEquals(0, batchEdgeReadStep.fireflyHasContainers.size());
            } else if (found && !foundSecond) {
                // Expect all has steps after composite id to be pulled into composite id.
                Assert.assertTrue(step instanceof HasStep);
                foundSecond = true;
            }
        }
        Assert.assertTrue(found);
    }
}
