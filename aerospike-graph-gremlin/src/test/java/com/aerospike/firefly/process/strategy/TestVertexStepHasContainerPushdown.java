package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeReadStep;
import com.aerospike.firefly.process.traversal.step.FireflyBatchVertexReadStep;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestVertexStepHasContainerPushdown extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testCompositeIdVertexStepHasContainerPushdownAerospike() {
        final Graph tg = TinkerFactory.createModern();
        graph.traversal().V().drop().iterate();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final GraphTraversal<?, ?> traversal = g.V().has("name", "marko").out().has("age", 29).has("foo", "blah");
        traversal.asAdmin().applyStrategies();
        final List<Step> steps = traversal.asAdmin().getSteps();

        boolean found = false;
        for (final Step step : steps) {
            if (step instanceof FireflyBatchVertexReadStep) {
                // Should only be found once.
                Assert.assertFalse(found);

                found = true;
                final FireflyBatchVertexReadStep compositeIdStep = (FireflyBatchVertexReadStep) step;
                // Expect both b/c cache.
                Assert.assertEquals(2, compositeIdStep.aerospikeHasContainers.size());
                Assert.assertEquals(2, compositeIdStep.fireflyHasContainers.size());
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
        graph.traversal().V().drop().iterate();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final GraphTraversal<?, ?> traversal = g.V().has("name", "marko").out().has("age", TextP.endingWith("old"));
        traversal.asAdmin().applyStrategies();
        final List<Step> steps = traversal.asAdmin().getSteps();

        boolean found = false;
        for (final Step step : steps) {
            if (step instanceof FireflyBatchVertexReadStep) {
                // Should only be found once.
                Assert.assertFalse(found);

                found = true;
                final FireflyBatchVertexReadStep compositeIdStep = (FireflyBatchVertexReadStep) step;
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
        graph.traversal().V().drop().iterate();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final GraphTraversal<?, ?> traversal = g.V().has("name", "marko").out().has("age", TextP.endingWith("old")).has("foo", P.gt(1));
        traversal.asAdmin().applyStrategies();
        final List<Step> steps = traversal.asAdmin().getSteps();

        boolean found = false;
        for (final Step step : steps) {
            if (step instanceof FireflyBatchVertexReadStep) {
                // Should only be found once.
                Assert.assertFalse(found);

                found = true;
                final FireflyBatchVertexReadStep compositeIdStep = (FireflyBatchVertexReadStep) step;
                Assert.assertEquals(1, compositeIdStep.aerospikeHasContainers.size());
                // Should have both.
                Assert.assertEquals(2, compositeIdStep.fireflyHasContainers.size());
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
        graph.traversal().V().drop().iterate();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final GraphTraversal<?, ?> traversal = g.V().has("name", "marko").out().as("a").has("age", TextP.endingWith("old")).has("foo", P.gt(1));
        traversal.asAdmin().getStrategies().removeStrategies(FilterRankingStrategy.class);
        traversal.asAdmin().applyStrategies();
        final List<Step> steps = traversal.asAdmin().getSteps();

        boolean found = false;
        boolean foundSecond = false;
        for (final Step step : steps) {
            if (step instanceof FireflyBatchVertexReadStep) {
                // Should only be found once.
                Assert.assertFalse(found);

                found = true;
                final FireflyBatchVertexReadStep compositeIdStep = (FireflyBatchVertexReadStep) step;
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
        graph.traversal().V().drop().iterate();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final GraphTraversal<?, ?> traversal = g.V().has("name", "marko").outE().has("age", 29).has("foo", "blah");
        traversal.asAdmin().applyStrategies();
        final List<Step> steps = traversal.asAdmin().getSteps();

        boolean found = false;
        for (final Step step : steps) {
            if (step instanceof FireflyBatchEdgeReadStep) {
                // Should only be found once.
                Assert.assertFalse(found);

                found = true;
                // Edges do not support pushdown of Aerospike containers.
                final FireflyBatchEdgeReadStep batchEdgeReadStep = (FireflyBatchEdgeReadStep) step;
                Assert.assertEquals(0, batchEdgeReadStep.aerospikeHasContainers.size());
                Assert.assertEquals(2, batchEdgeReadStep.fireflyHasContainers.size());
            } else if (found) {
                // Expect all Has step to not be pulled into batch edge read.
                Assert.assertTrue(step instanceof HasStep);
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testBatchEdgeStepHasContainerPushdownFirefly() {
        final Graph tg = TinkerFactory.createModern();
        graph.traversal().V().drop().iterate();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final GraphTraversal<?, ?> traversal = g.V().has("name", "marko").outE().has("age", TextP.endingWith("old"));
        traversal.asAdmin().applyStrategies();
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
                // Expect all Has step to not be pulled into batch edge read.
                Assert.assertTrue(step instanceof HasStep);
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testOutE() {
        final Graph tg = TinkerFactory.createModern();
        graph.traversal().V().drop().iterate();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final List<Edge> e = g.V().has("name", "marko").outE().has("foo", 1).toList();
    }

    @Test
    public void testBatchEdgeStepHasContainerPushdownMix() {
        final Graph tg = TinkerFactory.createModern();
        graph.traversal().V().drop().iterate();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final GraphTraversal<?, ?> traversal = g.V().has("name", "marko").outE().has("age", TextP.endingWith("old")).has("foo", P.gt(1));
        traversal.asAdmin().applyStrategies();
        final List<Step> steps = traversal.asAdmin().getSteps();

        boolean found = false;
        for (final Step step : steps) {
            if (step instanceof FireflyBatchEdgeReadStep) {
                // Should only be found once.
                Assert.assertFalse(found);

                found = true;
                final FireflyBatchEdgeReadStep batchEdgeReadStep = (FireflyBatchEdgeReadStep) step;
                Assert.assertEquals(0, batchEdgeReadStep.aerospikeHasContainers.size());
                Assert.assertEquals(2, batchEdgeReadStep.fireflyHasContainers.size());
            } else if (found) {
                // Expect all Has step to not be pulled into batch edge read.
                Assert.assertTrue(step instanceof HasStep);
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testBatchEdgeStepHasContainerPushdownMixLabelOmit() {
        final Graph tg = TinkerFactory.createModern();
        graph.traversal().V().drop().iterate();
        GraphHelper.cloneElements(tg, graph);

        final GraphTraversalSource g = graph.traversal();
        final GraphTraversal<?, ?> traversal = g.V().has("name", "marko").outE().as("a").has("age", TextP.endingWith("old")).has("foo", P.gt(1));
        traversal.asAdmin().getStrategies().removeStrategies(FilterRankingStrategy.class);
        traversal.asAdmin().applyStrategies();
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
