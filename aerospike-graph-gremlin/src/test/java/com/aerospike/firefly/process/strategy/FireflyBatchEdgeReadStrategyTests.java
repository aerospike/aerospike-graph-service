package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeReadSampleLimitStep;
import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeReadStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FireflyBatchEdgeReadStrategyTests {
    static final Configuration config;

    static {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
    }

    @Test
    public void verifyStrategyAppliedTest() {
        try (FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            GraphTraversal traversal = g.V(1).outE().where(__.inV().has((T.id), P.within(2, 3, 5)));
            traversal.asAdmin().applyStrategies();
            assertEquals(2, traversal.asAdmin().getSteps().size());
            assertTrue(traversal.asAdmin().getSteps().get(1) instanceof FireflyBatchEdgeReadStep);

            traversal = g.V(4).bothE().where(__.otherV().has((T.id), P.within(1, 2, 3)));
            traversal.asAdmin().applyStrategies();
            assertEquals(2, traversal.asAdmin().getSteps().size());
            assertTrue(traversal.asAdmin().getSteps().get(1) instanceof FireflyBatchEdgeReadStep);
        }
    }

    @Test
    public void verifyStrategyAppliedWithLimitAfterTest() {
        try (FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            GraphTraversal traversal = g.V(1).outE().where(__.inV().has((T.id), P.within(2, 3, 5))).limit(10);
            traversal.asAdmin().applyStrategies();
            assertEquals(3, traversal.asAdmin().getSteps().size());
            assertTrue(traversal.asAdmin().getSteps().get(1) instanceof FireflyBatchEdgeReadStep);
            assertTrue(traversal.asAdmin().getSteps().get(2) instanceof RangeGlobalStep);

            traversal = g.V(4).bothE().where(__.otherV().has((T.id), P.within(1, 2, 3))).limit(10);
            traversal.asAdmin().applyStrategies();
            assertEquals(3, traversal.asAdmin().getSteps().size());
            assertTrue(traversal.asAdmin().getSteps().get(1) instanceof FireflyBatchEdgeReadStep);
            assertTrue(traversal.asAdmin().getSteps().get(2) instanceof RangeGlobalStep);
        }
    }

    @Test
    public void verifyStrategyAppliedWithLimitBeforeTest() {
        try (FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            GraphTraversal traversal = g.V(1).outE().limit(10).where(__.inV().has((T.id), P.within(2, 3, 5)));
            traversal.asAdmin().applyStrategies();
            assertEquals(4, traversal.asAdmin().getSteps().size());
            assertTrue(traversal.asAdmin().getSteps().get(1) instanceof FireflyBatchEdgeReadSampleLimitStep);
            assertTrue(traversal.asAdmin().getSteps().get(2) instanceof RangeGlobalStep);
            assertTrue(traversal.asAdmin().getSteps().get(3) instanceof TraversalFilterStep);

            traversal = g.V(4).bothE().limit(10).where(__.otherV().has((T.id), P.within(1, 2, 3)));
            traversal.asAdmin().applyStrategies();
            assertEquals(4, traversal.asAdmin().getSteps().size());
            assertTrue(traversal.asAdmin().getSteps().get(1) instanceof FireflyBatchEdgeReadSampleLimitStep);
            assertTrue(traversal.asAdmin().getSteps().get(2) instanceof RangeGlobalStep);
            assertTrue(traversal.asAdmin().getSteps().get(3) instanceof TraversalFilterStep);
        }
    }

    @Test
    public void outInSuperNodeTest() {
        config.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, "1");
        try (FireflyGraph graph = FireflyGraph.open(config)) {
            graph.getBaseGraph().dropDatabase(graph, false);
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            final GraphTraversalSource g = graph.traversal();

            // should find edges connected to vertices 2 and 3
            final List<Map<Object, Object>> result = g.V(1).outE().where(__.inV().has((T.id), P.within(2, 3, 5))).elementMap().toList();

            assertEquals(2, result.size());
            assertEquals(1, ((Map) result.get(0).get(Direction.OUT)).get(T.id));
            assertEquals(1, ((Map) result.get(1).get(Direction.OUT)).get(T.id));
            final List<Integer> connections = new ArrayList<>();
            connections.add((Integer) ((Map) result.get(0).get(Direction.IN)).get(T.id));
            connections.add((Integer) ((Map) result.get(1).get(Direction.IN)).get(T.id));
            assertTrue(connections.contains(2));
            assertTrue(connections.contains(3));
        }
    }

    @Test
    public void bothSuperNodeTest() {
        config.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, "1");
        try (FireflyGraph graph = FireflyGraph.open(config)) {
            graph.getBaseGraph().dropDatabase(graph, false);
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            final GraphTraversalSource g = graph.traversal();

            // should find edges 1->4 and 4->3
            final List<Map<Object, Object>> result = g.V(4).bothE().where(__.otherV().has((T.id), P.within(1, 2, 3))).elementMap().toList();

            assertEquals(2, result.size());

            final int outId0 = (Integer) ((Map) result.get(0).get(Direction.OUT)).get(T.id);
            final int outId1 = (Integer) ((Map) result.get(1).get(Direction.OUT)).get(T.id);
            final int inId0 = (Integer) ((Map) result.get(0).get(Direction.IN)).get(T.id);
            final int inId1 = (Integer) ((Map) result.get(1).get(Direction.IN)).get(T.id);

            if (outId0 == 1) {
                assertEquals(4, inId0);
                assertEquals(4, outId1);
                assertEquals(3, inId1);
            } else if (outId0 == 4) {
                assertEquals(3, inId1);
                assertEquals(1, outId1);
                assertEquals(4, inId1);
            } else {
                fail("got unexpected edges.");
            }
        }
    }

    @Test
    public void bothSuperNodeTestLimitBefore() {
        config.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, "1");
        try (FireflyGraph graph = FireflyGraph.open(config)) {
            graph.getBaseGraph().dropDatabase(graph, false);
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            final GraphTraversalSource g = graph.traversal();

            // should find edges 1->4 and 4->3
            final List<Map<Object, Object>> result = g.V(4).bothE().limit(10).where(__.otherV().has((T.id), P.within(1, 2, 3))).elementMap().toList();

            assertEquals(2, result.size());

            final int outId0 = (Integer) ((Map) result.get(0).get(Direction.OUT)).get(T.id);
            final int outId1 = (Integer) ((Map) result.get(1).get(Direction.OUT)).get(T.id);
            final int inId0 = (Integer) ((Map) result.get(0).get(Direction.IN)).get(T.id);
            final int inId1 = (Integer) ((Map) result.get(1).get(Direction.IN)).get(T.id);

            if (outId0 == 1) {
                assertEquals(4, inId0);
                assertEquals(4, outId1);
                assertEquals(3, inId1);
            } else if (outId0 == 4) {
                assertEquals(3, inId1);
                assertEquals(1, outId1);
                assertEquals(4, inId1);
            } else {
                fail("got unexpected edges.");
            }
        }
    }

    @Test
    public void bothSuperNodeTestLimitAfter() {
        config.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, "1");
        try (FireflyGraph graph = FireflyGraph.open(config)) {
            graph.getBaseGraph().dropDatabase(graph, false);
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            final GraphTraversalSource g = graph.traversal();

            // should find edges 1->4 and 4->3
            final List<Map<Object, Object>> result = g.V(4).bothE().where(__.otherV().has((T.id), P.within(1, 2, 3))).limit(10).elementMap().toList();

            assertEquals(2, result.size());

            final int outId0 = (Integer) ((Map) result.get(0).get(Direction.OUT)).get(T.id);
            final int outId1 = (Integer) ((Map) result.get(1).get(Direction.OUT)).get(T.id);
            final int inId0 = (Integer) ((Map) result.get(0).get(Direction.IN)).get(T.id);
            final int inId1 = (Integer) ((Map) result.get(1).get(Direction.IN)).get(T.id);

            if (outId0 == 1) {
                assertEquals(4, inId0);
                assertEquals(4, outId1);
                assertEquals(3, inId1);
            } else if (outId0 == 4) {
                assertEquals(3, inId1);
                assertEquals(1, outId1);
                assertEquals(4, inId1);
            } else {
                fail("got unexpected edges.");
            }
        }
    }
}
