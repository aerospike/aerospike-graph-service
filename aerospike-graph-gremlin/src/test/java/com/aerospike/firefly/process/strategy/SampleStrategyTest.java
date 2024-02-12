package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeSampleLimitReadStep;
import com.aerospike.firefly.process.traversal.step.FireflyCompositeIdLimitSampleStep;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeReadStep;
import com.aerospike.firefly.process.traversal.step.FireflyCacheGCStep;
import com.aerospike.firefly.process.traversal.step.FireflyCompositeIdStep;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID;
import static com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromVertex.OutputType.VERTEX_ID;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT;

public class SampleStrategyTest {

    private static final int SUPERNODE_LOAD_SIZE = 500;
    private static final int RECORD_LIMIT = 100;
    private static final int LEAF_COUNT = 10;

    public void loadSimpleSupernode(final GraphTraversalSource g) {
        // Clear graph.
        g.V().drop().iterate();

        // Load data.
        final Vertex v1 = g.addV("entity").property("indexed", "value1").property("notindexed", "value2").next();
        for (int i = 0; i < SUPERNODE_LOAD_SIZE; i++) {
            final
            Vertex v2 = g.addV("entity-hop").
                    property("property1", "value1").
                    property("property2", "value2").
                    next();
            wait1Second();
            g.addE("has_entity").from(v1).to(v2).iterate();
            wait1Second();
            for (int j = 0; j < LEAF_COUNT; j++) {
                final Vertex v3 = g.addV("entity-hop2").
                        property("property1-2", "value1").
                        property("property2-2", "value2").
                        next();
                wait1Second();
                g.addE("has_entity2").from(v2).to(v3).iterate();
            }
        }
    }

    private void wait1Second() {
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void before() {
        // Force drop each time so that config changes don"t cause issues.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        AerospikeConnection.connect(config).dropDatabase(null, true);
    }

    @Test
    public void testSampleStrategy() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.index.vertex.properties", "indexed");
        config.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), RECORD_LIMIT);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            loadSimpleSupernode(g);
            final List<Vertex> vertices1 = g.V().has("indexed", "value1").out().sample(30).toList();
            final List<Vertex> vertices2 = g.V().has("property1", "value1").in().sample(30).toList();
            final List<Edge> edges1 = g.V().has("indexed", "value1").outE().sample(30).toList();
            final List<Edge> edges2 = g.V().has("property1", "value1").inE().sample(30).toList();
            Assert.assertEquals(30, vertices1.size());
            Assert.assertEquals(30, vertices2.size());
            Assert.assertEquals(30, edges1.size());
            Assert.assertEquals(30, edges2.size());

            final Set<Object> vertexIds1 = vertices1.stream().map(Element::id).collect(Collectors.toSet());
            final Set<Object> vertexIds2 = vertices2.stream().map(Element::id).collect(Collectors.toSet());

            // Graph structure is 1->many and many<-1 so this should hold true.
            Assert.assertEquals(30, vertexIds1.size());
            Assert.assertEquals(1, vertexIds2.size());

            final Set<Object> edgeIds1 = edges1.stream().map(Element::id).collect(Collectors.toSet());
            final Set<Object> edgeIds2 = edges2.stream().map(Element::id).collect(Collectors.toSet());

            // Edges are unique.
            Assert.assertEquals(30, edgeIds1.size());
            Assert.assertEquals(30, edgeIds2.size());

            final List<Vertex> vertices3 = g.V().has("property1", "value1").limit(30).in().sample(30).toList();
            final List<Vertex> vertices4 = g.V().has("property1", "value1").limit(29).in().sample(30).toList();
            final List<Vertex> vertices5 = g.V().has("property1", "value1").limit(31).in().sample(30).toList();
            Assert.assertEquals(30, vertices3.size());
            Assert.assertEquals(29, vertices4.size());
            Assert.assertEquals(30, vertices5.size());

            final List<Edge> edges3 = g.V().has("property1", "value1").limit(30).inE().sample(30).toList();
            final List<Edge> edges4 = g.V().has("property1", "value1").limit(29).inE().sample(30).toList();
            final List<Edge> edges5 = g.V().has("property1", "value1").limit(31).inE().sample(30).toList();
            Assert.assertEquals(30, edges3.size());
            Assert.assertEquals(29, edges4.size());
            Assert.assertEquals(30, edges5.size());

            final List<Vertex> allLeafVertex = g.V().has("indexed", "value1").out().sample(Integer.MAX_VALUE).out().sample(Integer.MAX_VALUE).toList();
            Assert.assertEquals(LEAF_COUNT * SUPERNODE_LOAD_SIZE, allLeafVertex.size());
            final Set<Vertex> allLeafVertexUnique = new HashSet<>(allLeafVertex);
            Assert.assertEquals(LEAF_COUNT * SUPERNODE_LOAD_SIZE, allLeafVertexUnique.size());
            for (final Vertex v : allLeafVertex) {
                // Make sure they are all the leaf vertices.
                Assert.assertEquals((Long) 0L, g.V(v.id()).out().count().next());
            }
        }
    }

    @Test
    public void testLimitStrategy() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.index.vertex.properties", "indexed");
        config.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), RECORD_LIMIT);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            loadSimpleSupernode(g);
            final List<Vertex> vertices1 = g.V().has("indexed", "value1").out().limit(30).toList();
            final List<Vertex> vertices2 = g.V().has("property1", "value1").in().limit(30).toList();
            final List<Edge> edges1 = g.V().has("indexed", "value1").outE().limit(30).toList();
            final List<Edge> edges2 = g.V().has("property1", "value1").inE().limit(30).toList();
            Assert.assertEquals(30, vertices1.size());
            Assert.assertEquals(30, vertices2.size());
            Assert.assertEquals(30, edges1.size());
            Assert.assertEquals(30, edges2.size());

            final Set<Object> vertexIds1 = vertices1.stream().map(Element::id).collect(Collectors.toSet());
            final Set<Object> vertexIds2 = vertices2.stream().map(Element::id).collect(Collectors.toSet());

            // Graph structure is 1->many and many<-1 so this should hold true.
            Assert.assertEquals(30, vertexIds1.size());
            Assert.assertEquals(1, vertexIds2.size());

            final Set<Object> edgeIds1 = edges1.stream().map(Element::id).collect(Collectors.toSet());
            final Set<Object> edgeIds2 = edges2.stream().map(Element::id).collect(Collectors.toSet());

            // Edges are unique.
            Assert.assertEquals(30, edgeIds1.size());
            Assert.assertEquals(30, edgeIds2.size());

            final List<Vertex> vertices3 = g.V().has("property1", "value1").limit(30).in().limit(30).toList();
            final List<Vertex> vertices4 = g.V().has("property1", "value1").limit(29).in().limit(30).toList();
            final List<Vertex> vertices5 = g.V().has("property1", "value1").limit(31).in().limit(30).toList();
            Assert.assertEquals(30, vertices3.size());
            Assert.assertEquals(29, vertices4.size());
            Assert.assertEquals(30, vertices5.size());

            final List<Edge> edges3 = g.V().has("property1", "value1").limit(30).inE().limit(30).toList();
            final List<Edge> edges4 = g.V().has("property1", "value1").limit(29).inE().limit(30).toList();
            final List<Edge> edges5 = g.V().has("property1", "value1").limit(31).inE().limit(30).toList();
            Assert.assertEquals(30, edges3.size());
            Assert.assertEquals(29, edges4.size());
            Assert.assertEquals(30, edges5.size());

            final List<Vertex> allLeafVertex = g.V().has("indexed", "value1").out().limit(Integer.MAX_VALUE).out().limit(Integer.MAX_VALUE).toList();
            Assert.assertEquals(LEAF_COUNT * SUPERNODE_LOAD_SIZE, allLeafVertex.size());
            final Set<Vertex> allLeafVertexUnique = new HashSet<>(allLeafVertex);
            Assert.assertEquals(LEAF_COUNT * SUPERNODE_LOAD_SIZE, allLeafVertexUnique.size());
            for (final Vertex v : allLeafVertex) {
                // Make sure they are all the leaf vertices.
                Assert.assertEquals((Long) 0L, g.V(v.id()).out().count().next());
            }
        }
    }

    @Test
    public void testSupernodeVertexDirectWithAdjacencyIndex() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.index.vertex.properties", "indexed");
        config.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), RECORD_LIMIT);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            loadSimpleSupernode(g);

            final FireflyVertex supernode = (FireflyVertex) g.V().has("indexed", "value1").next();

            final List<FireflyId> supernodeEdgeIds = supernode.getSupernodeIds(Direction.OUT, Set.of(), EDGE_ID);
            Assert.assertEquals(SUPERNODE_LOAD_SIZE - RECORD_LIMIT, supernodeEdgeIds.size());
            Assert.assertFalse(supernodeEdgeIds.stream().anyMatch(Objects::isNull));

            final List<FireflyEdge> supernodeEdges = graph.readEdges(List.of(), supernodeEdgeIds);
            Assert.assertEquals(SUPERNODE_LOAD_SIZE - RECORD_LIMIT, supernodeEdges.size());
            Assert.assertFalse(supernodeEdges.stream().anyMatch(Objects::isNull));

            final List<FireflyId> supernodeVertexIds = supernode.getSupernodeIds(Direction.OUT, Set.of(), VERTEX_ID);
            Assert.assertEquals(SUPERNODE_LOAD_SIZE - RECORD_LIMIT, supernodeVertexIds.size());
            Assert.assertFalse(supernodeVertexIds.stream().anyMatch(Objects::isNull));

            supernodeVertexIds.sort(Comparator.comparing(FireflyId::toString));
            final List<FireflyVertex> supernodeVertices = graph.readVertices(List.of(), supernodeVertexIds);
            Assert.assertEquals(SUPERNODE_LOAD_SIZE - RECORD_LIMIT, supernodeVertices.size());
            Assert.assertFalse(supernodeVertices.stream().anyMatch(Objects::isNull));

            final List<FireflyId> allEdgeIds = supernode.getEdgeIdsFromVertex(Direction.OUT, Set.of());
            Assert.assertEquals(SUPERNODE_LOAD_SIZE, allEdgeIds.size());
            Assert.assertFalse(allEdgeIds.stream().anyMatch(Objects::isNull));

            final List<FireflyEdge> allEdges = graph.readEdges(List.of(), allEdgeIds);
            Assert.assertEquals(SUPERNODE_LOAD_SIZE, allEdges.size());
            Assert.assertFalse(allEdges.stream().anyMatch(Objects::isNull));

            final List<FireflyId> allVertexIds = supernode.getVertexIdsFromVertex(Direction.OUT, Set.of());
            Assert.assertEquals(SUPERNODE_LOAD_SIZE, allVertexIds.size());
            Assert.assertFalse(allVertexIds.stream().anyMatch(Objects::isNull));
            Assert.assertEquals(SUPERNODE_LOAD_SIZE, new HashSet<>(allVertexIds).size());

            final List<FireflyVertex> allVertices = graph.readVertices(List.of(), allVertexIds);
            Assert.assertEquals(SUPERNODE_LOAD_SIZE, allVertices.size());
            Assert.assertFalse(allVertices.stream().anyMatch(Objects::isNull));
            Assert.assertEquals(SUPERNODE_LOAD_SIZE, new HashSet<>(allVertices).size());
        }
    }

    @Test
    public void testStrategyApplicationSample(){
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.index.vertex.properties", "indexed");
        config.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), RECORD_LIMIT);

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            final GraphTraversal<Vertex, Vertex> traversalOutSample = g.V().out().sample(1);
            traversalOutSample.asAdmin().applyStrategies();
            List<Step> stepsOutSample = traversalOutSample.asAdmin().getSteps();
            assertStepsSample(stepsOutSample, true, false, true);

            final GraphTraversal<Vertex, Vertex> traversalOutHasSample = g.V().out().has("foo", "bar").sample(1);
            traversalOutHasSample.asAdmin().applyStrategies();
            List<Step> stepsOutHasSample = traversalOutHasSample.asAdmin().getSteps();
            assertStepsSample(stepsOutHasSample, false, true, true);

            final GraphTraversal<Vertex, Vertex> traversalHasOutSample = g.V().out().sample(1).has("foo", "bar");
            traversalHasOutSample.asAdmin().applyStrategies();
            List<Step> stepsHasOutSample = traversalHasOutSample.asAdmin().getSteps();
            assertStepsSample(stepsHasOutSample, true, true, true);

            final GraphTraversal<Vertex, Vertex> traversalInSample = g.V().in().sample(1);
            traversalInSample.asAdmin().applyStrategies();
            List<Step> stepsInSample = traversalInSample.asAdmin().getSteps();
            assertStepsSample(stepsInSample, true, false, true);

            final GraphTraversal<Vertex, Vertex> traversalInHasSample = g.V().in().has("foo", "bar").sample(1);
            traversalInHasSample.asAdmin().applyStrategies();
            List<Step> stepsInHasSample = traversalInHasSample.asAdmin().getSteps();
            assertStepsSample(stepsInHasSample, false, true, true);

            final GraphTraversal<Vertex, Vertex> traversalHasInSample = g.V().in().sample(1).has("foo", "bar");
            traversalHasInSample.asAdmin().applyStrategies();
            List<Step> stepsHasInSample = traversalHasInSample.asAdmin().getSteps();
            assertStepsSample(stepsHasInSample, true, true, true);

            final GraphTraversal<Vertex, Edge> traversalOutESample = g.V().outE().sample(1);
            traversalOutESample.asAdmin().applyStrategies();
            List<Step> stepsOutESample = traversalOutESample.asAdmin().getSteps();
            assertStepsSample(stepsOutESample, true, false, false);

            final GraphTraversal<Vertex, Edge> traversalOutEHasSample = g.V().outE().has("foo", "bar").sample(1);
            traversalOutEHasSample.asAdmin().applyStrategies();
            List<Step> stepsOutEHasSample = traversalOutEHasSample.asAdmin().getSteps();
            assertStepsSample(stepsOutEHasSample, false, true, false);

            final GraphTraversal<Vertex, Edge> traversalHasOutESample = g.V().outE().sample(1).has("foo", "bar");
            traversalHasOutESample.asAdmin().applyStrategies();
            List<Step> stepsHasOutESample = traversalHasOutESample.asAdmin().getSteps();
            assertStepsSample(stepsHasOutESample, true, true, false);

            final GraphTraversal<Vertex, Edge> traversalInESample = g.V().inE().sample(1);
            traversalInESample.asAdmin().applyStrategies();
            List<Step> stepsInESample = traversalInESample.asAdmin().getSteps();
            assertStepsSample(stepsInESample, true, false, false);

            final GraphTraversal<Vertex, Edge> traversalInEHasSample = g.V().inE().has("foo", "bar").sample(1);
            traversalInEHasSample.asAdmin().applyStrategies();
            List<Step> stepsInEHasSample = traversalInEHasSample.asAdmin().getSteps();
            assertStepsSample(stepsInEHasSample, false, true, false);

            final GraphTraversal<Vertex, Edge> traversalHasInESample = g.V().inE().sample(1).has("foo", "bar");
            traversalHasInESample.asAdmin().applyStrategies();
            List<Step> stepsHasInESample = traversalHasInESample.asAdmin().getSteps();
            assertStepsSample(stepsHasInESample, true, true, false);
        }
    }

    @Test
    public void testStrategyApplicationLimit(){
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.index.vertex.properties", "indexed");
        config.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), RECORD_LIMIT);

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            final GraphTraversal<Vertex, Vertex> traversalOutLimit = g.V().out().limit(1);
            traversalOutLimit.asAdmin().applyStrategies();
            List<Step> stepsOutLimit = traversalOutLimit.asAdmin().getSteps();
            assertStepsLimit(stepsOutLimit, true, false, true);

            final GraphTraversal<Vertex, Vertex> traversalOutHasLimit = g.V().out().has("foo", "bar").limit(1);
            traversalOutHasLimit.asAdmin().applyStrategies();
            List<Step> stepsOutHasLimit = traversalOutHasLimit.asAdmin().getSteps();
            assertStepsLimit(stepsOutHasLimit, false, true, true);

            final GraphTraversal<Vertex, Vertex> traversalHasOutLimit = g.V().out().limit(1).has("foo", "bar");
            traversalHasOutLimit.asAdmin().applyStrategies();
            List<Step> stepsHasOutLimit = traversalHasOutLimit.asAdmin().getSteps();
            assertStepsLimit(stepsHasOutLimit, true, true, true);

            final GraphTraversal<Vertex, Vertex> traversalInLimit = g.V().in().limit(1);
            traversalInLimit.asAdmin().applyStrategies();
            List<Step> stepsInLimit = traversalInLimit.asAdmin().getSteps();
            assertStepsLimit(stepsInLimit, true, false, true);

            final GraphTraversal<Vertex, Vertex> traversalInHasLimit = g.V().in().has("foo", "bar").limit(1);
            traversalInHasLimit.asAdmin().applyStrategies();
            List<Step> stepsInHasLimit = traversalInHasLimit.asAdmin().getSteps();
            assertStepsLimit(stepsInHasLimit, false, true, true);

            final GraphTraversal<Vertex, Vertex> traversalHasInLimit = g.V().in().limit(1).has("foo", "bar");
            traversalHasInLimit.asAdmin().applyStrategies();
            List<Step> stepsHasInLimit = traversalHasInLimit.asAdmin().getSteps();
            assertStepsLimit(stepsHasInLimit, true, true, true);

            final GraphTraversal<Vertex, Edge> traversalOutELimit = g.V().outE().limit(1);
            traversalOutELimit.asAdmin().applyStrategies();
            List<Step> stepsOutELimit = traversalOutELimit.asAdmin().getSteps();
            assertStepsLimit(stepsOutELimit, true, false, false);

            final GraphTraversal<Vertex, Edge> traversalOutEHasLimit = g.V().outE().has("foo", "bar").limit(1);
            traversalOutEHasLimit.asAdmin().applyStrategies();
            List<Step> stepsOutEHasLimit = traversalOutEHasLimit.asAdmin().getSteps();
            assertStepsLimit(stepsOutEHasLimit, false, true, false);

            final GraphTraversal<Vertex, Edge> traversalHasOutELimit = g.V().outE().limit(1).has("foo", "bar");
            traversalHasOutELimit.asAdmin().applyStrategies();
            List<Step> stepsHasOutELimit = traversalHasOutELimit.asAdmin().getSteps();
            assertStepsLimit(stepsHasOutELimit, true, true, false);

            final GraphTraversal<Vertex, Edge> traversalInELimit = g.V().inE().limit(1);
            traversalInELimit.asAdmin().applyStrategies();
            List<Step> stepsInELimit = traversalInELimit.asAdmin().getSteps();
            assertStepsLimit(stepsInELimit, true, false, false);

            final GraphTraversal<Vertex, Edge> traversalInEHasLimit = g.V().inE().has("foo", "bar").limit(1);
            traversalInEHasLimit.asAdmin().applyStrategies();
            List<Step> stepsInEHasLimit = traversalInEHasLimit.asAdmin().getSteps();
            assertStepsLimit(stepsInEHasLimit, false, true, false);

            final GraphTraversal<Vertex, Edge> traversalHasInELimit = g.V().inE().limit(1).has("foo", "bar");
            traversalHasInELimit.asAdmin().applyStrategies();
            List<Step> stepsHasInELimit = traversalHasInELimit.asAdmin().getSteps();
            assertStepsLimit(stepsHasInELimit, true, true, false);

            final GraphTraversal<Vertex, Edge> traversalHasInESkip = g.V().inE().skip(1).has("foo", "bar");
            traversalHasInESkip.asAdmin().applyStrategies();
            List<Step> stepsHasInELimitSkip = traversalHasInESkip.asAdmin().getSteps();

            // Doesn't apply with skip(). (This is required since it is also a RangeGlobalStep with limit()).
            Assert.assertTrue(stepsHasInELimitSkip.get(0) instanceof FireflyGraphStep);
            Assert.assertTrue(stepsHasInELimitSkip.get(1) instanceof FireflyBatchEdgeReadStep);
            Assert.assertTrue(stepsHasInELimitSkip.get(2) instanceof RangeGlobalStep);
            Assert.assertTrue(stepsHasInELimitSkip.get(3) instanceof HasStep);

            final GraphTraversal<Vertex, Vertex> traversalHasInSkip = g.V().in().skip(1).has("foo", "bar");
            traversalHasInSkip.asAdmin().applyStrategies();
            List<Step> stepsHasInLimitSkip = traversalHasInSkip.asAdmin().getSteps();

            // Doesn't apply with skip(). (This is required since it is also a RangeGlobalStep with limit()).
            Assert.assertTrue(stepsHasInLimitSkip.get(0) instanceof FireflyGraphStep);
            Assert.assertTrue(stepsHasInLimitSkip.get(1) instanceof FireflyCompositeIdStep);
            Assert.assertTrue(stepsHasInLimitSkip.get(2) instanceof RangeGlobalStep);
            Assert.assertTrue(stepsHasInLimitSkip.get(3) instanceof HasStep);
        }
    }

    public void assertStepsSample(List<Step> steps, final boolean sampleFirst, final boolean hasHas, final boolean isVertex) {
        if (!hasHas) {
            // Graph step, composite id step, limit step, cache step.
            Assert.assertEquals(4, steps.size());
            Assert.assertTrue(steps.get(2) instanceof RangeGlobalStep);
            Assert.assertTrue(steps.get(3) instanceof FireflyCacheGCStep);
        } else if (!sampleFirst) {
            if (!isVertex) {
                Assert.assertEquals(5, steps.size());
                // Graph step, has step, limit step, has step, cache step.
                Assert.assertTrue(steps.get(2) instanceof HasStep);
                Assert.assertTrue(steps.get(3) instanceof SampleGlobalStep);
                Assert.assertTrue(steps.get(4) instanceof FireflyCacheGCStep);
            } else {
                Assert.assertEquals(4, steps.size());
                // Graph step, composite id step, limit step, has step, cache step.
                Assert.assertTrue(steps.get(2) instanceof SampleGlobalStep);
                Assert.assertTrue(steps.get(3) instanceof FireflyCacheGCStep);
            }
        } else {
            // Graph step, composite id step, has step, sample step, cache step.
            Assert.assertEquals(5, steps.size());
            Assert.assertTrue(steps.get(2) instanceof RangeGlobalStep);
            Assert.assertTrue(steps.get(3) instanceof HasStep);
            Assert.assertTrue(steps.get(4) instanceof FireflyCacheGCStep);
        }
        Assert.assertTrue(steps.get(0) instanceof FireflyGraphStep);
        if (isVertex) {
            if (sampleFirst) {
                Assert.assertTrue(steps.get(1) instanceof FireflyCompositeIdLimitSampleStep);
            } else {
                Assert.assertTrue(steps.get(1) instanceof FireflyCompositeIdStep);
            }
        } else {
            if (sampleFirst) {
                Assert.assertTrue(steps.get(1) instanceof FireflyBatchEdgeSampleLimitReadStep);
            } else {
                Assert.assertTrue(steps.get(1) instanceof FireflyBatchEdgeReadStep);
            }
        }
    }

    public void assertStepsLimit(List<Step> steps, final boolean limitFirst, final boolean hasHas, final boolean isVertex) {
        if (!hasHas) {
            // Graph step, composite id step, limit step, cache step.
            Assert.assertEquals(4, steps.size());
            Assert.assertTrue(steps.get(2) instanceof RangeGlobalStep);
            Assert.assertTrue(steps.get(3) instanceof FireflyCacheGCStep);
        } else if (!limitFirst) {
            if (!isVertex) {
                Assert.assertEquals(5, steps.size());
                // Graph step, has step, limit step, has step, cache step.
                Assert.assertTrue(steps.get(2) instanceof HasStep);
                Assert.assertTrue(steps.get(3) instanceof RangeGlobalStep);
                Assert.assertTrue(steps.get(4) instanceof FireflyCacheGCStep);
            } else {
                Assert.assertEquals(4, steps.size());
                // Graph step, composite id step, limit step, has step, cache step.
                Assert.assertTrue(steps.get(2) instanceof RangeGlobalStep);
                Assert.assertTrue(steps.get(3) instanceof FireflyCacheGCStep);
            }
        } else {
            // Graph step, composite id step, has step, sample step, cache step.
            Assert.assertEquals(5, steps.size());
            Assert.assertTrue(steps.get(2) instanceof RangeGlobalStep);
            Assert.assertTrue(steps.get(3) instanceof HasStep);
            Assert.assertTrue(steps.get(4) instanceof FireflyCacheGCStep);
        }
        Assert.assertTrue(steps.get(0) instanceof FireflyGraphStep);
        if (isVertex) {
            if (limitFirst) {
                Assert.assertTrue(steps.get(1) instanceof FireflyCompositeIdLimitSampleStep);
            } else {
                Assert.assertTrue(steps.get(1) instanceof FireflyCompositeIdStep);
            }
        } else {
            if (limitFirst) {
                Assert.assertTrue(steps.get(1) instanceof FireflyBatchEdgeSampleLimitReadStep);
            } else {
                Assert.assertTrue(steps.get(1) instanceof FireflyBatchEdgeReadStep);
            }
        }
    }
}
