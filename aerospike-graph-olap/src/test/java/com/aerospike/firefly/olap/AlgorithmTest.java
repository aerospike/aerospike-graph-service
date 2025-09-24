package com.aerospike.firefly.olap;

import com.aerospike.firefly.olap.config.QueryParameters;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.clustering.connected.ConnectedComponentVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRank;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PeerPressure;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.Order.desc;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.valueMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AlgorithmTest {
    private Configuration config;

    @Before
    public void beforeEach() {
        config = ConfigurationHelper.loadFromFile(Tokens.INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
    }

    @Test
    public void testPageRank() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Vertex> output = graph.traversal()
                    .withComputer().with(QueryParameters.ALLOW_UNFILTERED_ALGORITHM, true)
                    .with("aerospike.graph.analytics.temp.write.disabled", true)
                    .V().pageRank()
                    .toList();

            assertEquals(6L, output.size());
            final Vertex v1 = output.stream().filter(v -> v.id().equals(1)).findFirst().get();
            //precision is PageRankProgram.epsilon
            assertEquals(0.113755d, v1.value(PageRankVertexProgram.PAGE_RANK), 0.00001d);

            // verify saved data
            final Map<Object, Object> savedV1 = graph.traversal().V(1).elementMap().next();
            assertEquals(0.113755d, (Double) savedV1.get(PageRankVertexProgram.PAGE_RANK), 0.00001d);
        }
    }

    @Test
    public void testPageRankWithEdgeFilterByDirection() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Vertex> output = graph.traversal()
                    .withComputer().with(QueryParameters.ALLOW_UNFILTERED_ALGORITHM, true)
                    .with("aerospike.graph.analytics.temp.write.disabled", true)
                    .V().pageRank()
                    .with(PageRank.edges, __.inE())
                    .toList();

            assertEquals(6L, output.size());

            final Vertex v1 = output.stream().filter(v -> v.id().equals(1)).findFirst().get();
            //precision is PageRankProgram.epsilon
            assertEquals(0.380829d, v1.value(PageRankVertexProgram.PAGE_RANK), 0.00001d);

            // verify saved data
            final Map<Object, Object> savedV1 = graph.traversal().V(1).elementMap().next();
            assertEquals(0.380829d, (Double) savedV1.get(PageRankVertexProgram.PAGE_RANK), 0.00001d);
        }
    }

    @Test
    public void testPageRankWithEdgeFilterByLabel() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Vertex> output = graph.traversal()
                    .withComputer().with(QueryParameters.ALLOW_UNFILTERED_ALGORITHM, true)
                    .with("aerospike.graph.analytics.temp.write.disabled", true)
                    .V().pageRank()
                    .with(PageRank.edges, __.outE("knows"))
                    .toList();

            assertEquals(6L, output.size());

            final Vertex v1 = output.stream().filter(v -> v.id().equals(1)).findFirst().get();
            //precision is PageRankProgram.epsilon
            assertEquals(0.145985d, v1.value(PageRankVertexProgram.PAGE_RANK), 0.00001d);

            // verify saved data
            final Map<Object, Object> savedV1 = graph.traversal().V(1).elementMap().next();
            assertEquals(0.145985d, (Double) savedV1.get(PageRankVertexProgram.PAGE_RANK), 0.00001d);
        }
    }

    @Test
    public void testPageRankWithEdgeFilterBy2Labels() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            // should be same result as for all labels, because all edges are either knows or created
            final List<Vertex> output = graph.traversal()
                    .withComputer().with(QueryParameters.ALLOW_UNFILTERED_ALGORITHM, true)
                    .with("aerospike.graph.analytics.temp.write.disabled", true)
                    .V().pageRank()
                    .with(PageRank.edges, __.outE("knows", "created"))
                    .toList();

            assertEquals(6L, output.size());

            final Vertex v1 = output.stream().filter(v -> v.id().equals(1)).findFirst().get();
            //precision is PageRankProgram.epsilon
            assertEquals(0.113755d, v1.value(PageRankVertexProgram.PAGE_RANK), 0.00001d);

            // verify saved data
            final Map<Object, Object> savedV1 = graph.traversal().V(1).elementMap().next();
            assertEquals(0.113755d, (Double) savedV1.get(PageRankVertexProgram.PAGE_RANK), 0.00001d);
        }
    }

    @Test
    public void testPageRankWithEdgeFilterWithEmptyResult() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Vertex> output = graph.traversal()
                    .withComputer().with(QueryParameters.ALLOW_UNFILTERED_ALGORITHM, true)
                    .with("aerospike.graph.analytics.temp.write.disabled", true)
                    .V().pageRank()
                    .with(PageRank.edges, __.outE("foo")) // no such edge label
                    .toList();

            assertEquals(6L, output.size());

            // all vertices should have same PageRank value 1/6 = 0.166666
            output.stream().forEach(v -> assertEquals(0.166666d, v.value(PageRankVertexProgram.PAGE_RANK), 0.00001d));
        }
    }

    @Test
    public void testPageRankWithEdgeAndVertexFilter() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Vertex> output = graph.traversal()
                    .withComputer(Computer.compute().vertices(__.hasLabel("person")))
                    .with(QueryParameters.ALLOW_UNFILTERED_ALGORITHM, true)
                    .with("aerospike.graph.analytics.temp.write.disabled", true)
                    .V().pageRank()
                    .with(PageRank.edges, __.outE("knows"))
                    .toList();

            // should be 4 vertices with label "person", id 1 2 4 6
            assertEquals(4L, output.size());

            final Vertex v1 = output.stream().filter(v -> v.id().equals(1)).findFirst().get();
            assertEquals(0.206185d, v1.value(PageRankVertexProgram.PAGE_RANK), 0.00001d);
        }
    }

    @Test
    public void testPageRankWithEdgeFilterValidation() {
        final List<GraphTraversal> edgeFilters = List.of(
                __.out(), __.out().outE(), __.identity(), __.E(), __.outE().has("foo", "bar"));

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            for (final GraphTraversal edgeFilter : edgeFilters) {
                try {
                    final List<Vertex> output = graph.traversal()
                            .withComputer().with(QueryParameters.ALLOW_UNFILTERED_ALGORITHM, true)
                            .with("aerospike.graph.analytics.temp.write.disabled", true)
                            .V().pageRank()
                            .with(PageRank.edges, edgeFilter)
                            .toList();
                    fail("should throw an exception");
                } catch (final IllegalStateException e) {
                    assertEquals("The edge traversal for the PageRankProgram must have only single inE()/outE()/bothE() step.",
                            e.getMessage());
                }
            }
        }
    }

    @Test
    public void testPageRankWithTempDir() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Vertex> output = graph.traversal()
                    .withComputer().with(QueryParameters.ALLOW_UNFILTERED_ALGORITHM, true)
                    .with("aerospike.graph.analytics.temp.write.directory", System.getProperty("java.io.tmpdir"))
                    .V().pageRank()
                    .toList();

            assertEquals(6L, output.size());
            final Vertex v1 = output.stream().filter(v -> v.id().equals(1)).findFirst().get();
            //precision is PageRankProgram.epsilon
            assertEquals(0.113755d, v1.value(PageRankVertexProgram.PAGE_RANK), 0.00001d);
        }
    }

    @Test
    public void testPageRankWithPartitions() throws IOException {
        try (final OutputCapturer outputCapturer = new OutputCapturer();
             final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Vertex> output = graph.traversal()
                    .withComputer().with(QueryParameters.ALLOW_UNFILTERED_ALGORITHM, true)
                    .with("aerospike.graph.analytics.temp.write.disabled", true)
                    .with("aerospike.graph.analytics.partitions", 2)
                    .with("aerospike.graph.analytics.debug.df", "true")
                    .V().pageRank()
                    .toList();

            assertEquals(6L, output.size());
            final Vertex v1 = output.stream().filter(v -> v.id().equals(1)).findFirst().get();
            //precision is PageRankProgram.epsilon
            assertEquals(0.113755d, v1.value(PageRankVertexProgram.PAGE_RANK), 0.00001d);

            final String[] logList = outputCapturer.getLines();
            boolean repartitionedMessageFound = false;
            boolean startingMessageFound = false;
            for (String line : logList) {
                if (line.contains("Repartitioned query with 2 partitions.")) {
                    repartitionedMessageFound = true;
                } else if (line.contains("Starting with 2 partitions.")) {
                    startingMessageFound = true;
                }
            }

            assertTrue(repartitionedMessageFound);
            assertTrue(startingMessageFound);
        }
    }

    @Test
    public void testPageRankWithMissingTempDir() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            try {
                graph.traversal()
                        .withComputer().with(QueryParameters.ALLOW_UNFILTERED_ALGORITHM, true)
                        .V().pageRank()
                        .toList();
                fail("Should have thrown an exception");
            } catch (final IllegalStateException e) {
                assertTrue(e.getMessage().contains("Temporary write directory must be specified for algorithm programs."));
            }
        }
    }

    @Test
    public void testPageRankUnfiltered() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            try {
                graph.traversal()
                        .withComputer().with("aerospike.graph.analytics.temp.write.disabled", true)
                        .V().pageRank().with(PageRank.times, 3)
                        .toList();
                fail("Should have thrown an exception");
            } catch (final IllegalStateException e) {
                assertTrue(e.getMessage().contains("Attempted to run an algorithm that does not filter results after execution"));
            }
        }
    }

    @Test
    public void testPageRankWithUnsupportedFilter() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            try {
                graph.traversal()
                        .withComputer().with("aerospike.graph.analytics.temp.write.disabled", true)
                        .V().pageRank()
                        .has("some_random_property", P.lt(0.12))
                        .toList();
                fail("Should have thrown an exception");
            } catch (final IllegalStateException e) {
                assertTrue(e.getMessage().contains("Attempted to run an algorithm that does not filter results after execution"));
            }
        }
    }

    @Test
    public void testPageRankWithUnsupportedSorting() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            try {
                graph.traversal()
                        .withComputer().with("aerospike.graph.analytics.temp.write.disabled", true)
                        .V().pageRank()
                        .order().by("some_random_property")
                        .toList();
                fail("Should have thrown an exception");
            } catch (final IllegalStateException e) {
                assertTrue(e.getMessage().contains("Attempted to run an algorithm that does not filter results after execution"));
            }
        }
    }

    @Test
    public void testPageRankWithOrderWithoutLimit() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            try {
                graph.traversal()
                        .withComputer().with("aerospike.graph.analytics.temp.write.disabled", true)
                        .V().pageRank().order().by(PageRankVertexProgram.PAGE_RANK, desc)
                        .toList();
                fail("Should have thrown an exception");
            } catch (final IllegalStateException e) {
                assertTrue(e.getMessage().contains("Attempted to run an algorithm that does not filter results after execution"));
            }
        }
    }

    @Test
    public void testPageRankWithFilterById() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Map<Object, Object>> output = graph.traversal()
                    .withComputer().with("aerospike.graph.analytics.temp.write.disabled", true)
                    .with("aerospike.graph.analytics.persist", "MEMORY_ONLY")
                    .V(1, 2, 3).pageRank().with(PageRank.times, 25)
                    .elementMap()
                    .toList();

            assertEquals(3L, output.size());
            final Map v1 = output.stream().filter(v -> v.get(T.id).equals(1)).findFirst().get();
            //precision is PageRankProgram.epsilon
            assertEquals(0.113755d, (Double) v1.get(PageRankVertexProgram.PAGE_RANK), 0.00001d);
        }
    }

    @Test
    public void testPageRankWithFilterByPageRank() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Map<Object, Object>> output = graph.traversal()
                    .withComputer().with("aerospike.graph.analytics.temp.write.disabled", true)
                    .with("aerospike.graph.analytics.persist", "MEMORY_ONLY")
                    .V().pageRank().with(PageRank.times, 25)
                    .has(PageRankVertexProgram.PAGE_RANK, P.gt(0.15))
                    .elementMap()
                    .toList();

            assertEquals(2L, output.size()); //v3 and v5
            final Map v3 = output.stream().filter(v -> v.get(T.id).equals(3)).findFirst().get();
            //precision is PageRankProgram.epsilon
            assertEquals(0.30472d, (Double) v3.get(PageRankVertexProgram.PAGE_RANK), 0.00001d);
        }
    }

    @Test
    public void testPageRankWithDumpingFactor() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Map<Object, Object>> output = graph.traversal()
                    .withComputer().with(QueryParameters.ALLOW_UNFILTERED_ALGORITHM, true)
                    .with("aerospike.graph.analytics.temp.write.disabled", true).with("aerospike.graph.analytics.persist", "MEMORY_ONLY")
                    .V().pageRank(0.86)
                    .elementMap()
                    .toList();

            assertEquals(6L, output.size());
            final Map v3 = output.stream().filter(v -> v.get(T.id).equals(3)).findFirst().get();
            //precision is PageRankProgram.epsilon
            assertEquals(0.30589d, (Double) v3.get(PageRankVertexProgram.PAGE_RANK), 0.00001d);
        }
    }

    @Test
    public void testPageRankWithFilterByPageRankWithCustomProperty() {
        final String propName = "some_custom_name";

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Map<Object, Object>> output = graph.traversal()
                    .with("aerospike.graph.analytics.temp.write.disabled", true)
                    .withComputer().with("aerospike.graph.analytics.persist", "MEMORY_ONLY")
                    .V().pageRank().with(PageRank.propertyName, propName)
                    .has(propName, P.lt(0.12))
                    .elementMap()
                    .toList();

            assertEquals(2L, output.size()); //v1 and v6
            final Map v1 = output.stream().filter(v -> v.get(T.id).equals(1)).findFirst().get();
            //precision is PageRankProgram.epsilon
            assertEquals(0.113755d, (Double) v1.get(propName), 0.00001d);
        }
    }

    @Test
    public void testPageRankWithOrder() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Map<Object, Object>> output = graph.traversal()
                    .withComputer().with("aerospike.graph.analytics.temp.write.disabled", true)
                    .with("aerospike.graph.analytics.persist", "MEMORY_ONLY")
                    .V().pageRank().order().by(PageRankVertexProgram.PAGE_RANK, desc).limit(3)
                    .elementMap()
                    .toList();

            assertEquals(3L, output.size());
            assertEquals(3, output.get(0).get(T.id));
            assertEquals(5, output.get(1).get(T.id));
        }
    }

    @Test
    public void testPageRankWithoutSavingResults() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Vertex> output = graph.traversal()
                    .withComputer().with("aerospike.graph.analytics.temp.write.disabled", true)
                    .with("aerospike.graph.analytics.persist", "MEMORY_ONLY")
                    .with(QueryParameters.ALLOW_UNFILTERED_ALGORITHM, true)
                    .V().pageRank().with("gremlin.pageRankVertexProgram.saveResults", false)
                    .toList();

            // correct result returned, but not saved
            assertEquals(6L, output.size());
            final Vertex v1 = output.stream().filter(v -> v.id().equals(1)).findFirst().get();
            assertEquals(0.113755d, v1.value(PageRankVertexProgram.PAGE_RANK), 0.00001d);

            final Map<Object, Object> savedV1 = graph.traversal().V(1).elementMap().next();
            assertFalse(savedV1.containsKey(PageRankVertexProgram.PAGE_RANK));
        }
    }

    @Test
    public void testConnectedComponents() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            graph.traversal().V(6).outE().drop().iterate();
            waitForSummaryUpdate(graph);

            final List<Vertex> output = graph.traversal()
                    .withComputer().with(QueryParameters.ALLOW_UNFILTERED_ALGORITHM, true)
                    .with("aerospike.graph.analytics.temp.write.disabled", true)
                    .with("aerospike.graph.analytics.persist", "MEMORY_ONLY")
                    .V().connectedComponent()
                    .toList();

            assertEquals(6L, output.size());

            final Vertex v1 = output.stream().filter(v -> v.id().equals(1)).findFirst().get();
            final Vertex v5 = output.stream().filter(v -> v.id().equals(5)).findFirst().get();
            final Vertex v6 = output.stream().filter(v -> v.id().equals(6)).findFirst().get();
            //v1 and v5 are in same group, v6 separate because I removed edge connected v6 to other
            assertEquals(v1.id().toString(), v1.value(ConnectedComponentVertexProgram.COMPONENT));
            assertEquals(v1.id().toString(), v5.value(ConnectedComponentVertexProgram.COMPONENT));
            assertEquals(v6.id().toString(), v6.value(ConnectedComponentVertexProgram.COMPONENT));
        }
    }

    @Test
    public void testConnectedComponentsWithFilter() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            graph.traversal().V(6).outE().drop().iterate();
            waitForSummaryUpdate(graph);

            final List<Map<Object, Object>> output = graph.traversal()
                    .withComputer().with("aerospike.graph.analytics.temp.write.disabled", true)
                    .with("aerospike.graph.analytics.persist", "MEMORY_ONLY")
                    .V(1, 5, 6).connectedComponent()
                    .elementMap()
                    .toList();

            assertEquals(3L, output.size());

            final Map v1 = output.stream().filter(v -> v.get(T.id).equals(1)).findFirst().get();
            final Map v5 = output.stream().filter(v -> v.get(T.id).equals(5)).findFirst().get();
            final Map v6 = output.stream().filter(v -> v.get(T.id).equals(6)).findFirst().get();
            //v1 and v5 are in same group, v6 separate because I removed edge connected v6 to other
            assertEquals(v1.get(T.id).toString(), v1.get(ConnectedComponentVertexProgram.COMPONENT));
            assertEquals(v1.get(T.id).toString(), v5.get(ConnectedComponentVertexProgram.COMPONENT));
            assertEquals(v6.get(T.id).toString(), v6.get(ConnectedComponentVertexProgram.COMPONENT));
        }
    }

    @Test
    public void testConnectedComponentsWithFilterByComponent() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            graph.traversal().V(6).outE().drop().iterate();
            waitForSummaryUpdate(graph);

            final List<Map<Object, Object>> output = graph.traversal()
                    .withComputer().with("aerospike.graph.analytics.temp.write.disabled", true)
                    .with("aerospike.graph.analytics.persist", "MEMORY_ONLY")
                    .V().connectedComponent()
                    .has(ConnectedComponentVertexProgram.COMPONENT, 6)
                    .elementMap()
                    .toList();

            assertEquals(1L, output.size());

            final Map v6 = output.stream().filter(v -> v.get(T.id).equals(6)).findFirst().get();
            //v1 and v5 are in same group, v6 separate because I removed edge connected v6 to other
            assertEquals(v6.get(T.id).toString(), v6.get(ConnectedComponentVertexProgram.COMPONENT));
        }
    }

    @Test
    public void testPeerPressureWithFilterById() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Map<Object, Object>> output = graph.traversal().withComputer()
                    .with("aerospike.graph.analytics.temp.write.disabled", true)
                    .with("aerospike.graph.analytics.persist", "MEMORY_ONLY")
                    .V(1, 5, 6).peerPressure().elementMap().toList();

            assertEquals(3, output.size());

            final Map v1 = output.stream().filter(v -> v.get(T.id).equals(1)).findFirst().get();
            final Map v5 = output.stream().filter(v -> v.get(T.id).equals(5)).findFirst().get();
            final Map v6 = output.stream().filter(v -> v.get(T.id).equals(6)).findFirst().get();
            //v1 and v5 are in same group, v6 separate
            assertEquals(v1.get(T.id).toString(), v1.get(PeerPressureVertexProgram.CLUSTER));
            assertEquals(v1.get(T.id).toString(), v5.get(PeerPressureVertexProgram.CLUSTER));
            assertEquals(v6.get(T.id).toString(), v6.get(PeerPressureVertexProgram.CLUSTER));
        }
    }

    @Test
    public void testPeerPressureWithCustomPropertyName() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Map<Object, Object>> output = graph.traversal().withComputer()
                    .with("aerospike.graph.analytics.temp.write.disabled", true)
                    .with("aerospike.graph.analytics.persist", "MEMORY_ONLY")
                    .V(1, 5, 6).peerPressure().with(PeerPressure.propertyName, "test_name")
                    .elementMap().toList();

            assertEquals(3, output.size());

            final Map v1 = output.stream().filter(v -> v.get(T.id).equals(1)).findFirst().get();
            final Map v5 = output.stream().filter(v -> v.get(T.id).equals(5)).findFirst().get();
            final Map v6 = output.stream().filter(v -> v.get(T.id).equals(6)).findFirst().get();
            //v1 and v5 are in same group, v6 separate
            assertEquals(v1.get(T.id).toString(), v1.get("test_name"));
            assertEquals(v1.get(T.id).toString(), v5.get("test_name"));
            assertEquals(v6.get(T.id).toString(), v6.get("test_name"));
        }
    }

    @Test
    public void testPeerPressureWithFilterByCluster() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Map<Object, Object>> output = graph.traversal().withComputer()
                    .with("aerospike.graph.analytics.temp.write.disabled", true)
                    .with("aerospike.graph.analytics.persist", "MEMORY_ONLY")
                    .V().peerPressure()
                    .with(PeerPressure.times, 5)
                    .with(PeerPressure.propertyName, "pp")
                    .has("pp", 1)
                    .elementMap().toList();

            assertEquals(5, output.size());

            final Map v1 = output.stream().filter(v -> v.get(T.id).equals(1)).findFirst().get();
            final Map v5 = output.stream().filter(v -> v.get(T.id).equals(5)).findFirst().get();
            //v1 and v5 are in same group, v6 separate
            assertEquals("1", v1.get("pp"));
            assertEquals("1", v5.get("pp"));
        }
    }

    private void waitForSummaryUpdate(final FireflyGraph graph) {
        while (graph.fireflySummaryUpdater.getFireflyStatistics().totalVertexCount() == 0) {
            graph.fireflySummaryUpdater.forceWrite();
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Ignore
    @Test
    public void playTest() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final var output = graph.traversal()
                    .withComputer(Computer.compute().vertices(__.hasLabel("person")).edges(__.outE("knows")))
                    // .with("aerospike.graph.analytics.debug.df", "true")
                    .V().out().elementMap().toList();

            output.forEach(System.out::println);
        }
    }
}
