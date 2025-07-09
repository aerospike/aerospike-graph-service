package com.aerospike.firefly.olap;

import com.aerospike.firefly.olap.config.QueryParameters;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.computer.clustering.connected.ConnectedComponentVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRank;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PeerPressure;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
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
                    .V().pageRank()
                    .toList();

            assertEquals(6L, output.size());
            final Vertex v1 = output.stream().filter(v -> v.id().equals(1)).findFirst().get();
            //precision is PageRankProgram.epsilon
            assertEquals(0.113755d, v1.value(PageRankVertexProgram.PAGE_RANK), 0.00001d);
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
                        .withComputer()
                        .V().pageRank().with(PageRank.times, 3)
                        .toList();
                fail("Should have thrown an exception");
            } catch (final IllegalStateException e) {
                assertTrue(e.getMessage().contains("Attempting to run an algorithm that does does filter the results down after execution"));
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
                        .withComputer()
                        .V().pageRank()
                        .has("some_random_property", P.lt(0.12))
                        .toList();
                fail("Should have thrown an exception");
            } catch (final IllegalStateException e) {
                assertTrue(e.getMessage().contains("Attempting to run an algorithm that does does filter the results down after execution"));
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
                        .withComputer()
                        .V().pageRank()
                        .order().by("some_random_property")
                        .toList();
                fail("Should have thrown an exception");
            } catch (final IllegalStateException e) {
                assertTrue(e.getMessage().contains("Attempting to run an algorithm that does does filter the results down after execution"));
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
                        .withComputer()
                        .V().pageRank().order().by(PageRankVertexProgram.PAGE_RANK, Order.desc)
                        .toList();
                fail("Should have thrown an exception");
            } catch (final IllegalStateException e) {
                assertTrue(e.getMessage().contains("Attempting to run an algorithm that does does filter the results down after execution"));
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
                    .withComputer()
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
                    .withComputer()
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
                    .withComputer()
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
                    .withComputer()
                    //.with("aerospike.graph.olap.temp.write.directory", "c:\\tmp\\")
                    .V().pageRank().order().by(PageRankVertexProgram.PAGE_RANK, Order.desc).limit(3)
                    .elementMap()
                    .toList();

            assertEquals(3L, output.size());
            assertEquals(3, output.get(0).get(T.id));
            assertEquals(5, output.get(1).get(T.id));
        }
    }

    @Test
    public void testPageRankWithSavingResults() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            final List<Vertex> output = graph.traversal()
                    .withComputer()
                    .with(QueryParameters.ALLOW_UNFILTERED_ALGORITHM, true)
                    //.with("aerospike.graph.olap.temp.write.directory", "c:\\tmp\\")
                    .V().pageRank().with("gremlin.pageRankVertexProgram.saveResults", true)
                    .toList();

            assertEquals(6L, output.size());

            final Map<Object, Object> v1 = graph.traversal().V(1).elementMap().next();
            assertEquals(0.113755d, (Double) v1.get(PageRankVertexProgram.PAGE_RANK), 0.00001d);
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
                    .withComputer()
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
                    .withComputer()
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

            final Object output = graph.traversal().withComputer()
                    .with("aerospike.graph.olap.debug.df", "true")
                    .V().out().order().by("name").limit(2).explain();

            System.out.println(output);
        }
    }
}
