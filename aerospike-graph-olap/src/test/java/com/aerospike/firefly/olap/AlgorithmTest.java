package com.aerospike.firefly.olap;

import com.aerospike.firefly.olap.process.ConnectedComponentProgram;
import com.aerospike.firefly.olap.process.PageRankProgram;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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

            List<Vertex> output = graph.traversal()
                    .withComputer()
                    .V().pageRank()
                    .toList();

            Assert.assertEquals(6L, output.size());
            final Vertex v1 = output .stream().filter(v -> v.id().equals(1)).findFirst().get();
            //precision is PageRankProgram.epsilon
            assertEquals(0.113755d, (Double) v1.value(PageRankProgram.property), 0.00001d);
        }
    }

    @Test
    public void testPageRankWithFilter() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            waitForSummaryUpdate(graph);

            List<Map<Object, Object>> output = graph.traversal()
                    .withComputer()
                    .V(1,2,3).pageRank()
                    .elementMap()
                    .toList();

            Assert.assertEquals(3L, output.size());
            final Map v1 = output.stream().filter(v -> v.get(T.id).equals(1)).findFirst().get();
            //precision is PageRankProgram.epsilon
            assertEquals(0.113755d, (Double) v1.get(PageRankProgram.property), 0.00001d);
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

            List<Vertex> output = graph.traversal()
                    .withComputer()
                    .V().connectedComponent()
                    .toList();

            Assert.assertEquals(6L, output.size());

            final Vertex v1 = output.stream().filter(v -> v.id().equals(1)).findFirst().get();
            final Vertex v5 = output.stream().filter(v -> v.id().equals(1)).findFirst().get();
            final Vertex v6 = output.stream().filter(v -> v.id().equals(1)).findFirst().get();
            //v1 and v5 are in same group, v6 separate because I removed edge connected v6 to other
            assertEquals(v1.id().toString(), v1.value(ConnectedComponentProgram.property));
            assertEquals(v1.id().toString(), v5.value(ConnectedComponentProgram.property));
            assertEquals(v6.id().toString(), v6.value(ConnectedComponentProgram.property));
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

            List<Map<Object, Object>> output = graph.traversal()
                    .withComputer()
                    .V(1,5,6).connectedComponent()
                    .elementMap()
                    .toList();

            Assert.assertEquals(3L, output.size());

            final Map v1 = output.stream().filter(v -> v.get(T.id).equals(1)).findFirst().get();
            final Map v5 = output.stream().filter(v -> v.get(T.id).equals(1)).findFirst().get();
            final Map v6 = output.stream().filter(v -> v.get(T.id).equals(1)).findFirst().get();
            //v1 and v5 are in same group, v6 separate because I removed edge connected v6 to other
            assertEquals(v1.get(T.id).toString(), v1.get(ConnectedComponentProgram.property));
            assertEquals(v1.get(T.id).toString(), v5.get(ConnectedComponentProgram.property));
            assertEquals(v6.get(T.id).toString(), v6.get(ConnectedComponentProgram.property));
        }
    }

    private void waitForSummaryUpdate(final FireflyGraph graph) {
        while(graph.fireflySummaryUpdater.getFireflyStatistics().totalVertexCount() == 0) {
            graph.fireflySummaryUpdater.forceWrite();
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
