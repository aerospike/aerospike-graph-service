package com.aerospike.firefly.structure;

import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TestEdgeCount extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testEdgeCount() {
        // Ensure that the number of connections is low enough to avoid being considered a supernode.
        edgeCountTest(30);
    }

    @Test
    public void testSupernodeEdgeCount() {
        // Ensure that the number of connections is high enough to be considered a supernode.
        final long numOfConnections = Math.max(10000, graph.getBaseGraph().ON_RECORD_ID_LIMIT + 100);
        edgeCountTest(numOfConnections);
    }

    private void edgeCountTest(long numOfConnections) {
        final GraphTraversalSource g = graph.traversal();

        // Create 3 vertices.
        final Vertex v1 = g.addV("person").property("name", "v1").next();
        final Vertex v2 = g.addV("movie").property("name", "v2").next();
        final Vertex v3 = g.addV("movie").property("name", "v3").next();

        // Add many edges from v1 to v2, to make sure they are considered a supernode.
        for (int i = 0; i < numOfConnections; i++) {
            v1.addEdge("knows", v2, "edgeId", i);
        }

        // Add a single edge from v3 to v2.
        v3.addEdge("knows", v2, "edgeId", "solo");

        final List<Map<String, Object>> resultIn = g.V()
                .project("vertex", "edgeCount")
                .by(__.identity())
                .by(__.inE().count()).toList();

        final List<Map<String, Object>> resultOut = g.V()
                .project("vertex", "edgeCount")
                .by(__.identity())
                .by(__.outE().count()).toList();

        final List<Map<String, Object>> resultBoth = g.V()
                .project("vertex", "edgeCount")
                .by(__.identity())
                .by(__.bothE().count()).toList();

        final Map<Object, Long> expectedIn = Map.of(
                v1, 0L,
                v2, numOfConnections + 1L,
                v3, 0L
        );

        final Map<Object, Long> expectedOut = Map.of(
                v1, numOfConnections,
                v2, 0L,
                v3, 1L
        );

        final Map<Object, Long> expectedBoth = Map.of(
                v1, numOfConnections,
                v2, numOfConnections + 1L,
                v3, 1L
        );

        for (int i = 0; i < resultIn.size(); i++) {
            Map<String, Object> inEntry = resultIn.get(i);
            Object vertex = inEntry.get("vertex");
            Assert.assertEquals(expectedIn.get(vertex), inEntry.get("edgeCount"));

            Map<String, Object> outEntry = resultOut.get(i);
            vertex = outEntry.get("vertex");
            Assert.assertEquals(expectedOut.get(vertex), outEntry.get("edgeCount"));

            Map<String, Object> bothEntry = resultBoth.get(i);
            vertex = bothEntry.get("vertex");
            Assert.assertEquals(expectedBoth.get(vertex), bothEntry.get("edgeCount"));
        }
    }
}
