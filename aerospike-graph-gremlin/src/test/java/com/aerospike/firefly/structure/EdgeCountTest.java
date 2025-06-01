package com.aerospike.firefly.structure;

import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class EdgeCountTest extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testEdgeCount() {
        final GraphTraversalSource g = graph.traversal();

        // Create 3 vertices
        Vertex v1 = g.addV("person").property("name", "v1").next();
        Vertex v2 = g.addV("movie").property("name", "v2").next();
        Vertex v3 = g.addV("movie").property("name", "v3").next();

        // Add many edges from v1 to v2, to make sure they are considered a supernode,
        // Has to be a relatively high number to replicate GRAPH-1555 issue.
        for (int i = 0; i < 10000; i++) {
            v1.addEdge("knows", v2, "edgeId", i);
        }

        // Add a single edge from v3 to v2
        v3.addEdge("knows", v2, "edgeId", "solo");

        List<Map<String, Object>> result = g.V()
                .project("vertex", "edgeCount")
                .by(__.identity())
                .by(__.bothE().count()).toList();

        for (int i = 0; i < 3; i++) {
            if (result.get(i).get("vertex").equals(v1)) {
                Assert.assertEquals(10000L, result.get(i).get("edgeCount"));
            } else if (result.get(i).get("vertex").equals(v2)) {
                Assert.assertEquals(10001L, result.get(i).get("edgeCount"));
            } else if (result.get(i).get("vertex").equals(v3)) {
                Assert.assertEquals(1L, result.get(i).get("edgeCount"));
            }
        }
    }
}
