package com.aerospike.firefly.io;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.AbstractFireflySuite.exited;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.HTTP_ENABLED;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.MRT_ENABLED_FLAG;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.PHAT_EDGE_SIZE;
import static com.aerospike.firefly.util.config.ConfigurationHelper.loadFromFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

// to run this test need AeroSpike database 8+ with strong consistency
public class TestMRTSupported {

    @Test
    public void testTransactionSupport() {
        FireflyGraph.EXIT_MANAGER = new AbstractFireflySuite.ExitManagerTest();

        final Configuration config = loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(HTTP_ENABLED.toLowerCase(), "false");

        // should not fail
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().count().iterate();
            // should be default supernode limit for test config
            assertEquals(6553, graph.getBaseGraph().ON_RECORD_ID_LIMIT);
        }
        assertFalse(exited);

        config.setProperty(MRT_ENABLED_FLAG, "true");
        // should not fail
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().count().iterate();
            // supernode limit should be more strict
            assertEquals(1023, graph.getBaseGraph().ON_RECORD_ID_LIMIT);
        }
        assertFalse(exited);
    }

    @Test
    public void testMRTDropSupernode() {
        final Configuration config = loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(HTTP_ENABLED.toLowerCase(), "false");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.getBaseGraph().dropDatabase(graph, true);
        }

        FireflyGraph graph = null;
        try {
            config.setProperty(MRT_ENABLED_FLAG.toLowerCase(), true);
            config.setProperty(PHAT_EDGE_SIZE.toLowerCase(), 1);
            graph = FireflyGraph.open(config);
            final GraphTraversalSource g = graph.traversal();
            final Map<Integer, Vertex> vertices = new HashMap<>();
            for (int i = 1; i <= 7000; i++) {
                vertices.put(i, g.addV("vertex").property(T.id, i).next());
            }
            for (int i = 2; i<= 7000; i++) {
                g.addE("edge").from(vertices.get(1)).to(vertices.get(i)).iterate();
                Assert.assertTrue(g.V(i).inE().hasNext());
            }
            try {
                g.V(1).drop().iterate();
            } catch (final Exception e) {
                Assert.fail("Failed to drop a supernode Vertex in a MRT: " + e.getMessage());
            }
            Assert.assertFalse(g.V(1).outE().hasNext());
            Assert.assertFalse(g.E().hasNext());
            for (int i = 2; i <= 7000; i++) {
                Assert.assertTrue(g.V(i).hasNext());
                Assert.assertFalse(g.V(i).inE().hasNext());
            }
        } finally {
            if (graph != null) {
                graph.getBaseGraph().dropDatabase(graph, true);
                graph.close();
            }
        }
    }
}
