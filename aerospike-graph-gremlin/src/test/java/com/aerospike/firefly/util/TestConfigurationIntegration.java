package com.aerospike.firefly.util;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static junit.framework.TestCase.assertEquals;

/*
  Created by Grant Haywood grant@iowntheinter.net
  7/27/23
*/
public class TestConfigurationIntegration {

    @Test
    public void testDefaultSetNames() {
        final Configuration config = ConfigurationHelper.loadFromResources("integration-test-settings.properties");
        config.setProperty(ConfigurationHelper.Keys.DEBUG_MODE_FLAG, "false");
        final FireflyGraph fireflyGraph = FireflyGraph.open(config);
        final AerospikeConnection a = fireflyGraph.getBaseGraph();
        assertEquals(ConfigurationHelper.getPrefix(config) + ConfigurationHelper.Keys.Sets.VERTEX_AERO_SET.getValue().numeric, a.VERTEX_AERO_SET);
        assertEquals(ConfigurationHelper.getPrefix(config) + ConfigurationHelper.Keys.Sets.EDGE_AERO_SET.getValue().numeric, a.EDGE_AERO_SET);
        assertEquals(ConfigurationHelper.getPrefix(config) + ConfigurationHelper.Keys.Sets.IN_VP_SET.getValue().numeric, a.IN_VP_SET);
        assertEquals(ConfigurationHelper.getPrefix(config) + ConfigurationHelper.Keys.Sets.OUT_VP_SET.getValue().numeric, a.OUT_VP_SET);

    }

    @Test
    public void testDebuggingSetNames() {
        final Configuration config = ConfigurationHelper.loadFromResources("integration-test-settings.properties");
        config.setProperty(ConfigurationHelper.Keys.DEBUG_MODE_FLAG, "true");
        final FireflyGraph fireflyGraph = FireflyGraph.open(config);
        final AerospikeConnection a = fireflyGraph.getBaseGraph();
        assertEquals(ConfigurationHelper.getPrefix(config) + ConfigurationHelper.Keys.Sets.VERTEX_AERO_SET.getValue().english, a.VERTEX_AERO_SET);
        assertEquals(ConfigurationHelper.getPrefix(config) + ConfigurationHelper.Keys.Sets.EDGE_AERO_SET.getValue().english, a.EDGE_AERO_SET);
        assertEquals(ConfigurationHelper.getPrefix(config) + ConfigurationHelper.Keys.Sets.IN_VP_SET.getValue().english, a.IN_VP_SET);
        assertEquals(ConfigurationHelper.getPrefix(config) + ConfigurationHelper.Keys.Sets.OUT_VP_SET.getValue().english, a.OUT_VP_SET);
    }

    @Test
    public void testReadWriteUnderDebug() {
        final Configuration config = ConfigurationHelper.loadFromResources("integration-test-settings.properties");
        config.setProperty(ConfigurationHelper.Keys.DEBUG_MODE_FLAG, "true");
        final FireflyGraph fireflyGraph = FireflyGraph.open(config);
        final AerospikeConnection a = fireflyGraph.getBaseGraph();
        a.dropDatabase(fireflyGraph,true);
        final GraphTraversalSource g = fireflyGraph.traversal();
        Vertex lemon = g.addV("lemon").property("color", "yellow").property("type", "plant").next();
        Vertex lime = g.addV("lime").property("color", "green").property("type", "plant").next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("a", "b").iterate();
        Vertex s1 = g.V().has("type", "taxonomy").next();
        List<Vertex> s2 = g.V().has("type", "plant").next(2);
        List<Edge> things = g.E().has("a", "b").toList();
        Assert.assertEquals(2, (long) g.V(fruit.id()).inE().count().next());
        a.dropDatabase(fireflyGraph,true);
    }
}
