package com.aerospike.firefly.util;

import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
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
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final AerospikeConnection a = fireflyGraph.getBaseGraph();
            assertEquals(ConfigurationHelper.getPrefix(config) + ConfigurationHelper.Keys.Sets.VERTEX_AERO_SET.getValue().numeric, a.getConfig().vertexAeroSet);
            assertEquals(ConfigurationHelper.getPrefix(config) + ConfigurationHelper.Keys.Sets.EDGE_AERO_SET.getValue().numeric, a.getConfig().edgeAeroSet);
            assertEquals(ConfigurationHelper.getPrefix(config) + ConfigurationHelper.Keys.Sets.IN_VP_SET.getValue().numeric, a.getConfig().inVpSet);
            assertEquals(ConfigurationHelper.getPrefix(config) + ConfigurationHelper.Keys.Sets.OUT_VP_SET.getValue().numeric, a.getConfig().outVpSet);
        }
    }

    @Test
    public void testNamesUnique() {
        final Set<String> uniqueNames = new HashSet<>();
        ConfigurationHelper.Keys.Bins.keys().forEach(it -> {
            if (!uniqueNames.add(ConfigurationHelper.Keys.Bins.valueOf(it).getValue().english)) {
                Assert.fail("Duplicate bin name: " + ConfigurationHelper.Keys.Bins.valueOf(it).getValue().english);
            }
        });
        ConfigurationHelper.Keys.InternalConfigs.keys().forEach(it -> {
            if (!uniqueNames.add(ConfigurationHelper.Keys.InternalConfigs.valueOf(it).getValue().english)) {
                Assert.fail("Duplicate internal config name: " + ConfigurationHelper.Keys.InternalConfigs.valueOf(it).getValue().english);
            }
        });
    }

    @Test
    public void testDebuggingSetNames() {
        final Configuration config = ConfigurationHelper.loadFromResources("integration-test-settings.properties");
        config.setProperty(ConfigurationHelper.Keys.DEBUG_MODE_FLAG, "true");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final AerospikeConnection a = fireflyGraph.getBaseGraph();
            assertEquals(ConfigurationHelper.getPrefix(config) + ConfigurationHelper.Keys.Sets.VERTEX_AERO_SET.getValue().english, a.getConfig().vertexAeroSet);
            assertEquals(ConfigurationHelper.getPrefix(config) + ConfigurationHelper.Keys.Sets.EDGE_AERO_SET.getValue().english, a.getConfig().edgeAeroSet);
            assertEquals(ConfigurationHelper.getPrefix(config) + ConfigurationHelper.Keys.Sets.IN_VP_SET.getValue().english, a.getConfig().inVpSet);
            assertEquals(ConfigurationHelper.getPrefix(config) + ConfigurationHelper.Keys.Sets.OUT_VP_SET.getValue().english, a.getConfig().outVpSet);
        }
    }

    @Test
    public void testReadWriteUnderDebug() {
        final Configuration config = ConfigurationHelper.loadFromResources("integration-test-settings.properties");
        config.setProperty(ConfigurationHelper.Keys.DEBUG_MODE_FLAG, "true");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final AerospikeConnection a = fireflyGraph.getBaseGraph();
            a.dropDatabase(fireflyGraph, true);
            final GraphTraversalSource g = fireflyGraph.traversal();


            Vertex lemon = g.addV("lemon").next();
            g.V(lemon).property("color", "yellow").next();
            g.V(lemon).property("type", "plant").next();
            Vertex lime = g.addV("lime").property("color", "green").property("type", "plant").next();
            Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
            g.V(lime).addE("IsA").to(fruit).property("genus", "citrus").next();
            g.V()
                    .has("type", "taxonomy").as("a")
                    .V().has("type", "plant").as("b")
                    .addE("IsA").from("b").to("a").property("a", "b").iterate();
            Vertex s1 = g.V().has("type", "taxonomy").next();
            List<Vertex> s2 = g.V().has("type", "plant").next(2);
            List<Edge> things = g.E().has("a", "b").toList();
            Assert.assertEquals(3, (long) g.V(fruit.id()).inE().count().next());
            a.dropDatabase(fireflyGraph, true);
        }
    }

    @Test
    public void testCaseInsensitivity() {
        final Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        conf.setProperty("aeroSpike.grAph.strAtegy.meRge.edGe.poLl.intErval", "7531");
        conf.setProperty("AEROSPIKE.client.POLICY.write.TOTALTIMEOUT", "1357");
        try (final FireflyGraph graph = FireflyGraph.open(conf)) {
            Assert.assertEquals(7531, graph.getBaseGraph().getConfig().mergeEdgePollInterval);
            WritePolicy policy = new WritePolicy();
            graph.getBaseGraph().configureWritePolicy(policy);
            Assert.assertEquals(1357, policy.totalTimeout);
        }
    }
}
