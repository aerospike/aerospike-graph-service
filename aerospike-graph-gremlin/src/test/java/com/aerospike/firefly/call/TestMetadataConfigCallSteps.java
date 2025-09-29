package com.aerospike.firefly.call;

import com.aerospike.firefly.process.call.metadata.MetadataServiceConfig;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;


public class TestMetadataConfigCallSteps extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testExecutionFull() throws InterruptedException {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.EDGE_ID_BUFFER_SIZE, "30000");
        config.setProperty("aerospike.graph.http.port", "4000");

        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            Map<String, Map<String, Object>> result = (Map<String, Map<String, Object>>) g.call("aerospike.graph.admin.metadata.config").with("mode", "full").next();
            Assert.assertNotEquals(6, result.get("Graph Properties").size());
            Assert.assertEquals("30000", result.get("Graph Properties").get(ConfigurationHelper.Keys.EDGE_ID_BUFFER_SIZE));
            Assert.assertNotNull(result.get("Gremlin Server Configuration"));
            Assert.assertEquals("4000", result.get("Graph Properties").get("aerospike.graph.http.port"));
            Assert.assertEquals("test", result.get("Graph Properties").get("aerospike.client.namespace"));

            //Internal untouched configs
            Assert.assertEquals(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.AUTH_MODE, config), result.get("Graph Properties").get(ConfigurationHelper.Keys.AUTH_MODE));
            Assert.assertEquals(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.QUERY_IMPL, config), result.get("Graph Properties").get(ConfigurationHelper.Keys.QUERY_IMPL));


            for (final Map.Entry<String, Object> entry : result.get("Graph Properties").entrySet()) {
                String keystr = entry.getKey();
                Object value = entry.getValue();
                if (MetadataServiceConfig.isSensitive(keystr)) {
                    Assert.assertEquals("*******", value);
                } else {
                    Assert.assertEquals(ConfigurationHelper.getOrDefault(keystr, config).toString(), value);
                }
            }
        }
    }

    @Test
    public void testExecutionNoArgs() throws InterruptedException {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.EDGE_ID_BUFFER_SIZE, "30000");
        config.setProperty("aerospike.graph.http.port", "4000");

        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            Map<String, Map<String, Object>> result = (Map<String, Map<String, Object>>)
                    g.call("aerospike.graph.admin.metadata.config").with("mode", "delta").next();
            Assert.assertEquals(6, result.get("Graph Properties").size());
            Assert.assertEquals("30000", result.get("Graph Properties")
                    .get(ConfigurationHelper.Keys.EDGE_ID_BUFFER_SIZE));
            Assert.assertNotNull(result.get("Gremlin Server Configuration"));
            Assert.assertEquals("4000", result.get("Graph Properties").get("aerospike.graph.http.port"));
            Assert.assertEquals("test", result.get("Graph Properties").get("aerospike.client.namespace"));

            for (final Map.Entry<String, Object> entry : result.get("Graph Properties").entrySet()) {
                String keystr = entry.getKey();
                Object value = entry.getValue();
                if (MetadataServiceConfig.isSensitive(keystr)) {
                    Assert.assertEquals("*******", value);
                } else {
                    Assert.assertEquals(ConfigurationHelper.getOrDefault(keystr, config).toString(), value);
                }
            }
        }
    }

    @Test
    public void testExecutionBase() throws InterruptedException {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.EDGE_ID_BUFFER_SIZE, "30000");
        config.setProperty("aerospike.graph.http.port", "4000");

        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            Map<String, Map<String, Object>> result = (Map<String, Map<String, Object>>)
                    g.call("aerospike.graph.admin.metadata.config").with("mode", "delta").next();
            Assert.assertEquals(6, result.get("Graph Properties").size());
            Assert.assertEquals("30000", result.get("Graph Properties")
                    .get(ConfigurationHelper.Keys.EDGE_ID_BUFFER_SIZE));
            Assert.assertNotNull(result.get("Gremlin Server Configuration"));
            Assert.assertEquals("4000", result.get("Graph Properties").get("aerospike.graph.http.port"));
            Assert.assertEquals("test", result.get("Graph Properties").get("aerospike.client.namespace"));

            for (final Map.Entry<String, Object> entry : result.get("Graph Properties").entrySet()) {
                String keystr = entry.getKey();
                Object value = entry.getValue();
                if (MetadataServiceConfig.isSensitive(keystr)) {
                    Assert.assertEquals("*******", value);
                } else {
                    Assert.assertEquals(ConfigurationHelper.getOrDefault(keystr, config).toString(), value);
                }
            }
        }
    }

    @Test
    public void testWrongConfig() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        g.V().drop().iterate();
        Thread.sleep(100);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> g.call("aerospike.graph.admin.metadata.config")
                        .with("Bombo").next());

        Assert.assertThrows(IllegalArgumentException.class,
                () -> g.call("aerospike.graph.admin.metadata.config")
                        .with("Rass", "Clart").next());

        Assert.assertThrows(IllegalArgumentException.class,
                () -> g.call("aerospike.graph.admin.metadata.config")
                        .with("Jah", "full").next());

        Assert.assertThrows(IllegalArgumentException.class,
                () -> g.call("aerospike.graph.admin.metadata.config")
                        .with("mode", "monkeh").next());
    }
}

