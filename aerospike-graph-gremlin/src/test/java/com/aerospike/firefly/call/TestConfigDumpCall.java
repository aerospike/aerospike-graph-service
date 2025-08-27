package com.aerospike.firefly.call;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;
import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import java.util.Map;

public class TestConfigDumpCall extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testExecution() throws InterruptedException {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.EDGE_ID_BUFFER_SIZE, "30000");
        config.setProperty("aerospike.graph.http.port", "4000");

        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            Map<String, Object> result = (Map<String, Object>) g.call("aerospike.graph.admin.config.dump-config").next();
            Assert.assertEquals("30000", result.get(ConfigurationHelper.Keys.EDGE_ID_BUFFER_SIZE));
            Assert.assertEquals("4000", result.get("aerospike.graph.http.port"));
            Assert.assertEquals("test", result.get("aerospike.client.namespace"));

            for (final String key : result.keySet()) {
                Object value = result.get(key);
                if (key.contains("password") || key.contains("secret") || key.contains("token") || key.contains("passkey")) {
                    Assert.assertEquals("*******", value);
                }else{
                    Assert.assertEquals(ConfigurationHelper.getOrDefault(key, config), value);
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
                () -> g.call("aerospike.graph.admin.config.dump-config")
                        .with("Bombo").with("Rass", "Clart").next());
    }
}

