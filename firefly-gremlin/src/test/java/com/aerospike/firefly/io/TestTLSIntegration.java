package com.aerospike.firefly.io;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Ignore;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.AEROSPIKE_HOST;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.AEROSPIKE_PORT;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.TLS;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestTLSIntegration {
    @Test
    public void testTLSConnection() {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(TLS.toLowerCase(), "true");
        config.setProperty(AEROSPIKE_HOST.toLowerCase(), "aerospike.test.aerospike.dev");
        config.setProperty(AEROSPIKE_PORT.toLowerCase(), 4303);
        AerospikeConnection db = AerospikeConnection.connect(config);
        FireflyGraph graph = FireflyGraph.open(config);
        Vertex v = graph.addVertex();
    }
}
