package com.aerospike.firefly.io;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.cluster.Cluster;
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
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestTLSIntegration {
    @Test
    @Ignore
    public void testTLSConnection() {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(TLS.toLowerCase(), "true");
        config.setProperty(AEROSPIKE_HOST.toLowerCase(), "aerospike.test.aerospike.dev");
        config.setProperty(AEROSPIKE_PORT.toLowerCase(), 4303);
        AerospikeConnection db = AerospikeConnection.connect(config);
        FireflyGraph graph = FireflyGraph.open(config);
        Vertex v = graph.addVertex();
    }

    @Test
    public void testTLSName() {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(TLS.toLowerCase(), "true");
        config.setProperty(AEROSPIKE_HOST.toLowerCase(), "172.17.0.1");
        config.setProperty(ConfigurationHelper.Keys.TLS_NAMES, "172.17.0.1:aerospike.test.aerospike.dev");
        config.setProperty(AEROSPIKE_PORT.toLowerCase(), 4303);
        AerospikeConnection db = AerospikeConnection.connect(config);
        final Cluster c = db.getClient().getCluster();
        FireflyGraph graph = FireflyGraph.open(config);
        Vertex v = graph.addVertex();
    }

    @Test
    public void testTLSNameNegative() {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(TLS.toLowerCase(), "true");
        config.setProperty(AEROSPIKE_HOST.toLowerCase(), "172.17.0.1");
        config.setProperty(ConfigurationHelper.Keys.TLS_NAMES, "172.17.0.1:aerospike-ker.test.aerospike.dev");
        config.setProperty(AEROSPIKE_PORT.toLowerCase(), 4303);
        boolean success = false;
        try{
            AerospikeConnection db = AerospikeConnection.connect(config);
        }catch (AerospikeException aerospikeException){
            if(aerospikeException.getMessage().contains("Invalid TLS"))
                success = true;
        }
        assertTrue(success);
    }
}
