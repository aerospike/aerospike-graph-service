package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.AUTH_MODE;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.CLIENT_SERVICES_ALTERNATE;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.CLUSTER_NAME;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.VALIDATE_CLUSTER_NAME;

public class TestAerospikeClientProviderConfiguration {

    @Test
    public void testSettingSingletonConfigs() {
//        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
//        config.setProperty(AUTH_MODE, "external_insecure");
//        config.setProperty(CLIENT_SERVICES_ALTERNATE, "true");
//        config.setProperty(CLUSTER_NAME, "test");
//        config.setProperty(VALIDATE_CLUSTER_NAME, "false");
//        try (final FireflyGraph firefly = FireflyGraph.open(config)) {
//            final AerospikeClient client = firefly.getBaseGraph().getClient();
//            Assert.assertEquals(AuthMode.EXTERNAL_INSECURE, client.getCluster().authMode);
//            Assert.assertEquals("test", client.getCluster().getClusterName());
//        }
    }
}
