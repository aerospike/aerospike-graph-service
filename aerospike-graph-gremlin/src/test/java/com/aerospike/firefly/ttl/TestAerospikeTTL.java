package com.aerospike.firefly.ttl;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestAerospikeTTL {
    static boolean exited = false;

    class ExitManagerTest extends FireflyGraph.ExitManager {
        @Override
        public void exit(final int code) {
            exited = true;
        }
    }

    @Test
    public void test() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        FireflyGraph.EXIT_MANAGER = new ExitManagerTest();
        final FireflyGraph graph = FireflyGraph.open(config);
        Assert.assertNull(graph);
        FireflyGraph.EXIT_MANAGER = new FireflyGraph.ExitManager();
        Assert.assertTrue(exited);
    }
}
