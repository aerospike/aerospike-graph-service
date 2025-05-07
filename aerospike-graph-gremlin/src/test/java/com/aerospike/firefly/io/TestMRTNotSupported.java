package com.aerospike.firefly.io;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.OutputCapturer;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.junit.Test;

import java.io.IOException;

import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.MRT_ENABLED_FLAG;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// to run this test need AeroSpike database below 8 OR without SC
public class TestMRTNotSupported extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return false;
    }

    @Test
    public void testTransactionSupport() throws IOException {
        FireflyGraph.EXIT_MANAGER = new ExitManagerTest();

        Configuration txnConfig = ConfigurationUtils.cloneConfiguration(config);

        // should not fail
        try (final FireflyGraph graph = FireflyGraph.open(txnConfig)) {
            graph.traversal().V().count().iterate();
        }
        assertFalse(exited);

        try (final OutputCapturer outputCapturer = new OutputCapturer()) {
            txnConfig.setProperty(MRT_ENABLED_FLAG, "true");
            FireflyGraph.open(txnConfig);

            assertTrue(exited);

            final String[] logList = outputCapturer.getLines();
            boolean errorMessageFound = false;
            for (String line : logList) {
                if (line.contains("Minimum version 8 required for MRT. Please verify that all nodes in the cluster are running a compatible version of Aerospike.")) {
                    errorMessageFound = true;
                    break;
                }
            }
            assertTrue(errorMessageFound);
        }
    }

}
