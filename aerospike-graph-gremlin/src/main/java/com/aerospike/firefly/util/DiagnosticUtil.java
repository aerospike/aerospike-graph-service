package com.aerospike.firefly.util;

import com.aerospike.client.AerospikeClient;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

public class DiagnosticUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DiagnosticUtil.class);

    public static AerospikeClient enableWriteFails(final AerospikeClient aerospikeClient, final Configuration conf) {
        try {
            final double clientFailureRate = Double.valueOf(ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.CLIENT_FAILURE_RATE, conf));
            final Class clientFailureClass = Class.forName("com.aerospike.firefly.bulkloader.integration.util.FailingAerospikeClient");
            final Method method = clientFailureClass.getMethod("clientWithWriteFails", AerospikeClient.class, double.class);
            return (AerospikeClient) method.invoke(null, aerospikeClient, clientFailureRate);
        } catch (final Exception e) {
            LOG.error("Error instantiating failure client for testing", e);
            throw new RuntimeException(e);
        }

    }
}
