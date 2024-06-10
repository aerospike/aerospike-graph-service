package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyAerospikeGraphServiceCheck {
    final private static Logger LOGGER = LoggerFactory.getLogger(FireflyAerospikeGraphServiceCheck.class);

    private FireflyAerospikeGraphServiceCheck() {
    }

    public static void checkFeatureKey(final AerospikeClient client) {
        for (final Node node: client.getNodes()) {
            LOGGER.debug("Info.request: feature-key");
            final String infoResponse = Info.request(new InfoPolicy(), node, "feature-key");
            validateInfoResponse(infoResponse);
        }
    }

    public static void validateInfoResponse(final String infoResponse) {
        if (infoResponse == null || "".equals(infoResponse)) {
            throw new RuntimeException("Failed to initialize graph-service due to unsupported Server version. " +
                    "Please ensure you're running Aerospike Server Enterprise Edition on all Aerospike nodes in the cluster.");
        }
        final String[] features = infoResponse.split(";");
        for (final String feature : features) {
            if (feature.startsWith("graph-service")) {
                final String[] graphService = feature.split("=");
                if (graphService.length == 2) {
                    if ("true".equals(graphService[1])) {
                        return;
                    }
                }
            }
        }
        throw new RuntimeException("Failed to initialize graph-service due to missing feature-key. " +
                "Please ensure you're licensed for graph-service on all Aerospike nodes in the cluster.");
    }
}
