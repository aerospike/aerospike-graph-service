package com.aerospike.firefly.structure.util;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.policy.InfoPolicy;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyAerospikeGraphServiceCheck {
    private FireflyAerospikeGraphServiceCheck() {
    }

    public static void checkFeatureKey(final AerospikeClient client) {
        final String infoResponse = Info.request(new InfoPolicy(), client.getNodes()[0], "feature-key");
        validateInfoResponse(infoResponse);
    }

    static void validateInfoResponse(final String infoResponse) {
        if (infoResponse == null || "".equals(infoResponse)) {
            throw new RuntimeException("Failed to initialize graph-service due to unsupported Server version. " +
                    "Please ensure you're running Aerospike Server Enterprise Edition.");
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
                "Please ensure you're licensed for graph-service");
    }
}
