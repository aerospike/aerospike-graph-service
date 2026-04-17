/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
