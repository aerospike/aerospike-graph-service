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

package com.aerospike.firefly.util;

/**
 * Hostnames for integration tests that connect from the CI runner to Firefly containers
 * published with {@code -p 8182:8182}. Self-hosted EC2 runners historically used
 * {@code 172.17.0.1}, but GitHub-hosted runners expose published ports on localhost.
 */
public final class RemoteDockerTestHost {
    private static final String DEFAULT_GREMLIN_HOST = "localhost";
    private static final String DEFAULT_AEROSPIKE_HOST_FROM_CONTAINER = "host.docker.internal";

    private RemoteDockerTestHost() {
    }

    public static String gremlinHost() {
        final String override = System.getenv("FIREFLY_DOCKER_GREMLIN_HOST");
        if (override != null && !override.isBlank()) {
            return override;
        }
        return DEFAULT_GREMLIN_HOST;
    }

    public static String aerospikeHostFromContainer() {
        final String override = System.getenv("FIREFLY_DOCKER_AEROSPIKE_HOST");
        if (override != null && !override.isBlank()) {
            return override;
        }
        return DEFAULT_AEROSPIKE_HOST_FROM_CONTAINER;
    }

    public static String aerospikeHostFromContainerWithPort(final int port) {
        return aerospikeHostFromContainer() + ":" + port;
    }
}
