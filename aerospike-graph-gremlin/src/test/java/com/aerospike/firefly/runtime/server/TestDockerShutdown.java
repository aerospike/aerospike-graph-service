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

package com.aerospike.firefly.runtime.server;

import com.aerospike.firefly.util.DockerUtil;
import org.junit.Test;

import java.util.Queue;

import static org.junit.Assert.assertTrue;

public class TestDockerShutdown {

    private static final String[] DEFAULT_ENV_VARIABLES = new String[]{
            "aerospike.client.host=host.docker.internal:3000",
            "aerospike.graph.auto.preheat.enabled=false",
            "aerospike.graph.http.enabled=false",
    };

    @Test
    public void testShutdown() throws Exception {
        final DockerUtil dockerUtil = new DockerUtil();

        try {
            final String containerId = dockerUtil.startDockerImageCustom("firefly-slim", true, DEFAULT_ENV_VARIABLES);
            dockerUtil.stopDocker(containerId);

            final Queue<String> log = dockerUtil.getLogs(containerId);
            boolean foundSuccess = false;
            for (final String line : log) {
                if (line.contains("Closing Aerospike client.")) {
                    foundSuccess = true;
                    break;
                }
            }
            assertTrue(foundSuccess);
        } finally {
            dockerUtil.stopAllDockerImages();
        }
    }
}
