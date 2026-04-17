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

package com.aerospike.firefly.runtime;

import com.aerospike.firefly.util.DockerUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Queue;

public class TestDockerJavaOptions {
    private static final DockerUtil DOCKER_UTIL = new DockerUtil();

    private static final String[] DEFAULT_ENV_VARIABLES = new String[]{
            "aerospike.client.host=172.17.0.1:3000",
            "aerospike.graph.auto.preheat.enabled=false",
            "JAVA_OPTIONS=-DtestProp" };

    public void testDockerImageSettings(final String[] environmentVariables) throws InterruptedException {
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", true, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundSuccess = false;
        for (final String line : log) {
            System.out.println(line);
            if (line.contains("Channel started at port 8182.")) {
                foundSuccess = true;
                break;
            }
        }
        Assert.assertTrue(foundSuccess);
    }

    @Test
    public void testDefaults() throws InterruptedException {
        testDockerImageSettings(DEFAULT_ENV_VARIABLES);
    }

    @After
    public void afterEachTest() {
        // Cleanup any dangling containers (catch all for test issues).
        DOCKER_UTIL.stopAllDockerImages();
    }
}
