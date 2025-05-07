package com.aerospike.firefly.runtime.server;

import com.aerospike.firefly.util.DockerUtil;
import org.junit.Test;

import java.util.Queue;

import static org.junit.Assert.assertTrue;

public class TestDockerShutdown {

    private static final String[] DEFAULT_ENV_VARIABLES = new String[]{
            "aerospike.client.host=172.17.0.1:3000",
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
