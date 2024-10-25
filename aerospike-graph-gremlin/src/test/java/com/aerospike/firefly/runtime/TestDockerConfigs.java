package com.aerospike.firefly.runtime;

import com.aerospike.firefly.util.DockerUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Queue;

public class TestDockerConfigs {
    private static final DockerUtil DOCKER_UTIL = new DockerUtil();

    public void testDockerImageSettings(final String[] environmentVariables) throws InterruptedException {
        final String containerId = DOCKER_UTIL.startDockerImageCustom("firefly", true, environmentVariables);
        final Queue<String> log = DOCKER_UTIL.getLogs(containerId);
        boolean foundErrorMsg = false;
        for (final String line : log) {
            System.out.println(line);
            if (line.contains("Error: 'aerospike.client.clientPolicy.minConnsPerNode' is set to '2' which is greater than 'aerospike.client.clientPolicy.maxConnsPerNode' set to '1'. 'aerospike.client.clientPolicy.minConnsPerNode' must be less than or equal to 'aerospike.client.clientPolicy.maxConnsPerNode'.")) {
                foundErrorMsg = true;
                break;
            }
        }
        Assert.assertTrue(foundErrorMsg);
    }

    @Test
    public void testMinConnPerNodeGreaterThanMaxConnPerNode() throws InterruptedException {
        testDockerImageSettings(new String[]{
                "aerospike.client.host=172.17.0.1:3000",
                "aerospike.client.clientPolicy.maxConnsPerNode=1",
                "aerospike.client.clientPolicy.minConnsPerNode=2"});
    }

    @After
    public void afterEachTest() {
        // Cleanup any dangling containers (catch all for test issues).
        DOCKER_UTIL.stopAllDockerImages();
    }
}
