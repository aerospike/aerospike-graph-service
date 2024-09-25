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
            "JAVA_OPTIONS=-DtestProp"};

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
