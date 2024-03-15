package com.aerospike.firefly.process;


import org.apache.tinkerpop.gremlin.GraphProvider;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;

public class FireflyTestListener implements GraphProvider.TestListener {
    private Instant start = Instant.now();
    private final Logger LOG;

    public FireflyTestListener(final Logger LOG) {
        this.LOG = LOG;
    }

    public void onTestStart(final Class<?> test, final String testName) {
        start = Instant.now();
        LOG.warn("===> Running " + testName + " <===");
    }

    public void onTestEnd(final Class<?> test, final String testName) {
        LOG.warn("===> " + testName + " - " + Duration.between(start, Instant.now()).toMillis() + " ms <===");
    }
}
