package com.aerospike.firefly.runtime.tasks;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;

public class FireflyConfigurationTask extends TimerTask {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyConfigurationTask.class);
    private final AerospikeConnection connection;

    public FireflyConfigurationTask(final AerospikeConnection connection) {
        this.connection = connection;
    }

    // Periodic execution.
    public void run() {
        try {
            connection.refreshConfiguration();
        } catch (Exception ex) {
            LOG.error("Error in FireflyConfiguration update thread:", ex);
        }
    }
}
