package com.aerospike.firefly.structure.util;

import com.aerospike.firefly.io.FireflyCardinalityMetadata;
import com.aerospike.firefly.io.FireflyMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyMetadataTask extends TimerTask {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyMetadataTask.class);
    private final FireflyMetadata task;

    public FireflyMetadataTask(final FireflyMetadata task) {
        this.task = task;
    }

    // Periodic execution.
    public void run() {
        try {
            task.updateMetadata();
        } catch (Exception ex) {
            LOG.error("Error in FireflyMetadata update thread:", ex);
        }
    }
}
