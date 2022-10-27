package com.aerospike.firefly.structure.util;

import com.aerospike.firefly.io.FireflyCardinalityMetadata;
import com.aerospike.firefly.io.FireflyMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyMetadataTask extends TimerTask {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyCardinalityMetadata.class);
    private static Timer time = new Timer();
    private final FireflyMetadata task;

    public FireflyMetadataTask(final FireflyMetadata task, final long updateFrequency) {
        this.task = task;
        time.schedule(this, 0, updateFrequency);
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
