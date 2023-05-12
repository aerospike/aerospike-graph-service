package com.aerospike.firefly.io;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class AerospikeLogger implements Log.Callback {
    Logger LOG = LoggerFactory.getLogger(AerospikeClient.class);

    @Override
    public void log(final Log.Level level, final String s) {
        if (level == Log.Level.INFO) {
            LOG.info(s);
        } else if (level == Log.Level.WARN) {
            LOG.warn(s);
        } else if (level == Log.Level.ERROR) {
            LOG.error(s);
        } else if (level == Log.Level.DEBUG) {
            LOG.debug(s);
        } else {
            LOG.info(s);
        }
    }
}
