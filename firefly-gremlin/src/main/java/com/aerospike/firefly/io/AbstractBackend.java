package com.aerospike.firefly.io;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public abstract class AbstractBackend {
    protected final AerospikeConnection db;

    protected AbstractBackend(AerospikeConnection db) {
        this.db = db;
    }
}
