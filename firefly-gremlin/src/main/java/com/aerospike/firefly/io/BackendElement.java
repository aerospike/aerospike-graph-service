package com.aerospike.firefly.io;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public abstract class BackendElement {
    protected final AerospikeConnection db;

    protected BackendElement(AerospikeConnection db) {
        this.db = db;
    }
}
