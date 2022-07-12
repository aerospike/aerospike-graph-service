package com.aerospike.firefly.io.impl.standard;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.Backend;
import com.aerospike.firefly.io.BackendElement;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class EdgeBackend extends BackendElement implements Backend.Edge {

    public EdgeBackend(AerospikeConnection db) {
        super(db);
    }
}
