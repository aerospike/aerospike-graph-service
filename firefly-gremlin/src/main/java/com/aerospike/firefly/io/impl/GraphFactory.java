package com.aerospike.firefly.io.impl;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.linked.LinkedGraph;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
final public class GraphFactory {
    private static final String FIREFLY_DATA_MODEL = "firefly_data_model";
    public static FireflyGraph createGraph(final AerospikeConnection db, final Configuration config) {
        switch (config.get(String.class, FIREFLY_DATA_MODEL)) {
            case LinkedGraph.DATA_MODEL:
                return new LinkedGraph(db, config);
            default:
                throw new IllegalArgumentException("Unknown graph type: " + config.get(String.class, FIREFLY_DATA_MODEL));
        }
    }
}
