package com.aerospike.firefly.io.impl;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.relational.linked.LinkedGraph;
import com.aerospike.firefly.io.impl.relational.linked.LinkedVertexProperty;
import com.aerospike.firefly.io.impl.relational.packed.PackedGraph;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.util.ConfigurationHelper.Keys.FIREFLY_DATA_MODEL;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
final public class GraphFactory {
    private static final Logger LOG = LoggerFactory.getLogger(LinkedVertexProperty.class);

    public static FireflyGraph createGraph(final AerospikeConnection db, final Configuration config) {
        try {
            switch (config.get(String.class, FIREFLY_DATA_MODEL.toLowerCase())) {
                case LinkedGraph.DATA_MODEL:
                    if (Upgrade.checkNeedsUpgrade(LinkedGraph.class, db))
                        Upgrade.performUpgrade(LinkedGraph.class, db);
                    LOG.info("Constructing Graph for linked data model.");
                    return new LinkedGraph(db, config);
                case PackedGraph.DATA_MODEL:
                    if (Upgrade.checkNeedsUpgrade(PackedGraph.class, db))
                        Upgrade.performUpgrade(PackedGraph.class, db);
                    LOG.info("Constructing Graph for packed data model.");
                    return new PackedGraph(db, config);
                default:
                    throw new IllegalArgumentException("Unknown graph type: " + config.get(String.class, FIREFLY_DATA_MODEL.toLowerCase()));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
