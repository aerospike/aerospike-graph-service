package com.aerospike.firefly.io.impl;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.relational.RelationalGraph;
import com.aerospike.firefly.io.impl.relational.packed.PackedGraph;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.aerospike.firefly.structure.FireflyGraph.getGremlinServerSettings;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.FIREFLY_DATA_MODEL;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
final public class GraphFactory {
    private static final Map<String, Class<? extends FireflyGraph>> DATA_MODEL_MAP = ImmutableMap.of(
            RelationalGraph.DATA_MODEL, PackedGraph.class
    );
    private static final Logger LOG = LoggerFactory.getLogger(GraphFactory.class);
    public static FireflyGraph createGraph(final AerospikeConnection db, final Configuration config) {
        final String dataModel = ConfigurationHelper.getOrDefaultString(FIREFLY_DATA_MODEL, config);
        if (!DATA_MODEL_MAP.containsKey(dataModel)) {
            throw new IllegalArgumentException("Unknown graph type: " + dataModel);
        } else {
            LOG.info("Constructing Graph for {} data model.", dataModel);
            try {
                if (DataModelVersioning.checkNeedsUpgrade(PackedGraph.class, db))
                    DataModelVersioning.errorNeedsUpgrade(PackedGraph.class, db);
                db.checkConfigurationCompatibility(config);
                return new PackedGraph(db, config, getGremlinServerSettings());
            } catch (Exception e) {
                throw new RuntimeException("Error constructing graph", e);
            }
        }
    }
}
