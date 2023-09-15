package com.aerospike.firefly.io.impl;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.relational.packed.PackedGraph;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.server.Settings;
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
    // We can map the class here, but we do not instantiate the Graphs because that would
    // be unnecessary overhead.
    private static final Map<String, Class<? extends FireflyGraph>> DATA_MODEL_MAP = ImmutableMap.of(
            PackedGraph.DATA_MODEL, PackedGraph.class
    );
    private static final Logger LOG = LoggerFactory.getLogger(GraphFactory.class);

    public static FireflyGraph createGraph(final AerospikeConnection db, final Configuration config) {
        final String dataModel = ConfigurationHelper.getOrDefaultString(FIREFLY_DATA_MODEL, config);
        if (!DATA_MODEL_MAP.containsKey(dataModel)) {
            throw new IllegalArgumentException("Unknown graph type: " + dataModel);
        } else {
            LOG.info("Constructing Graph for {} data model.", dataModel);
            try {
                final Class<? extends FireflyGraph> graphClass = DATA_MODEL_MAP.get(dataModel);
                if (Upgrade.checkNeedsUpgrade(graphClass, db))
                    Upgrade.performUpgrade(graphClass, db);
                db.checkConfigurationCompatibility(config);
                return graphClass.getConstructor(AerospikeConnection.class, Configuration.class, Settings.class).newInstance(db, config, getGremlinServerSettings());
            } catch (final Exception e) {
                // This should never happen, but this prevents us from having to put a throws on the function signature.
                // Gotta love Java...
                throw new RuntimeException("Error constructing graph", e);
            }
        }
    }
}
